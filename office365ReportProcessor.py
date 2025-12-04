#!/usr/bin/env python3
"""
Excel Data Processor GUI & CLI with Full Analysis Pipeline

This script provides a graphical user interface (GUI) and a command-line interface (CLI)
for processing Excel files containing multiple reports, and then executing an analysis
pipeline consisting of:
1. Data cleaning and consolidation
2. Query clustering and analysis (general_query_cluster_analysis.py)

Requirements:
- pandas>=2.0.0
- openpyxl>=3.0.0
- python>=3.13
"""

import os
import re
import json
import tkinter as tk
import subprocess
import sys
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

try:
    import pandas as pd
    import openpyxl
    LIBRARIES_OK = True
except ImportError:
    LIBRARIES_OK = False

def find_processed_query_files(search_path: str) -> Tuple[bool, Optional[Path], Optional[Path]]:
    """
    Search filesystem for processed query report files.
    
    Args:
        search_path: Directory to search in
        
    Returns:
        Tuple[bool, Optional[Path], Optional[Path]]: (found, top_query_path, abandoned_query_path)
    """
    search_dir = Path(search_path)
    
    # Search for Top Query Report files
    top_query_files = list(search_dir.glob("Top_Query_Report*processed.csv"))
    abandoned_query_files = list(search_dir.glob("Abandoned_Query_Report*processed.csv"))
    
    if top_query_files and abandoned_query_files:
        return True, top_query_files[0], abandoned_query_files[0]
    
    return False, None, None

def find_analysis_script(script_name: str, start_path: Optional[Path] = None) -> Optional[Path]:
    """
    Search for the analysis script in the current directory, then in a default path.
    
    Args:
        script_name: Name of the script to find (e.g., general_query_cluster_analysis.py)
        start_path: An optional directory to start the search from.
        
    Returns:
        Path of the script if found, otherwise None.
    """
    search_paths: List[Path] = []
    
    # 1. Current Working Directory (CWD)
    search_paths.append(Path.cwd())
    
    # 2. Start path provided (if different from CWD)
    if start_path and start_path != Path.cwd():
        search_paths.append(start_path)
    
    # 3. User's Home Directory
    search_paths.append(Path.home())
    
    # Remove duplicates and ensure paths exist
    search_paths = list(dict.fromkeys([p for p in search_paths if p.is_dir()]))

    for directory in search_paths:
        script_path = directory / script_name
        if script_path.exists():
            return script_path
            
    return None


class ExcelDataProcessor:
    """
    Processes Excel reports, intelligently cleaning data
    and adding date and worksheet name columns.
    """
    def __init__(self, input_file: str, output_dir: str):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.csv_file = self.output_dir / f"{self.input_file.stem}_processed.csv"
        self.json_summary_file = self.output_dir / f"{self.input_file.stem}_processed.summary.json"
        self.json_data_file = self.output_dir / f"{self.input_file.stem}_processed.data.json"
        
        self.date_patterns = [
            r'\b(\d{8})\b',
            r'\b(\d{2}/\d{2}/\d{4})\b',
            r'\b(\d{4}-\d{2}-\d{2})\b',
            r'\b(\d{2}-\d{2}-\d{4})\b',
            r'\b(\d{1,2}/\d{1,2}/\d{4})\b',
            r'\b(\d{1,2}-\d{1,2}/\d{4})\b',
        ]
    
    def validate_excel_file(self) -> Tuple[bool, str]:
        try:
            openpyxl.load_workbook(self.input_file)
            return True, ""
        except openpyxl.utils.exceptions.InvalidFileException:
            return False, "Error: The file is not a valid Excel file."
        except Exception as e:
            return False, f"An unexpected error occurred during file validation: {str(e)}"

    def extract_date_from_text(self, text: Any) -> Optional[str]:
        if pd.isna(text) or text is None:
            return None
            
        text_str = str(text).strip()
        
        for pattern in self.date_patterns:
            matches = re.findall(pattern, text_str)
            if matches:
                date_str = matches[0]
                try:
                    parsed_date = self._parse_date_string(date_str)
                    if parsed_date:
                        return parsed_date.strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    continue
        
        return None
    
    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        formats = [
            '%Y%m%d',
            '%m%d%Y',
            '%m/%d/%Y',
            '%Y-%m-%d',
            '%m-%d-%Y',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def get_date_from_worksheet(self, worksheet_name: str, df: pd.DataFrame) -> Optional[str]:
        date_from_name = self.extract_date_from_text(worksheet_name)
        if date_from_name:
            return date_from_name
        
        max_rows = min(5, len(df))
        for i in range(max_rows):
            for col in df.columns:
                try:
                    cell_value = df.iloc[i, df.columns.get_loc(col)]
                    if pd.notna(cell_value):
                        date_from_content = self.extract_date_from_text(cell_value)
                        if date_from_content:
                            return date_from_content
                except (IndexError, KeyError):
                    continue
        
        return None
    
    def is_rollup_pivot_table(self, worksheet_name: str) -> bool:
        skip_keywords = ['rollup', 'pivot', 'summary', 'total', 'aggregate', 'overview']
        name_lower = worksheet_name.lower()
        return any(keyword in name_lower for keyword in skip_keywords)
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        
        header_row_index = 0
        for idx, row in df.iterrows():
            if sum(1 for val in row if pd.notna(val) and isinstance(val, str)) >= 2:
                header_row_index = idx
                break

        new_header = df.iloc[header_row_index]
        df = df[header_row_index + 1:].copy()
        
        cleaned_columns = []
        for i, col in enumerate(new_header):
            col_str = str(col).strip()
            if not col_str:
                cleaned_columns.append(f'Unnamed_Column_{i}')
            else:
                cleaned_columns.append(col_str)
        df.columns = cleaned_columns
        
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        
        return df.reset_index(drop=True)
    
    def process_file(self, log_callback) -> Tuple[bool, Dict[str, pd.DataFrame]]:
        try:
            log_callback(f"Processing file: {self.input_file}")
            
            if not self.input_file.exists():
                raise FileNotFoundError(f"Input file '{self.input_file}' not found")

            is_valid, error_message = self.validate_excel_file()
            if not is_valid:
                log_callback(error_message)
                return False, {}
            
            try:
                all_sheets = pd.read_excel(self.input_file, sheet_name=None, header=None, engine='openpyxl')
            except Exception as e:
                log_callback(f"An error occurred while reading the file: {e}")
                return False, {}
            
            if not all_sheets:
                log_callback("Error: The selected Excel file appears to be empty.")
                return False, {}

            processed_sheets = {}
            
            for sheet_name, df in all_sheets.items():
                log_callback(f"\nProcessing worksheet: {sheet_name}")
                
                if self.is_rollup_pivot_table(sheet_name):
                    log_callback(f"Skipping rollup/pivot table: {sheet_name}")
                    continue
                
                if df.empty:
                    log_callback(f"No data found in worksheet: {sheet_name}")
                    continue
                
                cleaned_df = self.clean_dataframe(df)
                date_value = self.get_date_from_worksheet(sheet_name, cleaned_df)
                
                if date_value:
                    cleaned_df.insert(0, 'Report_Date', date_value)
                    log_callback(f"Added date column with value: {date_value}")
                else:
                    cleaned_df.insert(0, 'Report_Date', '')
                    log_callback("Warning: Could not determine date for this sheet.")

                cleaned_df.insert(1, 'Worksheet_Name', sheet_name)
                processed_sheets[sheet_name] = cleaned_df
            
            if processed_sheets:
                log_callback("\nWriting consolidated CSV and JSON files.")
                consolidated_df = pd.concat(processed_sheets.values(), ignore_index=True)
                consolidated_df.to_csv(self.csv_file, index=False, encoding='utf-8')
                self._create_json_data_file(consolidated_df, log_callback)
                self._create_json_summary(processed_sheets, log_callback)
            
            log_callback("Data processing completed successfully!")
            return True, processed_sheets
            
        except Exception as e:
            log_callback(f"Error processing file: {str(e)}")
            return False, {}
    
    def _create_json_summary(self, processed_sheets: Dict[str, pd.DataFrame], log_callback) -> None:
        try:
            summary_data = {
                'processing_date': datetime.now().isoformat(),
                'input_file': str(self.input_file),
                'worksheets': []
            }
            
            for sheet_name, processed_df in processed_sheets.items():
                row_count = len(processed_df)
                has_date_column = 'Report_Date' in processed_df.columns
                
                worksheet_info = {
                    'name': sheet_name,
                    'row_count': row_count,
                    'has_date_column': has_date_column,
                    'columns': list(processed_df.columns) if not processed_df.empty else []
                }
                
                if has_date_column and row_count > 0:
                    worksheet_info['report_date'] = processed_df['Report_Date'].iloc[0]
                
                summary_data['worksheets'].append(worksheet_info)
            
            with open(self.json_summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            log_callback(f"Error creating JSON summary: {e}")

    def _create_json_data_file(self, df: pd.DataFrame, log_callback) -> None:
        try:
            df.to_json(self.json_data_file, orient='records', indent=2)
        except Exception as e:
            log_callback(f"Error creating JSON data file: {e}")

    def generate_console_summary(self, log_callback, processed_sheets: Dict[str, pd.DataFrame]) -> None:
        try:
            log_callback("\n" + "="*60)
            log_callback("DATA PROCESSING SUMMARY")
            log_callback("="*60)
            
            for sheet_name, df in processed_sheets.items():
                row_count = len(df)
                has_date_column = 'Report_Date' in df.columns and df['Report_Date'].notna().any()
                
                if has_date_column:
                    unique_date = df['Report_Date'].iloc[0] if row_count > 0 else 'N/A'
                    log_callback(f"✓ {sheet_name}: {row_count} rows, Date: {unique_date}")
                else:
                    log_callback(f"✗ {sheet_name}: {row_count} rows, No date added")
            
            log_callback(f"\nTotal worksheets processed: {len(processed_sheets)}")
            
        except Exception as e:
            log_callback(f"Error generating console summary: {e}")


class ClusteringExecutor:
    """Executes clustering script using subprocess."""
    
    def __init__(self, output_dir: str, cluster_script: str,
                 cluster_args: Optional[Dict[str, Any]] = None):
        self.output_dir = str(output_dir)
        self.cluster_script = cluster_script
        self.cluster_args = cluster_args or {}
    
    def _stream_subprocess_output(self, process, log_callback):
        """Stream subprocess output line by line to log_callback"""
        # Read stdout line by line in a separate thread to prevent blocking
        def _read_stdout():
            try:
                for line in iter(process.stdout.readline, ''):
                    if line:
                        log_callback(line.rstrip())
            except ValueError: # Handle closed pipe
                pass
        
        # Read stderr line by line
        def _read_stderr():
            try:
                for line in iter(process.stderr.readline, ''):
                    if line:
                        log_callback(f"[STDERR]: {line.rstrip()}")
            except ValueError: # Handle closed pipe
                pass

        stdout_thread = threading.Thread(target=_read_stdout)
        stderr_thread = threading.Thread(target=_read_stderr)
        stdout_thread.daemon = True
        stderr_thread.daemon = True
        stdout_thread.start()
        stderr_thread.start()
        
        # Wait for the process to finish and ensure all output threads complete
        return_code = process.wait(timeout=3600)
        
        # Ensure output threads have time to finish reading last few lines
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        
        return return_code
    
    def run_clustering(self, log_callback) -> bool:
        """Run general_query_cluster_analysis.py with command-line arguments"""
        try:
            log_callback("\n" + "="*80)
            log_callback("STEP 1: EXECUTING QUERY CLUSTERING ANALYSIS")
            log_callback("="*80)
            
            # Build command with all arguments
            cmd = [sys.executable, self.cluster_script, '--out-dir', self.output_dir]
            
            # Add optional arguments if provided
            if self.cluster_args.get('sample'):
                cmd.append('--sample')
            
            if 'sample_n' in self.cluster_args:
                cmd.extend(['--sample-n', str(self.cluster_args['sample_n'])])
            
            if 'min_df' in self.cluster_args:
                cmd.extend(['--min-df', str(self.cluster_args['min_df'])])
            
            if 'max_features_word' in self.cluster_args:
                cmd.extend(['--max-features-word', str(self.cluster_args['max_features_word'])])
            
            if 'max_features_char' in self.cluster_args:
                cmd.extend(['--max-features-char', str(self.cluster_args['max_features_char'])])

            
            if 'random_state' in self.cluster_args:
                cmd.extend(['--random-state', str(self.cluster_args['random_state'])])
            
            log_callback(f"Command: {' '.join(cmd)}\n")
            
            # Start the process with pipes for real-time output
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )
            
            # Stream output in real-time and wait for return code (Timeout is high because clustering can be long)
            return_code = self._stream_subprocess_output(process, log_callback)
            
            if return_code != 0:
                log_callback(f"\n✗ Clustering analysis failed (return code: {return_code})")
                return False
            
            log_callback("\n✓ Clustering analysis completed successfully!")
            return True
            
        except subprocess.TimeoutExpired:
            log_callback("✗ Clustering analysis timed out after 1 hour")
            if 'process' in locals() and process:
                process.kill()
            return False
        except Exception as e:
            log_callback(f"✗ Error running clustering analysis: {str(e)}")
            return False
    
    def execute_pipeline(self, log_callback) -> bool:
        """Execute the clustering step."""
        try:
            log_callback("\n" + "="*80)
            log_callback("STARTING M365 SEARCH ANALYSIS PIPELINE (CLUSTERING ONLY)")
            log_callback("="*80)
            log_callback(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            # 1. Clustering (runs synchronously)
            cluster_success = self.run_clustering(log_callback)
            
            if cluster_success:
                log_callback("\n" + "="*80)
                log_callback("✓ PIPELINE COMPLETED SUCCESSFULLY (CLUSTERING)")
                log_callback("="*80)
                log_callback(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                log_callback("\n✗ Pipeline stopped: Clustering analysis failed")
            
            return cluster_success
            
        except Exception as e:
            log_callback(f"\n✗ Pipeline execution error: {str(e)}")
            return False


class ReportProcessorApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("M365 Report Processor")
        self.geometry("800x600")
        
        self.input_file = tk.StringVar()
        self.output_dir = tk.StringVar(value=self._get_downloads_folder())

        self.create_widgets()
        self.check_and_install_dependencies()

    def _get_downloads_folder(self) -> str:
        if sys.platform == 'win32':
            import winreg
            try:
                sub_key = r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'
                downloads_guid = '{374DE290-123F-4565-9164-39C4925E4647}'
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sub_key) as key:
                    return winreg.QueryValueEx(key, downloads_guid)[0]
            except:
                return str(Path.home() / "Downloads")
        else:
            return str(Path.home() / "Downloads")

    def create_widgets(self):
        """Create and place all GUI widgets."""
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Input File Selection
        input_frame = ttk.LabelFrame(main_frame, text="Input File", padding="10")
        input_frame.pack(fill=tk.X, pady=10)

        self.input_entry = ttk.Entry(input_frame, textvariable=self.input_file, state='readonly')
        self.input_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        browse_button = ttk.Button(input_frame, text="Browse...", command=self.select_input_file)
        browse_button.pack(side=tk.RIGHT)

        # Output Directory
        output_frame = ttk.LabelFrame(main_frame, text="Output Directory", padding="10")
        output_frame.pack(fill=tk.X, pady=10)

        self.output_entry = ttk.Entry(output_frame, textvariable=self.output_dir, state='readonly')
        self.output_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        change_dir_button = ttk.Button(output_frame, text="Change...", command=self.select_output_directory)
        change_dir_button.pack(side=tk.RIGHT)

        # Action Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)

        self.validate_button = ttk.Button(button_frame, text="Validate File", command=self.validate_file, state='disabled')
        self.validate_button.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0, 3))

        self.process_button = ttk.Button(button_frame, text="Process Report", command=self.process_report, state='disabled')
        self.process_button.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(3, 3))
        
        self.analysis_button = ttk.Button(button_frame, text="Execute Cluster Analysis", command=self.execute_full_analysis, state='disabled')
        self.analysis_button.pack(side=tk.RIGHT, expand=True, fill=tk.X, padx=(3, 0))
        
        # Log Area
        log_frame = ttk.LabelFrame(main_frame, text="Processing Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = ScrolledText(log_frame, wrap=tk.WORD, state='disabled', height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
    def log_message(self, message):
        """Thread-safe logging to GUI"""
        self.after(0, self._do_log_message, message)

    def _do_log_message(self, message):
        """Update log widget"""
        self.log_text.configure(state='normal')
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state='disabled')
    
    def check_and_install_dependencies(self):
        """Check for required libraries and install them if missing."""
        required_packages = {'pandas': 'pandas', 'openpyxl': 'openpyxl'}
        
        if LIBRARIES_OK:
            self.log_message("✓ All required libraries are installed.")
            self.process_button.configure(state='normal')
            self.validate_button.configure(state='normal')
            self.analysis_button.configure(state='normal')
            return

        self.log_message("Checking for required libraries...")
        all_ok = True
        
        for import_name, package_name in required_packages.items():
            try:
                # Check if the module is available without relying on global scope
                __import__(import_name)
                self.log_message(f"✓ Found: {package_name}")
            except ImportError:
                self.log_message(f"Installing {package_name}...")
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", package_name],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    self.log_message(f"✓ Installed {package_name}")
                except:
                    self.log_message(f"✗ Failed to install {package_name}")
                    all_ok = False
        
        if all_ok:
            self.log_message("\n✓ All dependencies ready!")
            self.process_button.configure(state='normal')
            self.validate_button.configure(state='normal')
            self.analysis_button.configure(state='normal')

    def select_input_file(self):
        file_path = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.input_file.set(file_path)

    def select_output_directory(self):
        dir_path = filedialog.askdirectory(title="Select Output Directory", initialdir=self.output_dir.get())
        if dir_path:
            self.output_dir.set(dir_path)

    def validate_file(self):
        """Validates the selected file and logs the sheets found."""
        input_path = self.input_file.get()
        if not input_path or not Path(input_path).exists():
            messagebox.showerror("Error", "Please select a valid input file.")
            return

        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', tk.END)
        self.log_text.configure(state='disabled')
        
        self.log_message("Validating file...")
        
        try:
            all_sheets = pd.read_excel(input_path, sheet_name=None, header=None, engine='openpyxl')
            self.log_message("\n✓ File is valid!")
            self.log_message(f"Worksheets found: {len(all_sheets)}")
            messagebox.showinfo("Success", "File validation successful!")
        except Exception as e:
            self.log_message(f"\n✗ Validation failed: {e}")
            messagebox.showerror("Validation Failed", "File validation failed. Check the log.")

    def process_report(self):
        """Process Excel report in background thread"""
        input_path = self.input_file.get()
        if not input_path or not Path(input_path).exists():
            messagebox.showerror("Error", "Please select a valid input file.")
            return

        self.process_button.configure(state='disabled')
        self.analysis_button.configure(state='disabled')
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', tk.END)
        self.log_text.configure(state='disabled')
        
        worker_thread = threading.Thread(target=self._threaded_process_task, args=(input_path, self.output_dir.get()))
        worker_thread.daemon = True
        worker_thread.start()

    def _threaded_process_task(self, input_path, output_dir_path):
        try:
            processor = ExcelDataProcessor(input_path, output_dir_path)
            success, processed_sheets = processor.process_file(self.log_message)
            self.after(0, self._process_completion_callback, success, processed_sheets, processor)
        except Exception as e:
            self.log_message(f"Error: {e}")
            self.after(0, self._process_completion_callback, False, {}, None)

    def _process_completion_callback(self, success, processed_sheets, processor):
        if success and processor:
            processor.generate_console_summary(self.log_message, processed_sheets)
            messagebox.showinfo("Success", "File processed successfully!")
        else:
            messagebox.showerror("Error", "Processing failed.")

        self.process_button.configure(state='normal')
        self.analysis_button.configure(state='normal')

    def execute_full_analysis(self):
        """Execute full analysis pipeline in background thread"""
        output_dir_path = self.output_dir.get()
        
        # Check if clustering script file exists
        cluster_script_path = find_analysis_script("general_query_cluster_analysis.py", Path.cwd())
        
        if not cluster_script_path:
            messagebox.showerror("Error", "Clustering script 'general_query_cluster_analysis.py' not found after searching.")
            return

        cluster_script = str(cluster_script_path)
        
        # Check if processed CSV files already exist
        top_query_files = list(Path(output_dir_path).glob("Top_Query_Report*processed.csv"))
        abandoned_query_files = list(Path(output_dir_path).glob("Abandoned_Query_Report*processed.csv"))
        
        input_path = self.input_file.get()
        has_input_file = input_path and Path(input_path).exists()
        has_processed_files = len(top_query_files) > 0 and len(abandoned_query_files) > 0
        
        # If no input file and no processed files, show error
        if not has_input_file and not has_processed_files:
            messagebox.showerror("Error", 
                "Please either:\n"
                "1. Select an Excel input file and process it first, OR\n"
                "2. Ensure processed CSV files exist in the output directory:\n"
                "   - Top_Query_Report*processed.csv\n"
                "   - Abandoned_Query_Report*processed.csv")
            return
        
        self.process_button.configure(state='disabled')
        self.analysis_button.configure(state='disabled')
        self.validate_button.configure(state='disabled')
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', tk.END)
        self.log_text.configure(state='disabled')
        
        # Default cluster arguments (can be customized)
        cluster_args = {
            'sample': False,
            'sample_n': 5000,
            'min_df': 2,
            'max_features_word': 120000,
            'max_features_char': 2500,
            'random_state': 42
        }
        
        worker_thread = threading.Thread(
            target=self._threaded_full_analysis,
            args=(input_path, output_dir_path, cluster_script, has_input_file, cluster_args)
        )
        worker_thread.daemon = True
        worker_thread.start()

    def _threaded_full_analysis(self, input_path, output_dir_path, cluster_script, process_excel, cluster_args):
        try:
            # Step 1: Process Excel file only if input file exists
            if process_excel:
                self.log_message("="*80)
                self.log_message("STEP 0: PROCESSING EXCEL FILE")
                self.log_message("="*80)
                
                processor = ExcelDataProcessor(input_path, output_dir_path)
                success, processed_sheets = processor.process_file(self.log_message)
                
                if not success:
                    self.log_message("\n✗ Excel processing failed. Aborting pipeline.")
                    self.after(0, self._analysis_completion_callback, False)
                    return
                
                processor.generate_console_summary(self.log_message, processed_sheets)
            else:
                self.log_message("="*80)
                self.log_message("SKIPPING EXCEL PROCESSING")
                self.log_message("="*80)
                self.log_message("Using existing processed query files from:")
                self.log_message(f"  Directory: {output_dir_path}")
                self.log_message("  Files: Top_Query_Report*processed.csv, Abandoned_Query_Report*processed.csv\n")
            
            # Step 2: Execute clustering
            executor = ClusteringExecutor(output_dir_path, cluster_script, cluster_args)
            pipeline_success = executor.execute_pipeline(self.log_message)
            
            self.after(0, self._analysis_completion_callback, pipeline_success)
            
        except Exception as e:
            self.log_message(f"\n✗ Pipeline execution error: {str(e)}")
            self.after(0, self._analysis_completion_callback, False)

    def _analysis_completion_callback(self, success):
        if success:
            messagebox.showinfo("Success", "Clustering analysis completed!")
        else:
            messagebox.showerror("Error", "Clustering analysis encountered errors. Check the log.")
        
        self.process_button.configure(state='normal')
        self.analysis_button.configure(state='normal')
        self.validate_button.configure(state='normal')


def cli_log(message):
    print(message)


def get_downloads_folder_cli() -> str:
    if sys.platform == 'win32':
        import winreg
        try:
            sub_key = r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'
            downloads_guid = '{374DE290-123F-4565-9164-39C4925E4647}'
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sub_key) as key:
                return winreg.QueryValueEx(key, downloads_guid)[0]
        except:
            return str(Path.home() / "Downloads")
    else:
        return str(Path.home() / "Downloads")


def check_and_install_cli_dependencies(log_callback) -> bool:
    required_packages = {'pandas': 'pandas', 'openpyxl': 'openpyxl'}

    log_callback("Checking for required libraries...")
    all_ok = True
    
    for import_name, package_name in required_packages.items():
        try:
            __import__(import_name)
            log_callback(f"✓ Found: {package_name}")
        except ImportError:
            log_callback(f"Installing {package_name}...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package_name],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                log_callback(f"✓ Installed {package_name}")
            except:
                log_callback(f"✗ Failed to install {package_name}")
                all_ok = False
                
    return all_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and analyze Office 365 search data.")
    parser.add_argument('-i', '--input_file', type=str, help="Path to the input Excel file.")
    parser.add_argument('-o', '--output_dir', type=str, default=get_downloads_folder_cli(),
                        help="Path to the output directory. Defaults to Downloads folder.")
    parser.add_argument('--cli', action='store_true', help="Run in CLI mode.")
    parser.add_argument('--full-analysis', action='store_true', 
                        help="Run clustering analysis pipeline (clustering only)")
    
    # NEW ARGUMENT: Path to clustering script
    parser.add_argument('--cluster-script-path', type=str, default=None,
                        help="Path to general_query_cluster_analysis.py. Searches if not provided.")
    
    # Clustering arguments
    parser.add_argument('--sample', action='store_true', help="Use sampling mode for clustering")
    parser.add_argument('--sample-n', type=int, default=5000, help="Sample size (default: 5000)")
    parser.add_argument('--min-df', type=int, default=2, help="Minimum document frequency for TF-IDF (default: 2)")
    parser.add_argument('--max-features-word', type=int, default=120000, help="Max word features (default: 120000)")
    parser.add_argument('--max-features-char', type=int, default=2500, help="Max char features (default: 2500)")
    parser.add_argument('--random-state', type=int, default=42, help="Random seed (default: 42)")
    
    args = parser.parse_args()

    if args.cli:
        if not check_and_install_cli_dependencies(cli_log):
            sys.exit(1)

        # If full-analysis flag is set, run the clustering pipeline
        if args.full_analysis:
            cli_log("\n" + "="*80)
            cli_log("CLUSTERING ANALYSIS PIPELINE - CLI MODE")
            cli_log("="*80)
            
            # Check if input file exists
            has_input_file = args.input_file and Path(args.input_file).exists()
            
            # Check for processed files
            found, top_file, abandoned_file = find_processed_query_files(args.output_dir)
            
            if not has_input_file and not found:
                cli_log("\n✗ Error: No input file provided and no processed query files found")
                cli_log("Please either:")
                cli_log("  1. Provide an Excel file with: --input_file <path>")
                cli_log("  2. Ensure processed CSV files exist in output directory:")
                cli_log("     - Top_Query_Report*processed.csv")
                cli_log("     - Abandoned_Query_Report*processed.csv")
                sys.exit(1)
            
            # Step 1: Process Excel if provided
            if has_input_file:
                cli_log("\n" + "="*80)
                cli_log("STEP 0: PROCESSING EXCEL FILE")
                cli_log("="*80)
                
                processor = ExcelDataProcessor(args.input_file, args.output_dir)
                success, processed_sheets = processor.process_file(cli_log)
                
                if not success:
                    cli_log("\n✗ Excel processing failed. Aborting pipeline.")
                    sys.exit(1)
                
                processor.generate_console_summary(cli_log, processed_sheets)
            else:
                cli_log("\n" + "="*80)
                cli_log("SKIPPING EXCEL PROCESSING")
                cli_log("="*80)
                cli_log("Using existing processed query files:")
                cli_log(f"  Top: {top_file}")
                cli_log(f"  Abandoned: {abandoned_file}\n")
            
            # Step 2: Execute clustering
            
            # Locate cluster script (search if not provided by arg)
            if args.cluster_script_path:
                cluster_script_path = Path(args.cluster_script_path)
            else:
                cluster_script_path = find_analysis_script("general_query_cluster_analysis.py")
            
            if not cluster_script_path or not cluster_script_path.exists():
                cli_log(f"\n✗ Clustering script 'general_query_cluster_analysis.py' not found.")
                cli_log(f"  Search paths failed. Use --cluster-script-path to specify manually.")
                sys.exit(1)
                
            cluster_script = str(cluster_script_path)
            
            # Build cluster arguments dictionary
            cluster_args = {
                'sample': args.sample,
                'sample_n': args.sample_n,
                'k_max': args.k_max,
                'min_df': args.min_df,
                'max_features_word': args.max_features_word,
                'max_features_char': args.max_features_char,
                'n_svd': args.n_svd,
                'random_state': args.random_state
            }
            
            # Execute pipeline
            executor = ClusteringExecutor(args.output_dir, cluster_script, cluster_args)
            pipeline_success = executor.execute_pipeline(cli_log)
            
            if pipeline_success:
                cli_log("\n✓ Clustering analysis pipeline completed successfully!")
                sys.exit(0)
            else:
                cli_log("\n✗ Pipeline encountered errors.")
                sys.exit(1)
        
        # Otherwise, just process the Excel file
        else:
            if not args.input_file:
                parser.error("The '--cli' flag requires an '--input_file' argument (or use --full-analysis).")
            
            processor = ExcelDataProcessor(args.input_file, args.output_dir)
            success, processed_sheets = processor.process_file(cli_log)
            if success:
                processor.generate_console_summary(cli_log, processed_sheets)
                sys.exit(0)
            else:
                sys.exit(1)
    else:
        app = ReportProcessorApp()
        app.mainloop()