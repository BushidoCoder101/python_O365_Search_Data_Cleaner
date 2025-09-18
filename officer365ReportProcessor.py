#!/usr/bin/env python3
"""
Excel Data Processor GUI & CLI

This script provides a graphical user interface (GUI) and a command-line interface (CLI)
for processing Excel files containing multiple reports. It intelligently identifies the
true header row, adds date and worksheet name columns, and consolidates the data into
clean CSV and JSON files.
\

Requirements:
- pandas>=2.0.0
- openpyxl>=3.0.0
- python>=3.13

Usage:
    Run the script directly to open the GUI:
        python office365ReportProcessor.py

    Or, use the command-line interface for batch processing:
        python office365ReportProcessor.py --cli -i <input_file.xlsx> [-o <output_directory>]
"""

import os
import re
import json
import tkinter as tk
import subprocess
import sys
import argparse
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

# We import the required libraries at the top to ensure they are available
# to all classes and functions.
try:
    import pandas as pd
    import openpyxl
    LIBRARIES_OK = True
except ImportError:
    LIBRARIES_OK = False

class ExcelDataProcessor:
    """
    Processes Excel reports, intelligently cleaning data
    and adding date and worksheet name columns.
    """
    def __init__(self, input_file: str, output_dir: str):
        """
        Initialize the processor with input file and output directory paths.

        Args:
            input_file (str): Path to input Excel file
            output_dir (str): Path to the output directory
        """
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.csv_file = self.output_dir / f"{self.input_file.stem}_processed.csv"
        self.json_summary_file = self.output_dir / f"{self.input_file.stem}_processed.summary.json"
        self.json_data_file = self.output_dir / f"{self.input_file.stem}_processed.data.json"
        
        # Date patterns to check for
        self.date_patterns = [
            r'\b(\d{8})\b',              # YYYYMMDD format like 20250909
            r'\b(\d{2}/\d{2}/\d{4})\b',    # MM/DD/YYYY format
            r'\b(\d{4}-\d{2}-\d{2})\b',    # YYYY-MM-DD format
            r'\b(\d{2}-\d{2}-\d{4})\b',    # MM-DD-YYYY format
            r'\b(\d{1,2}/\d{1,2}/\d{4})\b',# M/D/YYYY or MM/DD/YYYY
            r'\b(\d{1,2}-\d{1,2}-\d{4})\b',# M-D-YYYY or MM-DD/YYYY
        ]
    
    def validate_excel_file(self) -> Tuple[bool, str]:
        """
        Validates if an Excel file is a valid, readable format.
        
        This check is crucial for files downloaded from sources like Office 365,
        which may be improperly structured or have corrupted metadata that
        causes parsing errors. The solution is to open and resave them.

        Returns:
            Tuple[bool, str]: True and an empty string if valid, False and an error message otherwise.
        """
        try:
            # Attempt to open the file with openpyxl as a basic validation.
            # This is more resilient than pandas' reader for file structure issues.
            openpyxl.load_workbook(self.input_file)
            return True, ""
        except openpyxl.utils.exceptions.InvalidFileException:
            return False, "Error: The file is not a valid Excel file. Please open it, ensure it's not corrupted, and save it again before trying to process."
        except Exception as e:
            return False, f"An unexpected error occurred during file validation: {str(e)}"

    def extract_date_from_text(self, text: Any) -> Optional[str]:
        """
        Extract date from text using various patterns.

        Args:
            text: Text to search for date (can be any type)
            
        Returns:
            Formatted date string (YYYY-MM-DD) or None if not found
        """
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
        """
        Parse date string with various formats.
        
        Args:
            date_str: Date string to parse
            
        Returns:
            datetime object or None if parsing fails
        """
        formats = [
            '%Y%m%d',      # YYYYMMDD
            '%m%d%Y',      # MMDDYYYY
            '%m/%d/%Y',    # MM/DD/YYYY
            '%Y-%m-%d',    # YYYY-MM-DD
            '%m-%d-%Y',    # MM-DD-YYYY
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def get_date_from_worksheet(self, worksheet_name: str, df: pd.DataFrame) -> Optional[str]:
        """
        Extract date from worksheet name or dataframe content.

        Args:
            worksheet_name: Name of the worksheet
            df: DataFrame containing the worksheet data
            
        Returns:
            Formatted date string (YYYY-MM-DD) or None if not found
        """
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
        """
        Determine if worksheet is a rollup/pivot table to skip processing.
        
        Args:
            worksheet_name: Name of the worksheet
            
        Returns:
            True if worksheet should be skipped
        """
        skip_keywords = ['rollup', 'pivot', 'summary', 'total', 'aggregate', 'overview']
        name_lower = worksheet_name.lower()
        return any(keyword in name_lower for keyword in skip_keywords)
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the dataframe by finding the actual header row and setting it.

        Args:
            df: Raw dataframe from Excel
            
        Returns:
            Cleaned dataframe with proper headers
        """
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
        """
        Main processing function that reads Excel file and adds date columns.
        
        Args:
            log_callback (function): Function to log messages to the GUI
            
        Returns:
            Tuple[bool, Dict[str, pd.DataFrame]]: True if successful, and a dictionary of processed dataframes
        """
        try:
            log_callback(f"Processing file: {self.input_file}")
            
            if not self.input_file.exists():
                raise FileNotFoundError(f"Input file '{self.input_file}' not found")

            # Validate the file before attempting to read it with pandas
            is_valid, error_message = self.validate_excel_file()
            if not is_valid:
                log_callback(error_message)
                return False, {}
            
            try:
                all_sheets = pd.read_excel(self.input_file, sheet_name=None, header=None, engine='openpyxl')
            except Exception as e:
                log_callback(f"An error occurred while reading the file: {e}")
                log_callback("\nThis error is often caused by an invalid or corrupted worksheet name in the Excel file.")
                log_callback("Please manually open the file, check the sheet tabs for any blank or unusual names, and rename them before trying again.")
                return False, {}
            
            if not all_sheets:
                log_callback("Error: The selected Excel file appears to be empty or contains no readable worksheets.")
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
            
            log_callback("Processing completed successfully!")
            return True, processed_sheets
            
        except Exception as e:
            log_callback(f"Error processing file: {str(e)}")
            return False, {}
    
    def _create_json_summary(self, processed_sheets: Dict[str, pd.DataFrame], log_callback) -> None:
        """Create a JSON summary of the processed data."""
        try:
            summary_data = {
                'processing_date': datetime.now().isoformat(),
                'input_file': str(self.input_file),
                'output_files': {
                    'csv': str(self.csv_file),
                    'json_summary': str(self.json_summary_file),
                    'json_data': str(self.json_data_file)
                },
                'worksheets': []
            }
            
            total_records = 0
            sheets_with_dates = 0
            
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
                    sheets_with_dates += 1
                    total_records += row_count
                
                summary_data['worksheets'].append(worksheet_info)
            
            summary_data['summary'] = {
                'total_worksheets': len(processed_sheets),
                'worksheets_with_dates': sheets_with_dates,
                'total_records': total_records
            }
            
            log_callback(f"Writing JSON summary to: {self.json_summary_file}")
            with open(self.json_summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            log_callback(f"Error creating JSON summary: {e}")

    def _create_json_data_file(self, df: pd.DataFrame, log_callback) -> None:
        """Create a JSON data file from the dataframe."""
        try:
            log_callback(f"Writing JSON data to: {self.json_data_file}")
            df.to_json(self.json_data_file, orient='records', indent=2)
        except Exception as e:
            log_callback(f"Error creating JSON data file: {e}")

    def generate_console_summary(self, log_callback, processed_sheets: Dict[str, pd.DataFrame]) -> None:
        """Generate a summary report of the processed data for GUI output."""
        try:
            log_callback("\n" + "="*60)
            log_callback("PROCESSING SUMMARY")
            log_callback("="*60)
            
            total_rows = 0
            sheets_with_dates = 0
            
            for sheet_name, df in processed_sheets.items():
                row_count = len(df)
                has_date_column = 'Report_Date' in df.columns and df['Report_Date'].notna().any()
                
                if has_date_column:
                    sheets_with_dates += 1
                    unique_date = df['Report_Date'].iloc[0] if row_count > 0 else 'N/A'
                    total_rows += row_count
                    log_callback(f"✓ {sheet_name}: {row_count} rows, Date: {unique_date}")
                else:
                    log_callback(f"✗ {sheet_name}: {row_count} rows, No date added")
            
            log_callback(f"\nSummary:")
            log_callback(f"     Total worksheets processed: {len(processed_sheets)}")
            log_callback(f"     Worksheets with date columns: {sheets_with_dates}")
            log_callback(f"     Total records: {total_rows}")
            
            output_files = [self.csv_file, self.json_summary_file, self.json_data_file]
            existing_files = [f for f in output_files if f.exists()]
            log_callback(f"     Files created: {len(existing_files)}")
            
        except Exception as e:
            log_callback(f"Error generating console summary: {e}")


class ReportProcessorApp(tk.Tk):
    """
    Main application window for the Excel Data Processor GUI.
    """
    def __init__(self):
        super().__init__()
        self.title("Office 365 Search Data Cleaner")
        self.geometry("800x600")
        
        # Determine the user's downloads folder for default output
        self.output_dir = tk.StringVar(value=self._get_downloads_folder())
        self.input_file = tk.StringVar()
        
        self.create_widgets()
        # Start the dependency check when the application initializes
        self.after(100, self.check_and_install_dependencies)

    def _get_downloads_folder(self) -> str:
        """Dynamically gets the user's downloads folder path."""
        if sys.platform == 'win32':
            import winreg
            try:
                sub_key = r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'
                downloads_guid = '{374DE290-123F-4565-9164-39C4925E4647}'
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sub_key) as key:
                    downloads_path = winreg.QueryValueEx(key, downloads_guid)[0]
                    return downloads_path
            except Exception:
                return str(Path.home() / "Downloads")
        else: # For Unix/Linux/macOS
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
        self.validate_button.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0, 5))

        self.process_button = ttk.Button(button_frame, text="Process Report", command=self.process_report, state='disabled')
        self.process_button.pack(side=tk.RIGHT, expand=True, fill=tk.X, padx=(5, 0))
        
        # Log Area
        log_frame = ttk.LabelFrame(main_frame, text="Processing Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = ScrolledText(log_frame, wrap=tk.WORD, state='disabled', height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
    def log_message(self, message):
        """Appends a message to the log text widget."""
        self.log_text.configure(state='normal')
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state='disabled')
        self.update_idletasks()

    def check_and_install_dependencies(self):
        """Checks for required libraries and installs them if they are missing."""
        if LIBRARIES_OK:
            self.log_message("All required libraries are already installed.")
            self.process_button.configure(state='normal')
            self.validate_button.configure(state='normal')
            return

        required_packages = {
            'pandas': 'pandas',
            'openpyxl': 'openpyxl'
        }

        self.log_message("Checking for required Python libraries...")
        all_ok = True
        
        for import_name, package_name in required_packages.items():
            try:
                __import__(import_name)
                self.log_message(f"✓ Found required library: {package_name}")
            except ImportError:
                self.log_message(f"✗ '{package_name}' not found. Attempting to install...")
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", package_name],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    self.log_message(f"✓ Successfully installed {package_name}.")
                except subprocess.CalledProcessError:
                    self.log_message(f"Error: Failed to install {package_name}.")
                    self.log_message("Please install it manually by running 'pip install {package_name}' in your terminal.")
                    all_ok = False
        
        if all_ok:
            self.log_message("\nAll dependencies met. The application is ready.")
            self.process_button.configure(state='normal')
            self.validate_button.configure(state='normal')
        else:
            self.log_message("\nCould not install all dependencies. Please install them manually to proceed.")

    def select_input_file(self):
        """Opens a file dialog to select the Excel input file."""
        file_path = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.input_file.set(file_path)

    def select_output_directory(self):
        """Opens a directory dialog to select the output folder."""
        dir_path = filedialog.askdirectory(title="Select Output Directory", initialdir=self.output_dir.get())
        if dir_path:
            self.output_dir.set(dir_path)

    def validate_file(self):
        """
        Validates the selected file using pandas and provides feedback.
        If valid, it shows sheet names and head info. If not, it advises
        the user on the common Office 365 download issue.
        """
        input_path = self.input_file.get()
        if not input_path or not Path(input_path).exists():
            messagebox.showerror("Error", "Please select a valid input file.")
            return

        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', tk.END)
        self.log_text.configure(state='disabled')
        
        self.log_message("Performing file validation...")
        
        try:
            # Attempt to read the file with pandas.
            all_sheets = pd.read_excel(input_path, sheet_name=None, header=None, engine='openpyxl')
            
            # If successful, provide a summary of the file.
            self.log_message("\n✓ File is valid and ready for processing!")
            self.log_message("File Information:")
            self.log_message(f"    - Total worksheets found: {len(all_sheets)}")

            first_sheet_name = list(all_sheets.keys())[0]
            first_df = all_sheets[first_sheet_name]
            
            self.log_message(f"    - Displaying head of the first sheet ('{first_sheet_name}'):")
            
            # Convert DataFrame to a formatted string to display in the log.
            head_str = first_df.head().to_string()
            self.log_message(head_str)

            messagebox.showinfo("Success", "File validation successful!")

        except Exception as e:
            # If any error occurs during the pandas read, provide the specific O365 advice.
            self.log_message(f"\n✗ Validation failed. An error occurred while reading the file: {e}")
            self.log_message("This is a common issue with files downloaded from Office 365 or SharePoint, as they may have corrupted metadata.")
            self.log_message("Please **open the file** in Excel and **save it again** to fix the issue, then try validating it again.")
            messagebox.showerror("Validation Failed", "File validation failed. Please check the log and re-save your file.")


    def process_report(self):
        """Starts the report processing."""
        input_path = self.input_file.get()
        if not input_path or not Path(input_path).exists():
            messagebox.showerror("Error", "Please select a valid input file.")
            return

        self.process_button.configure(state='disabled')
        self.validate_button.configure(state='disabled')
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', tk.END)
        self.log_text.configure(state='disabled')
        
        input_file_path = Path(input_path)
        output_dir_path = Path(self.output_dir.get())
        
        processor = ExcelDataProcessor(input_file_path, output_dir_path)
        
        self.log_message("Starting processing...")
        success, processed_sheets = processor.process_file(self.log_message)
        
        if success:
            processor.generate_console_summary(self.log_message, processed_sheets)
            messagebox.showinfo("Success", "File processed successfully!")
        else:
            messagebox.showerror("Error", "Processing failed. Check the log for details.")

        self.process_button.configure(state='normal')
        self.validate_button.configure(state='normal')

def get_downloads_folder_cli() -> str:
    """Dynamically gets the user's downloads folder path for CLI mode."""
    if sys.platform == 'win32':
        import winreg
        try:
            sub_key = r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'
            downloads_guid = '{374DE290-123F-4565-9164-39C4925E4647}'
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sub_key) as key:
                downloads_path = winreg.QueryValueEx(key, downloads_guid)[0]
                return downloads_path
        except Exception:
            return str(Path.home() / "Downloads")
    else: # For Unix/Linux/macOS
        return str(Path.home() / "Downloads")
    

def check_and_install_cli_dependencies(log_callback) -> bool:
    """Checks for required libraries and installs them if they are missing for CLI mode."""
    required_packages = {
        'pandas': 'pandas',
        'openpyxl': 'openpyxl'
    }

    log_callback("Checking for required Python libraries...")
    all_ok = True
    
    for import_name, package_name in required_packages.items():
        try:
            __import__(import_name)
            log_callback(f"✓ Found required library: {package_name}")
        except ImportError:
            log_callback(f"✗ '{package_name}' not found. Attempting to install...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package_name],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                log_callback(f"✓ Successfully installed {package_name}.")
            except subprocess.CalledProcessError:
                log_callback(f"Error: Failed to install {package_name}.")
                log_callback("Please install it manually by running 'pip install {package_name}' in your terminal.")
                all_ok = False
                
    if all_ok:
        log_callback("\nAll dependencies met. Starting data processing...")
        return True
    else:
        log_callback("\nProcessing aborted due to missing dependencies.")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and consolidate Excel reports.")
    parser.add_argument('-i', '--input_file', type=str, help="Path to the input Excel file.")
    parser.add_argument('-o', '--output_dir', type=str, default=get_downloads_folder_cli(),
                        help="Path to the output directory. Defaults to the user's Downloads folder.")
    parser.add_argument('--cli', action='store_true',
                        help="Run in command-line interface mode. Requires --input_file.")
    
    args = parser.parse_args()

    # Simple log function for the CLI mode
    def cli_log(message):
        print(message)

    if args.cli:
        if not args.input_file:
            parser.error("The '--cli' flag requires an '--input_file' argument.")
        
        # Check and install dependencies for CLI
        if not check_and_install_cli_dependencies(cli_log):
            sys.exit(1)

        processor = ExcelDataProcessor(args.input_file, args.output_dir)
        success, processed_sheets = processor.process_file(cli_log)
        if success:
            processor.generate_console_summary(cli_log, processed_sheets)
    else:
        app = ReportProcessorApp()
        app.mainloop()
