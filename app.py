from flask import Flask, request, jsonify, send_file, make_response
from flask_cors import CORS
import pandas as pd
import mysql.connector
import os
from datetime import datetime, date
import io
import xlsxwriter
import re
from collections import Counter

# Importing the custom validation functions
from validation_students import _validate_and_prepare_student_sdcce, _validate_and_prepare_student_rms
from validation_fees import _validate_and_prepare_fees_data
# Importing the column mappings
from mappings import COLUMN_MAPPING

app = Flask(__name__)
CORS(app)

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'new_VVM_Process_db'
}

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    return mysql.connector.connect(**DB_CONFIG)

@app.route('/institutes', methods=['GET'])
def get_institutes():
    """Fetches a list of all institutes from the database, ordered by name."""
    db_conn = None
    cursor = None
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor(dictionary=True)
        
        query = "SELECT institute_name, institution_code FROM institutions ORDER BY institute_name ASC"
        cursor.execute(query)
        institutes = cursor.fetchall()
        
        return jsonify(institutes)

    except Exception as e:
        print(f"Error fetching institutes: {e}")
        return jsonify({'error': 'Could not fetch institute list from the database.'}), 500
    finally:
        if cursor:
            cursor.close()
        if db_conn and db_conn.is_connected():
            db_conn.close()

@app.route('/check_filename', methods=['POST', 'OPTIONS'])
def check_filename():
    """Checks if a filename already exists in the database."""
    if request.method == 'OPTIONS':
        return _build_cors_preflight_response()
        
    db_conn = None
    cursor = None
    try:
        data = request.get_json()
        filename = data.get('filename')
        if not filename:
            return jsonify({'error': 'Filename is required.'}), 400

        db_conn = get_db_connection()
        # Use a buffered cursor to prevent "Unread result found" errors
        cursor = db_conn.cursor(buffered=True)
        
        query = "SELECT 1 FROM user_upload_details WHERE file_name = %s"
        cursor.execute(query, (filename,))
        result = cursor.fetchone()
        
        return jsonify({'exists': result is not None})

    except Exception as e:
        print(f"Error checking filename: {e}")
        return jsonify({'error': 'Could not check filename in the database.'}), 500
    finally:
        if cursor:
            cursor.close()
        if db_conn and db_conn.is_connected():
            db_conn.close()

def _build_cors_preflight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add('Access-Control-Allow-Headers', "Content-Type,Authorization")
    response.headers.add('Access-Control-Allow-Methods', "GET,POST,OPTIONS")
    return response


def sanitize_column_name(column_name):
    """
    Converts column names to lowercase and replaces spaces with underscores.
    Removes dots, apostrophes, and parentheses.
    """
    sanitized = str(column_name).lower()
    sanitized = sanitized.replace(' ', '_')
    sanitized = sanitized.replace('-', '_')
    sanitized = sanitized.replace('.', '')
    sanitized = sanitized.replace("'", '')
    sanitized = sanitized.replace("(", '')
    sanitized = sanitized.replace(")", '')
    sanitized = sanitized.replace("-", '')
    sanitized = sanitized.replace("?", '')
    return sanitized

def read_file(file, file_name, column_map):
    """
    Reads an Excel or CSV file, identifies the header row based on a fixed number of key headers,
    and returns the DataFrame.
    """
    file_extension = os.path.splitext(file_name)[1].lower()

    if file_extension in ['.xlsx', '.xls']:
        read_func = pd.read_excel
    elif file_extension == '.csv':
        read_func = pd.read_csv
    else:
        raise ValueError("Unsupported file type. Please upload an Excel (.xlsx, .xls) or CSV (.csv) file.")
    
    try:
        # Read the first 20 rows to be sure we don't miss the header
        df_test = read_func(file, header=None, nrows=20)
        file.seek(0)
    except Exception as e:
        raise Exception(f"Error reading initial rows for header detection: {e}")

    MIN_REQUIRED_MATCHES = 5
    expected_sanitized_headers = {sanitize_column_name(k) for k in column_map.keys()}
    
    header_row_index = None
    for i in range(df_test.shape[0]):
        # If the first 10 columns are all empty, skip this row as it's likely a blank line.
        if df_test.iloc[i, :10].isna().all():
            print(f"Skipping row {i} because the first 10 columns are empty.")
            continue
        
        row_values = df_test.iloc[i].dropna().apply(str).apply(sanitize_column_name).tolist()
        matches = len(set(row_values) & expected_sanitized_headers)
        
        if matches >= MIN_REQUIRED_MATCHES:
            header_row_index = i
            break
    
    if header_row_index is None:
        raise ValueError('Could not find a suitable header row. The file does not contain enough matching columns to be processed.')

    df = read_func(file, header=header_row_index)
    return df, header_row_index


def process_and_validate_columns(df, column_map):
    """
    Processes and validates columns, correctly handling multiple source columns
    mapping to a single destination column in a case-insensitive manner.
    - If multiple source columns for a single destination exist, it merges them by
      taking the first available non-null value for each row.
    - Renames columns to the target database names and adds any missing columns.
    """
    # --- Step 1: Standardize the DataFrame's column names to lowercase ---
    # Keep a map of original names for error messages
    original_columns = {col.lower(): col for col in df.columns}
    df.columns = df.columns.str.lower()
    
    # --- Step 2: Create a reverse map from destination DB col to a LIST of lowercase source cols ---
    db_to_source_map = {}
    for source_col, db_col in column_map.items():
        source_col_lower = source_col.lower()
        if db_col not in db_to_source_map:
            db_to_source_map[db_col] = []
        db_to_source_map[db_col].append(source_col_lower)

    final_df = pd.DataFrame()
    errors = []
    
    # Get a unique, ordered list of expected database columns
    expected_db_cols = list(dict.fromkeys(column_map.values()))

    # --- Step 3: Iterate through each unique destination column ---
    for db_col in expected_db_cols:
        possible_source_cols = db_to_source_map.get(db_col, [])
        
        # Find which of the possible source columns actually exist in the uploaded file (case-insensitive)
        source_cols_in_df = [col for col in possible_source_cols if col in df.columns]
        
        # If none of the possible source columns are in the file, create an empty column
        if not source_cols_in_df:
            final_df[db_col] = pd.NA
            continue

        # --- Step 4: Merge data from all found source columns for the current destination ---
        # Initialize the destination column with data from the first found source column.
        merged_col = df[source_cols_in_df[0]].copy()
        
        # If there are other source columns, "coalesce" them.
        # This means for each row, if the value is null, try to fill it with the value
        # from the next source column.
        if len(source_cols_in_df) > 1:
            for next_col in source_cols_in_df[1:]:
                merged_col.fillna(df[next_col], inplace=True)
        
        final_df[db_col] = merged_col

    # --- Final check for columns that may still be entirely empty ---
    # This logic remains the same as before.
    if errors:
        raise ValueError("\n".join(errors))

    # Ensure the final DataFrame has all expected columns in the correct order
    return final_df.reindex(columns=expected_db_cols)
@app.route('/preview', methods=['POST'])
def preview_file():
    """
    Reads an Excel or CSV file and returns a preview of the processed data.
    """
    try:
        file = request.files.get('file')
        table_type = request.form.get('tableType')
        institution_code = request.form.get('institution_code')

        if not file or not table_type or not institution_code:
            return jsonify({'error': 'Missing required form data for preview'}), 400

        column_map = {}
        if table_type == 'Student Details':
            if institution_code in ['SDCCE', 'GRKCL']:
                column_map = COLUMN_MAPPING['students_sdcce_grkcl']
            elif institution_code in ['RMS', 'VVA']:
                column_map = COLUMN_MAPPING['students_rms_vva']
            else:
                return jsonify({'error': 'Invalid institution code for student upload'}), 400
        elif table_type == 'Fees Summary Report':
            column_map = COLUMN_MAPPING['fees']
        else:
            return jsonify({'error': 'Invalid file type'}), 400
        
        df, header_row_index = read_file(file, file.filename, column_map)
        
        # Check for duplicate rows BEFORE any further processing
        df['original_row_number'] = df.index + header_row_index + 2
        
        # Check for duplicates across all columns
        duplicate_rows = df[df.duplicated(subset=df.columns[:-1], keep=False)].sort_values(by=list(df.columns[:-1]))
        
        if not duplicate_rows.empty:
            duplicate_info = {}
            for index, row in duplicate_rows.iterrows():
                row_tuple = tuple(row.iloc[:-1])
                if row_tuple not in duplicate_info:
                    duplicate_info[row_tuple] = []
                duplicate_info[row_tuple].append(row['original_row_number'])

            duplicates_list = []
            for rows in duplicate_info.values():
                if len(rows) > 1:
                    duplicates_list.append({
                        'count': len(rows),
                        'row_numbers': sorted(rows)
                    })
            
            if duplicates_list:
                error_message = f"Duplicate rows detected. Total duplicate sets: {len(duplicates_list)}. Please fix the file before uploading."
                return jsonify({
                    'error': error_message,
                    'details': duplicates_list
                }), 409 # 409 Conflict

        df.drop('original_row_number', axis=1, inplace=True)

        final_df = process_and_validate_columns(df, column_map)
        
        # Drop rows with fewer than 3 non-NA values
        final_df = final_df.dropna(axis=0, thresh=7)

        if final_df.empty:
            return jsonify({'error': 'No matching columns found in the uploaded file.'}), 400
        
        cols_to_drop = [col for col in final_df.columns if final_df[col].isna().all()]
        final_df = final_df.drop(columns=cols_to_drop)

        date_columns = ['admission_date', 'date_of_birth','due_date','fees_paid_date','settlement_date','refund_date']
        for col in date_columns:
            if col in final_df.columns:
                final_df[col] = pd.to_datetime(final_df[col], errors='coerce').dt.strftime('%d-%m-%Y').fillna('N/A')

        preview_data = final_df.fillna(' ').values.tolist()
        headers = list(final_df.columns)
        
        return jsonify({'headers': headers, 'preview_data': preview_data}), 200

    except ValueError as ve:
        print(f"Validation error: {ve}")
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Handles the file upload and inserts data into the staging tables.
    """
    try:
        file = request.files.get('file')
        table_type = request.form.get('tableType')
        institution_code = request.form.get('institution_code')
        # Use .get() with a default value of '-' for academic year and quarter
        academic_year = request.form.get('academicYear', '-')
        academic_quarter = request.form.get('academicQuarter', '-')

        if not file or not table_type or not institution_code:
            return jsonify({'error': 'Missing required form data'}), 400
        
        column_map = {}
        target_table = ""
        if table_type == 'Student Details':
            if institution_code in ['SDCCE', 'GRKCL']:
                target_table = 'stg_sdcce_grkcl_student_details'
                column_map = COLUMN_MAPPING['students_sdcce_grkcl']
            elif institution_code in ['RMS', 'VVA']:
                target_table = 'stg_rms_vva_student_details'
                column_map = COLUMN_MAPPING['students_rms_vva']
            else:
                return jsonify({'error': 'Invalid institution code for student upload'}), 400
        elif table_type == 'Fees Summary Report':
            target_table = 'stg_fees_details'
            column_map = COLUMN_MAPPING['fees']
        else:
            return jsonify({'error': 'Invalid file type'}), 400

        df, _ = read_file(file, file.filename, column_map)
        
        # Check for duplicates a second time to be safe.
        if df.duplicated().any():
            return jsonify({'error': 'Duplicate rows detected in the uploaded file. Please remove them before uploading.'}), 409
        
        db_conn = get_db_connection()
        cursor = db_conn.cursor()

        insert_metadata_query = """
            INSERT INTO user_upload_details 
            (institution_code, file_name, table_type, academic_year, academic_quarter)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_metadata_query, 
                        (institution_code, file.filename, table_type, academic_year, academic_quarter))
        db_conn.commit()
        uploaded_file_id = cursor.lastrowid

        final_df = process_and_validate_columns(df, column_map)

        # Drop rows with fewer than 3 non-NA values
        final_df = final_df.dropna(axis=0, thresh=7)
        
        if final_df.empty:
            return jsonify({'error': 'No matching columns found in the uploaded file.'}), 400
        
        if not final_df.empty:
            last_row = final_df.iloc[-1]
            non_empty_count = last_row.dropna().count()
            if non_empty_count <= 2:
                print(f"Detected and dropped a potential footer row: {last_row.to_dict()}")
                final_df = final_df.iloc[:-1]
                

        final_df['uploaded_file_id'] = uploaded_file_id
      
      # Get a UNIQUE, ordered list of database columns from the mapping
        unique_db_cols = list(dict.fromkeys(column_map.values()))

      # Reorder the DataFrame to match the unique column list for insertion
        final_df = final_df[['uploaded_file_id'] + unique_db_cols]
      
        insert_template = ', '.join(['%s'] * len(final_df.columns))
        insert_query = f"""
            INSERT INTO {target_table} ({', '.join(final_df.columns)})
            VALUES ({insert_template})
        """
        
        data_to_insert = [tuple(None if pd.isna(item) else item for item in row) for row in final_df.to_numpy()]
        
        cursor.executemany(insert_query, data_to_insert)
        db_conn.commit()

        cursor.close()
        db_conn.close()

        return jsonify({'message': 'File uploaded successfully!', 'uploaded_file_id': uploaded_file_id}), 200

    except ValueError as ve:
        print(f"Validation error: {ve}")
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        print(f"Error: {e}")
        if 'db_conn' in locals() and db_conn.is_connected():
            db_conn.close()
        return jsonify({'error': str(e)}), 500
    
@app.route('/download_sample', methods=['GET'])
def download_sample_file():
    """Generates and returns an empty Excel template based on file type and institute."""
    try:
        file_type = request.args.get('fileType')
        institution_code = request.args.get('institution_code')

        if not file_type or not institution_code:
            return jsonify({'error': 'Missing required parameters: fileType and institution_code'}), 400

        target_table_name = ""
        column_map = {}
        if file_type == 'Student Details':
            if institution_code in ['SDCCE', 'GRKCL']:
                column_map = COLUMN_MAPPING['students_sdcce_grkcl']
                target_table_name = 'stg_sdcce_grkcl_student_details'
            elif institution_code in ['RMS', 'VVA']:
                column_map = COLUMN_MAPPING['students_rms_vva']
                target_table_name = 'stg_rms_vva_student_details'
            else:
                return jsonify({'error': 'Invalid institution code for student upload'}), 400
        elif file_type == 'Fees Summary Report':
            column_map = COLUMN_MAPPING['fees']
            target_table_name = 'stg_fees_details'
        else:
            return jsonify({'error': 'Invalid file type'}), 400
            
        template_headers = list(column_map.keys())
        df = pd.DataFrame(columns=template_headers)

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            
            for i, col in enumerate(df.columns):
                max_len = len(col) + 2
                worksheet.set_column(i, i, max_len)
        output.seek(0)
        
        filename = f"{target_table_name}.xlsx"
    
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        response.headers['Access-Control-Expose-Headers'] = 'Content-Disposition'

        return response

    except Exception as e:
        print(f"Error generating sample file: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/process_upload', methods=['POST'])
def process_upload():
    """
    Validates and moves data from the staging table to the master table
    using an explicit, all-or-nothing transaction.
    """
    db_conn = None
    cursor = None
    try:
        data = request.get_json()
        uploaded_file_id = data.get('uploaded_file_id')
        table_type = data.get('table_type')
        institution_code = data.get('institution_code')
        
        if not all([uploaded_file_id, table_type, institution_code]):
            return jsonify({'error': 'Missing required data for processing'}), 400
        
        db_conn = get_db_connection()
        db_conn.autocommit = False  
        cursor = db_conn.cursor(dictionary=True)

        metadata_query = "SELECT academic_year, academic_quarter FROM user_upload_details WHERE upload_id = %s"
        cursor.execute(metadata_query, (uploaded_file_id,))
        metadata = cursor.fetchone()
        if not metadata:
            return jsonify({'error': 'Metadata for this upload not found.'}), 404
        academic_year = metadata['academic_year']
        academic_quarter = metadata['academic_quarter']

        staging_table = ''
        master_table = ''
        
        if table_type == 'Student Details':
            if institution_code in ['SDCCE', 'GRKCL']:
                staging_table = 'stg_sdcce_grkcl_student_details'
                master_table = 'students_details_master'
            elif institution_code in ['RMS', 'VVA']:
                staging_table = 'stg_rms_vva_student_details'
                master_table = 'students_details_master'
            else:
                return jsonify({'error': 'Invalid institution code for student processing'}), 400
        elif table_type == 'Fees Summary Report':
            staging_table = 'stg_fees_details'
            master_table = 'student_fee_transactions'
        else:
            return jsonify({'error': 'Invalid table type for processing'}), 400
        
        select_query = f"SELECT * FROM {staging_table} WHERE uploaded_file_id = %s"
        cursor.execute(select_query, (uploaded_file_id,))
        staging_records = cursor.fetchall()
        
        if not staging_records:
            return jsonify({'message': 'No records found in staging table to process.'})

        processed_count = 0
        error_count = 0
        errors = []
        successful_insertions = []

        for i, record in enumerate(staging_records):
            row_number = i + 1
            master_insert_query = None
            values = None
            validation_errors = []

            result = None
            if table_type == 'Student Details':
                if institution_code in ['SDCCE', 'GRKCL']:
                    result = _validate_and_prepare_student_sdcce(cursor, record, institution_code, master_table)
                elif institution_code in ['RMS', 'VVA']:
                    result = _validate_and_prepare_student_rms(cursor, record, institution_code, master_table, academic_year, academic_quarter)
            elif table_type == 'Fees Summary Report':
                result = _validate_and_prepare_fees_data(cursor, record, uploaded_file_id, master_table, academic_year, academic_quarter, institution_code)

            if result is not None:
                master_insert_query, values, validation_errors = result
            else:
                validation_errors.append("Internal validation error: The validation function returned an unexpected value.")
            
            if not validation_errors and master_insert_query:
                successful_insertions.append({'query': master_insert_query, 'values': values})
                processed_count += 1
            else:
                error_count += 1
                errors.append({
                    'row_number': row_number,
                    'record_data': record,
                    'error_messages': validation_errors
                })

        if error_count > 0:
            db_conn.rollback()
            message = "Processing failed. No records were moved due to validation errors."
        else:
            for insertion in successful_insertions:
                cursor.execute(insertion['query'], insertion['values'])
            db_conn.commit()
            message = "Processing complete. All records successfully inserted."

        try:
            delete_staging_query = f"DELETE FROM {staging_table} WHERE uploaded_file_id = %s"
            cursor.close()
            cursor = db_conn.cursor()
            cursor.execute(delete_staging_query, (uploaded_file_id,))
            deleted_count = cursor.rowcount
            db_conn.commit()
            print(f"Cleanup: {deleted_count} rows deleted from {staging_table}")
        except Exception as cleanup_err:
            db_conn.rollback()
            print(f"Cleanup failed: {cleanup_err}")

        cursor.close()
        db_conn.close()
        
        return jsonify({
             'message': message,
             'total_records': len(staging_records),
             'processed_count': processed_count if error_count == 0 else 0,
             'error_count': error_count,
             'errors': errors
        }), 200

    except Exception as e:
        print(f"Error during processing: {e}")
        if db_conn and db_conn.is_connected():
            db_conn.rollback()
            db_conn.close()
        return jsonify({'error': str(e)}), 500
    
if __name__ == '__main__':
    app.run(debug=True)
