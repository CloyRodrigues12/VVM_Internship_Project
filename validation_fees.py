"""
This module contains a helper function for validating and preparing fees data
from the staging table for insertion into the master table.
"""
from datetime import datetime, date
import re

# phone number validation function
def _validate_and_standardize_phone_number(phone_number_value, field_name):
    errors = []
    standardized_number = None
    if phone_number_value:
        try:
            # This handles both '7709595126.0' (string) and 770959126.0 (float) converting to float first, then to an integer.
            phone_number_value = int(float(phone_number_value))
        except (ValueError, TypeError):
            pass

        cleaned_number = re.sub(r'\D', '', str(phone_number_value))
        phone_regex = r'^[6789]\d{9}$'
        if re.match(phone_regex, cleaned_number):
            standardized_number = int(cleaned_number)
        else:
            errors.append(f"Invalid {field_name.replace('_', ' ').title()}: '{phone_number_value}'. Must be a 10-digit number starting with 6, 7, 8, or 9.")
            
    return standardized_number, errors

def _validate_and_prepare_fees_data(cursor, record, uploaded_file_id, master_table, academic_year, academic_quarter, institution_code):
    """
    Validates a fees record and prepares the necessary SQL query and values
    for insertion into the master table, with institute-specific rules.

    Args:
        cursor: The database cursor object.
        record (dict): The dictionary representing a single row from the staging table.
        uploaded_file_id (int): The ID of the uploaded file.
        master_table (str): The name of the master fees table.
        academic_year (str): The academic year.
        academic_quarter (str): The academic quarter.
        institution_code (str): The code of the institute to determine the validation rules.

    Returns:
        tuple: A tuple containing (master_insert_query, values, validation_errors).
               Returns (None, None, list) if validation fails.
    """
    validation_errors = []
    
    # --- Common validation: Check for existence of the record and file ID ---
    if record is None or not uploaded_file_id:
        validation_errors.append("Error: Found an empty or invalid record or missing uploaded_file_id.")
        return None, None, validation_errors
        
    # --- SDCCE Fees Validation ---
    if institution_code == 'SDCCE':
        # 1. Define mandatory fields for SDCCE fees
        mandatory_fields = ['student', 'standard_course']
        
        for field in mandatory_fields:
            if not record.get(field):
                validation_errors.append(f"{field.replace('_', ' ').title()} is a mandatory field for SDCCE.")
        
        if validation_errors:
            return None, None, validation_errors
        
        # 2. Validate and standardize the student name to accept special characters
        student_name_raw = record.get('student')
        standardized_student_name = None
        if not student_name_raw:
            validation_errors.append("Student name is a mandatory field.")
        else:
            # Check for non-alphabetic characters (excluding spaces, hyphens, and apostrophes)
            # This allows names like "Jean-Pierre" or "O'Malley"
            if not isinstance(student_name_raw, str) or re.search(r'[^a-zA-Z\s\'-.]', student_name_raw):
                validation_errors.append(f"Invalid student name: {student_name_raw}. It must contain only characters, spaces, hyphens, or apostrophes.")
            else:
                standardized_student_name = ' '.join(student_name_raw.strip().split()).title()

        standard_course= record.get('standard_course')

        fees_id_raw = record.get('fees_id')
        fees_id = None
        if not fees_id_raw:
            validation_errors.append("Fees Id is a mandatory field.")
        else:
            try:
                # Corrected: Convert to float first to handle the .0
                fees_id = int(float(fees_id_raw))
                if fees_id <= 0:
                    validation_errors.append(f"Invalid Fees Id: '{fees_id_raw}'. Must be a positive integer.")
            except (ValueError, TypeError):
                validation_errors.append(f"Invalid Fees Id: '{fees_id_raw}'. Must be a valid integer.")

        fees_schedule_id_raw = record.get('fees_schedule_id')
        fees_schedule_id = None
        if not fees_schedule_id_raw:
            fees_schedule_id = 'NULL'
        else:
            try:
                # Corrected: Convert to float first to handle the .0
                fees_schedule_id = int(float(fees_schedule_id_raw))
                if fees_schedule_id <= 0:
                    validation_errors.append(f"Invalid Fees Schedule Id: '{fees_schedule_id_raw}'. Must be a positive integer.")
            except (ValueError, TypeError):
                validation_errors.append(f"Invalid Fees Schedule Id: '{fees_schedule_id_raw}'. Must be a valid integer.")

        email_raw = record.get('e_mail_address')
        # Corrected: A more robust regex that handles multi-part domains like .edu.in
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]{2,}$'
        if not email_raw:
            standardized_email = 'NULL'
        else:
            email_raw = email_raw.strip()
            if re.match(email_regex, email_raw):
                standardized_email = email_raw.lower()
            else:
                validation_errors.append(f"Invalid email format: '{email_raw}'.")

        mobile_number_raw = record.get('mobile_number')
        standardized_mobile_number = None
        standardized_mobile_number, mobile_errors = _validate_and_standardize_phone_number(mobile_number_raw, 'Mobile Number')
        validation_errors.extend(mobile_errors)

        # Validate and standardize 'Division'
        division_raw = record.get('division')
        standardized_division = None
        division_mapping = {'semester i and ii': '1st Year',
                             'semester iii and iv': '2nd Year','semester v and vi': '3rd Year',}
        if not division_raw:
            standardized_division = None
        else:
            normalized_division = ' '.join(division_raw.strip().split()).lower()
            if normalized_division in division_mapping:
                standardized_division = division_mapping[normalized_division]
            else:
                standardized_division = ' '.join(division_raw.strip().split()).title()

        standardized_registration_code=record.get('registration_code')
        
        # Validate and standardize 'Fee Head'
        fee_head_raw = record.get('fee_head')
        standardized_fee_head = None
        
        # Helper function to get the correct ordinal suffix
        def get_ordinal_suffix(n):
            if 10 <= n % 100 <= 20:
                return 'th'
            else:
                return {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')

        if not fee_head_raw:
            validation_errors.append("Fee Head is a mandatory field.")
        else:
            normalized_fee_head = ' '.join(fee_head_raw.strip().split()).lower()

            # Roman numeral mapping
            roman_numerals = {
                'i': 1, 'ii': 2, 'iii': 3, 'iv': 4, 'v': 5,
                'vi': 6, 'vii': 7, 'viii': 8, 'ix': 9, 'x': 10
            }

            # Check for 'semester' or 'full fees' first
            if 'semester' in normalized_fee_head or 'full' in normalized_fee_head:
                standardized_fee_head = "Full Fees"
            else:
                # Corrected regex to match full Roman numerals
                roman_match = re.search(r'\b(i|ii|iii|iv|v|vi|vii|viii|ix|x)\b', normalized_fee_head)
                
                # Check for numerical installments (e.g., 1st, 2nd)
                numerical_match = re.search(r'(\d+)(st|nd|rd|th)', normalized_fee_head)

                if roman_match:
                    roman_numeral = roman_match.group(1).lower()
                    installment_number = roman_numerals.get(roman_numeral)
                    if installment_number:
                        suffix = get_ordinal_suffix(installment_number)
                        standardized_fee_head = f"{installment_number}{suffix} installment"
                    else:
                        # Fallback for unrecognized Roman numeral (shouldn't happen with this regex)
                        standardized_fee_head = fee_head_raw.strip().title()
                elif numerical_match:
                    index = int(numerical_match.group(1))
                    suffix = get_ordinal_suffix(index)
                    standardized_fee_head = f"{index}{suffix} installment"
                else:
                    # Final fallback for any other format
                    standardized_fee_head = fee_head_raw.strip().title()

        # Validate 'Due Date'
        due_date_raw = record.get('due_date')
        standardized_due_date = None
        if not due_date_raw:
            validation_errors.append("Due Date is a mandatory field.")
        else:
            try:
                parsed_date = datetime.strptime(due_date_raw.strip(), '%d/%m/%Y').date()
                standardized_due_date = parsed_date.strftime('%Y-%m-%d')
            except (ValueError, TypeError):validation_errors.append(f"Invalid Due Date: '{due_date_raw}'. Expected format is DD/MM/YYYY.")

        # 3. Check for an exact duplicate row in the master table for SDCCE
        duplicate_check_query = f"""
            SELECT 1 FROM {master_table}
            WHERE institution_code = %s AND registration_code = %s AND fees_id = %s
        """
        values_for_check = (institution_code, record.get('registration_code'), record.get('fees_id'))
        cursor.execute(duplicate_check_query, values_for_check)
        
        if cursor.fetchone():
            validation_errors.append("Error: A record with this exact data already exists in the master table for SDCCE.")
            return None, None, validation_errors
        
        # 4. Prepare the insertion query and values for SDCCE
        master_insert_query = f"""
            INSERT INTO {master_table} (uploaded_file_id, institution_code, institute_name,student_name,course_name,fees_id,email_address,mobile_no,division_name,registration_code,installment_no,due_date)
            VALUES (%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        values = (uploaded_file_id, institution_code, record.get('institute'),standardized_student_name,standard_course,fees_id,standardized_email,standardized_mobile_number,standardized_division,standardized_registration_code,standardized_fee_head,standardized_due_date )
        
        return master_insert_query, values, validation_errors
        
    # --- RMS Fees Validation ---
    elif institution_code == 'RMS':
        # 1. Define mandatory fields for RMS fees
        mandatory_fields = ['student_id', 'fees_amount', 'transaction_date']
        
        for field in mandatory_fields:
            if not record.get(field):
                validation_errors.append(f"{field.replace('_', ' ').title()} is a mandatory field for RMS.")
        
        if validation_errors:
            return None, None, validation_errors

        # 2. Add other specific RMS validations here (e.g., amount is numeric, date format)
        # ...

        # 3. Check for an exact duplicate row in the master table for RMS
        duplicate_check_query = f"""
            SELECT 1 FROM {master_table}
            WHERE institution_code = %s AND student_id = %s AND fees_amount = %s
        """
        values_for_check = (institution_code, record.get('student_id'), record.get('fees_amount'))
        cursor.execute(duplicate_check_query, values_for_check)
        
        if cursor.fetchone():
            validation_errors.append("Error: A record with this exact data already exists in the master table for RMS.")
            return None, None, validation_errors
        
        # 4. Prepare the insertion query and values for RMS
        master_insert_query = f"""
            INSERT INTO {master_table} (uploaded_file_id, institution_code, student_id, fees_amount)
            VALUES (%s, %s, %s, %s)
        """
        values = (uploaded_file_id, institution_code, record.get('student_id'), record.get('fees_amount'))
        
        return master_insert_query, values, validation_errors

    # --- VVA Fees Validation ---
    elif institution_code == 'VVA':
        # 1. Extract and validate mandatory fields
        mandatory_fields = ['institute','standard_course', 'branch']
        
        for field in mandatory_fields:
            if not record.get(field):
                validation_errors.append(f"{field.capitalize()} is a mandatory field.")

        if not uploaded_file_id:
            validation_errors.append("Uploaded_file_id is a mandatory parameter.")

        if validation_errors:
            return None, None, validation_errors

        # Extract the validated institute name
        institute = record.get('institute')

        # --- Validate and standardize 'branch' first ---
        branch = record.get('branch')
        standardized_branch = None
        is_pre_primary_branch = False

        if branch:
            standardized_branch = branch.strip().title()
            primary_secondary_branch_types = ["Primary", "Secondary", "Senior Secondary"]
            pre_primary_branch_type = "Pre Primary"
        
            if standardized_branch.startswith(pre_primary_branch_type):
                is_pre_primary_branch = True
                standardized_branch = pre_primary_branch_type
            elif any(b in standardized_branch for b in primary_secondary_branch_types):
                standardized_branch = "Primary Secondary Senior Secondary"
            else:
                validation_errors.append(f"Invalid branch value: '{branch}'. Must be a recognized category like 'Pre Primary' or 'Primary/Secondary'.")

        # --- Validate and standardize the 'standard_course' based on the 'branch' ---
        course = record.get('standard_course')
        standardized_course = None
        
        if course:
            normalized_course = course.strip().upper()
            
            if is_pre_primary_branch:
                pre_primary_mapping = {
                    'NURSERY': 'Nursery',
                    'JUNIOR KG': 'Junior KG',
                    'SENIOR KG': 'Senior KG',
                }
                if normalized_course in pre_primary_mapping:
                    standardized_course = pre_primary_mapping[normalized_course]
                else:
                    validation_errors.append(f"Invalid course value: '{course}'. For a 'Pre Primary' branch, course must be 'Nursery', 'Junior KG', or 'Senior KG'.")
            else:
                try:
                    course_grade = int(normalized_course)
                    if 1 <= course_grade <= 12:
                        standardized_course = str(course_grade)
                    else:
                        validation_errors.append(f"Invalid course value: '{course}'. For a 'Primary/Secondary' branch, course must be a number from 1 to 12.")
                except ValueError:
                    validation_errors.append(f"Invalid course value: '{course}'. For a 'Primary/Secondary' branch, course must be a number from 1 to 12.")
        
        # Check for an exact duplicate row in the master table for VVA
        duplicate_check_query = f"""
            SELECT 1 FROM {master_table}
            WHERE institution_code = %s AND institute_name = %s AND course_name = %s
        """
        values_for_check = (institution_code, institute, course)
        cursor.execute(duplicate_check_query, values_for_check)
        
        if cursor.fetchone():
            validation_errors.append("Error: A record with this exact data already exists in the master table.")
            return None, None, validation_errors
        
        # Prepare the insertion query and values for VVA
        master_insert_query = f"""
            INSERT INTO {master_table} (uploaded_file_id, institution_code, institute_name, course_name)
            VALUES (%s, %s, %s, %s)
        """
        values = (uploaded_file_id, institution_code, institute, standardized_course)
        
        return master_insert_query, values, validation_errors
        
    # --- Catch-all for other institutes ---
    else:
        validation_errors.append(f"No specific fees validation rules found for institution code: {institution_code}.")
        return None, None, validation_errors
