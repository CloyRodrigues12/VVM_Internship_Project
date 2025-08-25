"""
This module contains helper functions for validating and preparing student data from different institute staging tables for insertion into the master table.
"""
from datetime import datetime, date
import re
import difflib

def validate_and_format_name(name_string):
    if not name_string or not str(name_string).strip():
        return False, f"Invalid name: '{name_string}'. Name cannot be empty."
    
    cleaned_name = str(name_string).strip()
    
    # Allow names with dots (e.g., for initials)
    if not all(char.isalpha() or char.isspace() or char == '.' for char in cleaned_name):
        return False, f"Invalid name: '{name_string}'. Only alphabets, spaces, and dots are allowed."

    formatted_name = " ".join(word.capitalize() for word in cleaned_name.split())
    return formatted_name

# A list of standardized occupations.
OCCUPATION_STANDARDS = [
    'Businessman',
    'Engineer',
    'Doctor',
    'Professor/Teacher',
    'Government Servant',
    'Housewife/Homemaker',
    'Private Sector'
]

# Set a threshold for fuzzy matching (0.0 to 1.0)
SIMILARITY_THRESHOLD = 0.8

def validate_and_standardize_occupation(occupation_string):
    """
    Validates and standardizes an occupation string using fuzzy matching.
    """
    if not occupation_string or not str(occupation_string).strip():
        # This function must consistently return a tuple for errors
        return False, f"Invalid occupation: '{occupation_string}'. Occupation cannot be empty."

    cleaned_string = str(occupation_string).strip().lower()

    if not all(c.isalnum() or c.isspace() or c in './-' for c in cleaned_string):
        return False, f"Invalid occupation: '{occupation_string}'. Contains invalid characters."
    
    # Keyword-based matching
    keyword_map = {
        'private': 'Private Sector',
        'housewife': 'Housewife/Homemaker',
        'business': 'Businessman',
        'engineer': 'Engineer',
        'teacher': 'Professor/Teacher',
        'professor': 'Professor/Teacher',
        'doctor': 'Doctor',
        'govt': 'Government Servant'
    }
    for keyword, standard in keyword_map.items():
        if keyword in cleaned_string:
            return standard

    # --- CORRECTED FUZZY MATCHING LOGIC ---
    # get_close_matches returns a list of strings, e.g., ['businessman']
    matches = difflib.get_close_matches(
        cleaned_string, 
        [s.lower() for s in OCCUPATION_STANDARDS], 
        n=1, 
        cutoff=SIMILARITY_THRESHOLD
    )

    # Check if the list of matches is not empty
    if matches:
        best_match_lower = matches[0]
        # Find the original properly-cased version from the standard list
        for standard_job in OCCUPATION_STANDARDS:
            if standard_job.lower() == best_match_lower:
                return standard_job
    
    # If no fuzzy match is found, return the cleaned, title-cased string
    return " ".join(word.capitalize() for word in cleaned_string.split())

def _validate_and_prepare_student_sdcce(cursor, record, institution_code, master_table):
    """
    Validates a student record from the SDC-GRKCL staging table and prepares 
    the necessary SQL query and values for insertion into the master table.
    
    Args:
        cursor: The database cursor object.
        record (dict): The dictionary representing a single row from the staging table.
        institution_code (str): The code of the institute (e.g., 'SDCCE').
        master_table (str): The name of the master student table.

    Returns:
        tuple: A tuple containing (master_insert_query, values, validation_errors).
               Returns (None, None, list) if validation fails.
    """
    validation_errors = []

    if record is None:
        return None, None, ["Error: Found an empty or invalid record in the staging data."]
    
    # Initialize standardized variables to avoid UnboundLocalError
    date_of_birth, full_address, admission_date, admission_feepayment_time = None, None, None, None
    standardized_admission_category, standardized_religion, standardized_blood_group = None, None, None
    standardized_email, mobile, alternate_mobile, mother_mobile, father_mobile = None, None, None, None, None
    city, state, student_name, father_name, mother_name = None, None, None, None, None
    father_occupation_category, mother_occupation_category, nationality, xii_division = None, None, None, None
    pincode_str, xii_passing_year_val, xii_percentage = None, None, 0.0
    pwd_category_and_percentage = 'N/A'

    # --- 1. Validate all mandatory fields ---
    required_fields = {
        'admission_transaction_number': 'Admission Transaction Number',
        'form_number': 'Form Number',
        'admission_fee_paid_on': 'Admission Fee Paid On',
        'programme_name': 'Programme Name',
        'name_of_the_applicant': 'Applicant Name',
        'gender': 'Gender',
        'admission_category': 'Admission Category',
        'dob_day': 'Day of Birth', 'dob_month': 'Month of Birth', 'dob_year': 'Year of Birth',
        'religion': 'Religion',
        'email': 'Email',
        'add_line_1': 'Address Line 1',
    }
    
    for field_key, field_name in required_fields.items():
        value = record.get(field_key)
        if value is None or (isinstance(value, str) and not value.strip()):
            validation_errors.append(f"Missing mandatory field: {field_name}")

    # --- 2. Coalesce City/Other City and State/Other State ---
    city = str(record.get('city') or record.get('other_city') or '').strip()
    if not city:
        city = ' '
        #validation_errors.append("City or Other City must have a value.")

    state = str(record.get('state') or record.get('other_state') or '').strip()
    if not state:
        validation_errors.append("State or Other State must have a value.")

    # --- 3. Name Validations ---
    for name_field, display_name in [('name_of_the_applicant', 'Student'), ('name_of_father', "Father's"), ('name_of_mother', "Mother's")]:
        raw_name = record.get(name_field)
        result = validate_and_format_name(raw_name)
        if isinstance(result, tuple):
            validation_errors.append(f"{display_name} Name Error: {result[1]}")
        elif name_field == 'name_of_the_applicant':
            student_name = result
        elif name_field == 'name_of_father':
            father_name = result
        elif name_field == 'name_of_mother':
            mother_name = result
    
    # --- 4. Validate and Combine Date of Birth ---
    dob_year, dob_month, dob_day = record.get('dob_year'), record.get('dob_month'), record.get('dob_day')
    if all((dob_year, dob_month, dob_day)):
        try:
            year, month, day = int(dob_year), int(dob_month), int(dob_day)
            dob = date(year, month, day)
            if dob > date.today():
                validation_errors.append(f"Invalid Date of Birth: '{dob.strftime('%Y-%m-%d')}' cannot be in the future.")
            else:
                date_of_birth = dob.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            validation_errors.append(f"Invalid Date of Birth provided: Year={dob_year}, Month={dob_month}, Day={dob_day}.")

    # --- 5. Validate Admission Fee Paid On ---
    admission_fee_paid_on = record.get('admission_fee_paid_on')
    if admission_fee_paid_on:
        try:
            admission_datetime = datetime.strptime(str(admission_fee_paid_on).strip(), '%Y-%m-%d %H:%M:%S')
            admission_date = admission_datetime.date()
            admission_feepayment_time = admission_datetime.time()
        except (ValueError, TypeError):
            validation_errors.append("Invalid Admission Fee Payment Date/Time. Expected format: YYYY-MM-DD HH:MM:SS")
    
    # --- 6. Validate Pincode ---
    pincode = record.get('pincode')
    if pincode:
        pincode_str = str(pincode).strip().replace('.0', '') # Handle floats like 403602.0
        if not (pincode_str.isdigit() and len(pincode_str) == 6):
            validation_errors.append(f"Invalid pincode format: '{pincode}'. Must be a 6-digit number.")
    
    # --- 7. Validate all mobile numbers ---
    def _validate_phone(number_val, field_name):
        if number_val:
            # First, convert to string to handle int, float, or str inputs
            num_str = str(number_val)
            
            # **FIX**: Specifically handle numbers that were read as floats (e.g., '9422059555.0')
            if num_str.endswith('.0'):
                num_str = num_str[:-2]  # Remove the trailing '.0'
            
            # Now, remove any remaining non-digit characters (like spaces, dashes, etc.)
            cleaned = re.sub(r'\D', '', num_str)
            
            # Validate against the 10-digit Indian mobile number format
            if re.match(r'^[6-9]\d{9}$', cleaned):
                return int(cleaned), None
            
            # If validation fails, return the error with the original value for context
            return None, f"Invalid {field_name}: '{number_val}'. Must be a 10-digit Indian mobile number."
        
        # If the input value is None or empty, return no value and no error
        return None, None

    mobile, err = _validate_phone(record.get('mobile'), 'Mobile Number')
    if err: validation_errors.append(err)
    alternate_mobile, err = _validate_phone(record.get('alternate_mobile'), 'Alternate Mobile')
    if err: validation_errors.append(err)
    father_mobile, err = _validate_phone(record.get('father_mobile'), "Father's Mobile")
    if err: validation_errors.append(err)
    mother_mobile, err = _validate_phone(record.get('mother_mobile'), "Mother's Mobile")
    if err: validation_errors.append(err)

    # --- 9. Validate and standardize 'admission_category' ---
    admission_category = record.get('admission_category')
    
    # UPDATED: Added PWBD and variations for SC/ST to be more robust.
    category_mapping = {
        'SCHEDULED CASTE': 'SC', 
        'SCHEDULE CASTE': 'SC', 
        'SC': 'SC',
        
        'SCHEDULED TRIBE': 'ST',
        'SCHEDULE TRIBE': 'ST',
        'SCHEDULED TRIBE(ST)': 'ST',
        'SCHEDULED TRIBE (ST)': 'ST',# entry with space
        'ST': 'ST',

        'OTHER BACKWARD CLASSES': 'OBC',
        'OBC': 'OBC',
        
        'PWBD': 'PWBD', 
        'PERSONS WITH BENCHMARK DISABILITIES': 'PWBD',
        'PWD': 'PWBD',
        
        'UNRESERVED': 'UR',
        'UR': 'UR',
        'GENERAL': 'UR'
    }

    if admission_category:
        normalized = str(admission_category).strip().upper()
        standardized_admission_category = category_mapping.get(normalized)
        if not standardized_admission_category:
            validation_errors.append(f"Invalid admission category: '{admission_category}'.")

    # --- 10. Validate 'religion' ---
    religion = record.get('religion')
    ALLOWED_RELIGIONS = {'HINDUISM', 'CHRISTIANITY', 'ISLAM', 'SIKHISM', 'BUDDHISM', 'JAINISM'}
    if religion:
        normalized = str(religion).strip().upper()
        if normalized in ALLOWED_RELIGIONS:
            standardized_religion = normalized.title()
        else:
            validation_errors.append(f"Invalid religion: '{religion}'. Accepted values are: {', '.join(sorted(list(ALLOWED_RELIGIONS)))}.")
    
    # --- 11. Validate 'blood_group' ---
    blood_group = record.get('blood_group')
    ALLOWED_BLOOD_GROUPS = {'A+', 'A-', 'B+', 'B-', 'O+', 'O-', 'AB+', 'AB-'}
    if blood_group:
        normalized = str(blood_group).strip().upper()
        if normalized in ALLOWED_BLOOD_GROUPS:
            standardized_blood_group = normalized
        else:
            validation_errors.append(f"Invalid blood group: '{blood_group}'.")

    # --- 12. Validate 'email' format ---
    email = record.get('email')
    if email:
        email = str(email).strip()
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            standardized_email = email.lower()
        else:
            validation_errors.append(f"Invalid email format: '{email}'.")
            
    # --- 13. Combine address fields ---
    address_parts = [str(part) for part in [record.get('add_line_1'), record.get('add_line_2'), city, state, pincode_str] if part]
    full_address = ', '.join(address_parts) if address_parts else None
    
    # --- Occupation Validations ---
    # # Initialize with empty strings as a default
    father_occupation_category = ''
    mother_occupation_category = ''
    for occ_field, display_name in [('father_occupation', "Father's"), ('mother_occupation', "Mother's")]:
        raw_occ = record.get(occ_field) # If a value exists, validate it. Otherwise, the category remains an empty string.
        if raw_occ and str(raw_occ).strip():
            result = validate_and_standardize_occupation(raw_occ)
            
            if isinstance(result, tuple):
                validation_errors.append(f"{display_name} Occupation Error: {result[1]}")# The category variable will keep its default empty string value on error
            elif occ_field == 'father_occupation':
                father_occupation_category = result
            elif occ_field == 'mother_occupation':
                mother_occupation_category = result

    # --- Nationality Validation ---
    if str(record.get('are_you_citizen_of_india')).strip().upper() in ('YES', 'Y'):
        nationality = 'Indian'
    elif record.get('other_nationality') and str(record.get('other_nationality')).strip():
        nationality = str(record.get('other_nationality')).strip().title()
    else:
        validation_errors.append("Nationality is missing. Specify if Indian citizen or provide other nationality.")
        
    # --- XII Passing Year Validation ---
    xii_passing_year = record.get('xii_passing_year')
    if xii_passing_year:
        try:
            year_val = int(float(str(xii_passing_year)))
            if not (1980 <= year_val <= date.today().year):
                 validation_errors.append(f"Invalid XII passing year: '{xii_passing_year}'. Must be between 1980 and the current year.")
            else:
                 xii_passing_year_val = year_val
        except (ValueError, TypeError):
             validation_errors.append(f"Invalid XII passing year format: '{xii_passing_year}'. Must be a 4-digit number.")
             
    # --- XII Percentage Validation ---
    xii_percentage_raw = record.get('xii_percentage')
    if xii_percentage_raw:
        try:
            cleaned_string = str(xii_percentage_raw).strip().replace('%', '')
            percentage_value = float(cleaned_string)
            if 0 < percentage_value <= 1:
                percentage_value *= 100
            if not (0 < percentage_value <= 100):
                validation_errors.append(f"XII Percentage: '{xii_percentage_raw}' must be between 1 and 100.")
            else:
                xii_percentage = round(percentage_value, 2)
        except (ValueError, TypeError):
            validation_errors.append(f"XII Percentage: '{xii_percentage_raw}' is not a valid number.") 

    # --- XII Division Validation ---
    xii_division_raw = record.get('xii_division')
    ALLOWED_DIVISIONS = {'DISTINCTION', 'FIRST DIVISION', 'PASS DIVISION', 'SECOND DIVISION'}
    if xii_division_raw:
        cleaned_string = str(xii_division_raw).strip().upper()
        if cleaned_string in ALLOWED_DIVISIONS:
            xii_division = cleaned_string
        else:
            validation_errors.append(f"Invalid XII Division: '{xii_division_raw}'.")
    
    # --- Urban/Rural Area Validation ---
    area_raw = record.get('urban_rural_semi_urban_metropolitan_area')
    ALLOWED_AREAS = {'METROPOLITAN', 'RURAL', 'SEMI-URBAN', 'URBAN'}
    if area_raw:
        cleaned_string = str(area_raw).strip().upper()
        if cleaned_string in ALLOWED_AREAS:
            urban_rural_semi_urban_metropolitan_area = cleaned_string
        else:
            validation_errors.append(f"Invalid Area: '{area_raw}'. Must be one of: {', '.join(ALLOWED_AREAS)}.")

    # --- PWD Validation and Combination ---
    category_string = str(record.get('pwd_category') or record.get('pwd_category_other') or '').strip()
    if category_string:
        percentage_raw = record.get('pwd_percentage_of_disability')
        if not percentage_raw:
            validation_errors.append("PWD Category is provided but percentage is missing.")
        else:
            try:
                cleaned_percentage = str(percentage_raw).strip().replace('%', '')
                percentage_value = float(cleaned_percentage)
                if 0 < percentage_value <= 1:
                    percentage_value *= 100
                if not (0 <= percentage_value <= 100):
                    validation_errors.append(f"PWD Percentage: '{percentage_raw}' must be between 0 and 100.")
                else:
                    pwd_category_and_percentage = f"{category_string.title()}: {percentage_value}%"
            except (ValueError, TypeError):
                validation_errors.append(f"PWD Percentage: '{percentage_raw}' is not a valid number.")

    # --- FINAL CHECK: Return all validation errors if any were found ---
    if validation_errors:
        return None, None, validation_errors
    
    # --- Prepare for DB Operations ---
    student_name_for_check = record.get('name_of_the_applicant').strip()
    form_number = record.get('form_number')

    # --- Duplicate Check ---
    duplicate_check_query = f"SELECT 1 FROM {master_table} WHERE institution_code = %s AND admission_no = %s AND is_active = 1"
    cursor.execute(duplicate_check_query, (institution_code, form_number))
    if cursor.fetchone():
        return None, None, [f"Error: An active record with Admission No '{form_number}' already exists for this institution."]
    
    # --- Deactivate Previous Records ---
    check_query = f"SELECT master_id FROM {master_table} WHERE student_name = %s AND date_of_birth = %s AND is_active = 1"
    cursor.execute(check_query, (student_name_for_check, date_of_birth))
    existing_records = cursor.fetchall()
    for rec in existing_records:
        update_query = f"UPDATE {master_table} SET is_active = 0 WHERE master_id = %s"
        cursor.execute(update_query, (rec['master_id'],))
    
    # --- INSERT query and values tuple ---
    master_insert_query = f"""
        INSERT INTO {master_table} (
            student_reference_id, institution_code, uploaded_file_id, admission_no, stream, pr_no, admission_date,
            admission_feepayment_time, student_name, date_of_birth, full_address, gender, 
            student_category, religion, blood_group, email_address, city, state, pin_code, 
            mobile_number, alt_mobile_number, fathers_mobile_number, mothers_mobile_number, 
            fathers_name, mothers_name, fathers_occupation, mothers_occupation, 
            fathers_occupation_category, mothers_occupation_category, nationality, 
            name_of_the_institution_attended_earlier, board_name, passing_year, xii_stream, passsing_percentage, 
            xii_passing_class,  pwd_category_and_Percentage, urban_rural_category
        ) VALUES (
           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    values = (
        record.get('admission_transaction_number'), institution_code, record.get('uploaded_file_id'), form_number, record.get('programme_name'), 
        record.get('enrollment_number'), admission_date, admission_feepayment_time, student_name, 
        date_of_birth, full_address, record.get('gender'), standardized_admission_category, 
        standardized_religion, standardized_blood_group, standardized_email, city, state, pincode_str, 
        mobile, alternate_mobile, father_mobile, mother_mobile, father_name, mother_name, 
        record.get('father_occupation'), record.get('mother_occupation'), father_occupation_category, 
        mother_occupation_category, nationality, record.get('xii_name_of_the_institution'), 
        record.get('xii_board'), xii_passing_year_val, record.get('xii_stream'), xii_percentage, 
        xii_division, pwd_category_and_percentage, record.get('urban_rural_semi_urban_metro_area')      
    )

    return master_insert_query, values, []


#validation for RMS and VVA
def _validate_and_prepare_student_rms(cursor, record, institution_code, master_table):
    
    validation_errors = []

    # cleaned variables
    standardized_admission_date = None

    # --- Admission Date Validation ---
    raw_admission_date = record.get('admission_date')
    if not raw_admission_date or not str(raw_admission_date).strip():
        # Assuming admission_date is mandatory. If it's optional, we can remove this line.
        validation_errors.append("Missing mandatory field: Admission Date")
    else:
        try:
            # Parse the date string assuming DD-MM-YYYY format
            date_obj = datetime.strptime(str(raw_admission_date).strip(), '%d-%m-%Y').date()

            # A logical check: an admission date should not be in the future.
            if date_obj > date.today():
                validation_errors.append(f"Invalid Admission Date: '{raw_admission_date}' cannot be in the future.")
            else:
                # Standardize the date into YYYY-MM-DD format for the database
                standardized_admission_date = date_obj.strftime('%Y-%m-%d')

        except ValueError:
            # This block catches errors if the date string doesn't match the format
            validation_errors.append(f"Invalid Admission Date format: '{raw_admission_date}'. Expected format is DD-MM-YYYY.")

    # --- FINAL CHECK: Return all validation errors if any were found ---
    if validation_errors:
        return None, None, validation_errors

    # --- 3. Check for an exact duplicate row in the master table before proceeding ---
    duplicate_check_query = f"""
        SELECT 1 FROM {master_table}
        WHERE institution_code = %s AND full_name = %s AND email_id = %s AND contact_no = %s AND gender = %s 
        
    """
    values_for_check = (
        institution_code, record.get('full_name'), record.get('email_id'), record.get('contact_no'), record.get('gender')
    )
    cursor.execute(duplicate_check_query, values_for_check)
    if cursor.fetchone():
        validation_errors.append("Error: A record with this exact data already exists in the master table.")
        return None, None, validation_errors
    
    # --- 4. Check for existing student record before inserting ---
    check_query = f"SELECT id FROM {master_table} WHERE institution_code = %s AND full_name = %s"
    cursor.execute(check_query, (institution_code, record.get('full_name'),))
    existing_record = cursor.fetchone()

    if existing_record:
        update_query = f"UPDATE {master_table} SET is_active = 0 WHERE id = %s"
        cursor.execute(update_query, (existing_record['id'],))

    # --- 5. Prepare the final INSERT query and values ---
    master_insert_query = f"""
        INSERT INTO {master_table} (institution_code, full_name, email_id, contact_no, gender)
        VALUES (%s, %s, %s, %s, %s)
    """
    values = (
        institution_code, record.get('full_name'), record.get('email_id'), record.get('contact_no'), record.get('gender')
    )
    
    return master_insert_query, values, validation_errors
