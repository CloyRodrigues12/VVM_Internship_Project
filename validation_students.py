"""
This module contains helper functions for validating and preparing student data from different institute staging tables for insertion into the master table.
"""
from datetime import datetime, date
import re
import difflib
from collections import OrderedDict

def validate_and_format_name(name_string):
    if not name_string or not str(name_string).strip():
        return False, f"Invalid name: '{name_string}'. Name cannot be empty."
    
    cleaned_name = str(name_string).strip()
    
    # Allow names with dots (e.g., for initials)
    if not all(char.isalpha() or char.isspace() or char == '.' for char in cleaned_name):
        return False, f"Invalid name: '{name_string}'. Only alphabets, spaces, and dots are allowed."

    formatted_name = " ".join(word.capitalize() for word in cleaned_name.split())
    return formatted_name

def validate_and_clean_mobile_number(raw_value):
    """
    Cleans and validates an Indian mobile number from various formats.
    
    Args:
        raw_value: The input value, which can be a string, int, float, or None.
        
    Returns:
        A clean, 10-digit string if the number is valid.
        None if the input is empty or the number is invalid.
    """
    # Rule 1 & 2: If value is missing or empty, return None.
    if raw_value is None or not str(raw_value).strip():
        return None

    # Convert to string and handle numbers read as floats (e.g., 9876543210.0)
    num_str = str(raw_value).strip()
    if num_str.endswith('.0'):
        num_str = num_str[:-2]

    # Remove all non-digit characters (like '+', '-', '(', ')', ' ')
    cleaned_num = re.sub(r'\D', '', num_str)

    # Handle common prefixes like '91' or '0'
    if len(cleaned_num) == 12 and cleaned_num.startswith('91'):
        cleaned_num = cleaned_num[2:]
    elif len(cleaned_num) == 11 and cleaned_num.startswith('0'):
        cleaned_num = cleaned_num[1:]

    # Final check: Must be 10 digits and start with 6, 7, 8, or 9.
    if re.match(r'^[6-9]\d{9}$', cleaned_num):
        return cleaned_num
    
    # Rule 3: If the number is invalid at this point, return None.
    return None

# A list of standardized occupations.
OCCUPATION_STANDARDS = [
    'Businessman',
    'Engineer',
    'Doctor',
    'Legal Professional',
    'Professor/Teacher',
    'IT / Software Professional',
    'Government Servant',
    'Defense / Law Enforcement',
    'Housewife/Homemaker',
    'Private Sector',
    'Skilled Worker / Tradesman',
    'Farmer/Agriculture',
    'Labourer',
    'Retired',
    'Student',
    'Unemployed',
    'NRI / Working Abroad',
    'Other'  # A fallback category for un-mappable entries
]

# Set a threshold for fuzzy matching (0.0 to 1.0)
SIMILARITY_THRESHOLD = 0.8

from collections import OrderedDict
import re # Make sure 're' is imported

def validate_and_standardize_occupation(occupation_string):
    """
    Robustly standardizes an occupation using a layered approach:
    1. Prioritized keyword matching (using whole word matching).
    2. Powerful fuzzy matching for typos.
    3. Smart fallback to a cleaned, title-cased version of the original input.
    """
    placeholders = {'NA', 'N/A', '-', '--'}
    if not occupation_string or not str(occupation_string).strip() or str(occupation_string).strip().upper() in placeholders:
        return None

    cleaned_input = str(occupation_string).strip()
    cleaned_lower = cleaned_input.lower()

    if not all(c.isalnum() or c.isspace() or c in './-' for c in cleaned_lower):
        return None

    # --- Stage 1: Prioritized Keyword-based Matching (FIXED) ---
    keyword_map = OrderedDict([
        # Most specific phrases first
        ('government servant', 'Government Servant'),
        ('private service', 'Private Sector'),
        ('self employed', 'Businessman'),
        ('house wife', 'Housewife/Homemaker'),
        ('home maker', 'Housewife/Homemaker'),
        # General keywords
        ('govt', 'Government Servant'),
        ('army', 'Defense / Law Enforcement'), ('navy', 'Defense / Law Enforcement'), ('air force', 'Defense / Law Enforcement'), ('police', 'Defense / Law Enforcement'), ('defense', 'Defense / Law Enforcement'), ('military', 'Defense / Law Enforcement'),
        ('housewife', 'Housewife/Homemaker'), ('homemaker', 'Housewife/Homemaker'),
        ('business', 'Businessman'), ('shopkeeper', 'Businessman'),
        ('teacher', 'Professor/Teacher'), ('professor', 'Professor/Teacher'),
        ('doctor', 'Doctor'), ('engineer', 'Engineer'),
        ('lawyer', 'Legal Professional'), ('advocate', 'Legal Professional'),
        ('software', 'IT / Software Professional'), ('it', 'IT / Software Professional'),
        ('developer', 'IT / Software Professional'), ('programmer', 'IT / Software Professional'),
        ('private', 'Private Sector'), ('service', 'Private Sector')
    ])
    
    for keyword, standard in keyword_map.items():
        # UPDATED LOGIC: Use regex word boundaries (\b) to match whole words/phrases only.
        # This prevents 'it' from matching inside 'electrition'.
        if re.search(r'\b' + re.escape(keyword) + r'\b', cleaned_lower):
            return standard

    # --- Stage 2: Powerful Fuzzy Matching ---
    all_known_spellings = list(set([s.lower() for s in OCCUPATION_STANDARDS] + list(keyword_map.keys())))
    matches = difflib.get_close_matches(
        cleaned_lower, all_known_spellings, n=1, cutoff=SIMILARITY_THRESHOLD
    )
    if matches:
        matched_term = matches[0]
        if matched_term in keyword_map: return keyword_map[matched_term]
        for standard_job in OCCUPATION_STANDARDS:
            if standard_job.lower() == matched_term: return standard_job

    # --- Stage 3: Smart Fallback ---
    return cleaned_input.title()



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
    # We split the logic: the student's name is mandatory, but parents' names are optional.
    
    # 1. Student's Name (Mandatory)
    raw_student_name = record.get('name_of_the_applicant')
    result = validate_and_format_name(raw_student_name)
    if isinstance(result, tuple):
        # For the mandatory student name, a failure is a critical error.
        validation_errors.append(f"Student Name Error: {result[1]}")
    else:
        student_name = result

    # 2. Parents' Names (Optional)
    # For these optional fields, if a value is present but invalid (e.g., '---'),
    # we will treat it as a placeholder and simply leave the field as None without raising an error.
    for name_field, display_name in [('name_of_father', "Father's"), ('name_of_mother', "Mother's")]:
        raw_name = record.get(name_field)
        
        if raw_name and str(raw_name).strip():
            # We attempt to validate and format the name.
            result = validate_and_format_name(raw_name)
            
            # Only assign the name if it was successfully validated and formatted.
            if not isinstance(result, tuple):
                if name_field == 'name_of_father':
                    father_name = result
                elif name_field == 'name_of_mother':
                    mother_name = result
            # If validation fails (result is a tuple), we intentionally do nothing.
            # The corresponding variable (father_name or mother_name) will remain None.
    
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
    
# --- 7. Validate all mobile numbers (using forgiving logic) ---
    
    # We will use the 'validate_and_clean_mobile_number' helper function directly.
    # It will return a valid 10-digit string or None, and will not raise errors.

    mobile = validate_and_clean_mobile_number(record.get('mobile'))
    alternate_mobile = validate_and_clean_mobile_number(record.get('alternate_mobile'))
    father_mobile = validate_and_clean_mobile_number(record.get('father_mobile'))
    mother_mobile = validate_and_clean_mobile_number(record.get('mother_mobile'))

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
            standardized_blood_group = 'Unknown' # Default to 'Unknown' if not valid
            

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
        return None, None, [f"Error: An active record with Admission No (FORM NUMBER) '{form_number}' already exists for this institution."]
    
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
            student_reference_id, institution_code, uploaded_file_id, admission_no, stream, pr_no,admission_scheme, admission_date,
            admission_feepayment_time, student_name, date_of_birth, full_address, gender, 
            student_category, religion, blood_group, email_address, city, state, pin_code, 
            mobile_number, alt_mobile_number, fathers_mobile_number, mothers_mobile_number, 
            fathers_name, mothers_name, fathers_occupation, mothers_occupation, 
            fathers_occupation_category, mothers_occupation_category, nationality, 
            name_of_the_institution_attended_earlier, board_name, passing_year, xii_stream,xii_max_marks,xii_marks_obtained,xii_sub_combination, passsing_percentage,xii_passing_class,pwd_category_and_Percentage,urban_rural_category
        ) VALUES (
           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s
        )
    """
    values = (
        record.get('admission_transaction_number'), institution_code, record.get('uploaded_file_id'), form_number, record.get('programme_name'), 
        record.get('enrollment_number'),record.get('admission_scheme'), admission_date, admission_feepayment_time, student_name, 
        date_of_birth, full_address, record.get('gender'), standardized_admission_category, 
        standardized_religion, standardized_blood_group, standardized_email, city, state, pincode_str, 
        mobile, alternate_mobile, father_mobile, mother_mobile, father_name, mother_name, 
        record.get('father_occupation'), record.get('mother_occupation'), father_occupation_category, 
        mother_occupation_category, nationality, record.get('xii_name_of_the_institution'), 
        record.get('xii_board'), xii_passing_year_val, record.get('xii_stream'),record.get('xii_maximum_marks'),record.get('xii_marks_obtained'),record.get('xii_subject_combination'),xii_percentage,xii_division, pwd_category_and_percentage, record.get('urban_rural_semi_urban_metro_area')      
    )

    return master_insert_query, values, []


#validation for RMS and VVA
def _validate_and_prepare_student_rms(cursor, record, institution_code, master_table, academic_year, academic_quarter):
    
    validation_errors = []


    # --- 2. Admission Number Validation ---
    admission_no = None # Initialize a cleaned variable
    raw_admission_no = record.get('admission_no')

    if not raw_admission_no or not str(raw_admission_no).strip():
        validation_errors.append("Missing mandatory field: Admission Number")
    else:
        # Clean the admission number by removing leading/trailing whitespace
        admission_no = str(raw_admission_no).strip()


    # --- 4. Admission Date Validation ---
    standardized_admission_date = None 
    raw_admission_date = record.get('admission_date')
    if not raw_admission_date or not str(raw_admission_date).strip():
        validation_errors.append("Missing mandatory field: Admission Date")
    else:
        # Pre-process the string to remove any time component
        date_string = str(raw_admission_date).strip().split(' ')[0]
        # List of expected date formats, now including YYYY-MM-DD
        possible_formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%Y/%m/%d']
        date_obj = None
        # Try parsing the date with each possible format
        for fmt in possible_formats:
            try:
                date_obj = datetime.strptime(date_string, fmt).date()
                break # If parsing is successful, break the loop
            except ValueError:
                continue # If the format doesn't match, continue
        # After the loop, check if parsing was successful
        if date_obj is None:
            validation_errors.append(f"Invalid Admission Date format: '{raw_admission_date}'. Expected DD/MM/YYYY, DD-MM-YYYY, or YYYY-MM-DD.")
        else:
            standardized_admission_date = date_obj.strftime('%Y-%m-%d')


# --- 3. Date of Birth Validation ---
    date_of_birth = None 
    raw_dob = record.get('date_of_birth')

    if not raw_dob or not str(raw_dob).strip():
        validation_errors.append("Missing mandatory field: Date of Birth")
    else:
        # Pre-process the string to remove any time component
        date_string = str(raw_dob).strip().split(' ')[0]
        
        # List of expected date formats, now including YYYY-MM-DD
        possible_formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%Y/%m/%d']
        date_obj = None
        
        # Try parsing the date with each possible format
        for fmt in possible_formats:
            try:
                date_obj = datetime.strptime(date_string, fmt).date()
                break 
            except ValueError:
                continue
        
        # After the loop, check if parsing was successful
        if date_obj is None:
            validation_errors.append(f"Invalid Date of Birth format: '{raw_dob}'. Expected DD/MM/YYYY, DD-MM-YYYY, or YYYY-MM-DD.")
        else:
            # Additional logical checks for a valid date of birth
            if date_obj > date.today():
                validation_errors.append(f"Invalid Date of Birth: '{raw_dob}' cannot be in the future.")
            elif date_obj.year < 1950: 
                validation_errors.append(f"Invalid Date of Birth: '{raw_dob}'. Year seems unusually early.")
            else:
                date_of_birth = date_obj.strftime('%Y-%m-%d')

    # --- 12. Validate 'email' format ---
    email = record.get('e_mail')
    standardized_email = None
    if email:
        email = str(email).strip()
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            standardized_email = email.lower()
        else:
            validation_errors.append(f"Invalid email format: '{email}'.")

    if institution_code == 'RMS':
        student_reference_id= record.get('gen_reg_no')
    elif institution_code == 'VVA':
        student_reference_id= admission_no

    # --- 5. Batch Details (Class, Section, Stream, Year) Extraction ---
    student_class, section, stream, batch_year = None, None, None, None
    raw_batch = record.get('batch')

    if not raw_batch or not str(raw_batch).strip():
        validation_errors.append("Missing mandatory field: Batch")
    else:
        cleaned_batch = str(raw_batch).strip()

        if institution_code == 'RMS':
            # Expected format: XII-COM - 2025-26 A
            match = re.match(r'^(\w+)-(\w+)\s*-\s*(\d{4}-\d{2})\s*([A-Z])$', cleaned_batch, re.IGNORECASE)
            if match:
                raw_class, raw_stream, raw_year, section = match.groups()

                # --- Standardize Batch Year (YYYY-YY to YYYY-YYYY) ---
                start_year, end_year_short = raw_year.split('-')
                century = start_year[:2] # e.g., '20' from '2025'
                batch_year = f"{start_year}-{century}{end_year_short}"

                # --- Standardize Class (Roman to Numeric) ---
                class_map = {'IX': '9', 'X': '10', 'XI': '11', 'XII': '12'}
                student_class = class_map.get(raw_class.upper())
                if not student_class:
                    validation_errors.append(f"Invalid class value '{raw_class}' in batch '{cleaned_batch}'.")

                # --- Standardize Stream ---
                stream_map = {'COM': 'Commerce', 'SCI': 'Science'}
                stream = stream_map.get(raw_stream.upper())
                if not stream:
                    validation_errors.append(f"Unknown stream '{raw_stream}' in batch '{cleaned_batch}'.")

            else:
                validation_errors.append(f"Invalid RMS batch format: '{raw_batch}'. Expected 'Class-Stream - YYYY-YY S'.")

        elif institution_code == 'VVA':
            # Expected format: CL-12 - B 25-26
            match = re.match(r'^CL-(\d+)\s*-\s*([A-Z])\s*(\d{2}-\d{2})$', cleaned_batch, re.IGNORECASE)
            if match:
                student_class, section, raw_year = match.groups()
                
                # --- Standardize Batch Year (YY-YY to YYYY-YY) ---
                start_year, end_year = raw_year.split('-')
                batch_year = f"20{start_year}-20{end_year}" # or simply f"20{raw_year}"
                
                # --- Assign Stream based on Class (as per VVA Prospectus) ---
                try:
                    class_num = int(student_class)
                    if 1 <= class_num <= 5:
                        stream = 'Primary'
                    elif 6 <= class_num <= 8:
                        stream = 'Middle School'
                    elif 9 <= class_num <= 10:
                        stream = 'Secondary'
                    elif 11 <= class_num <= 12:
                        stream = 'Senior Secondary'
                    else:
                        stream = None # Or 'N/A' for classes outside the 1-12 range
                except ValueError:
                    stream = None # If student_class is not a valid number

            else:
                validation_errors.append(f"Invalid VVA batch format: '{raw_batch}'. Expected 'CL-Class - Section YY-YY'.")
        
        else:
            validation_errors.append(f"Batch parsing logic not implemented for institution code: '{institution_code}'.")


    # --- Name Validations (Student, Father, Mother) ---
    full_name, father_full_name, mother_full_name = None, None, None

    # 1. Student's Full Name (Mandatory)
    # This field is required, so we expect validate_and_format_name to return a valid name.
    raw_student_name = record.get('full_name')
    result = validate_and_format_name(raw_student_name)
    if isinstance(result, tuple): # An error was returned
        validation_errors.append(f"Student Name Error: {result[1]}")
    else:
        full_name = result

# 2. Parents' Names (Optional) - More Robust Logic
    # For optional fields, if a value is present but invalid, we will
    # treat it as a placeholder and set the field to None without raising an error.
    for field_key, display_name in [('father_full_name', "Father's Full Name"), 
                                    ('mother_full_name', "Mother's Full Name")]:
        raw_name = record.get(field_key)
        
        if raw_name and str(raw_name).strip():
            # We attempt to validate and format the name.
            result = validate_and_format_name(raw_name)
            
            # The 'result' will either be a formatted name or a (False, error_message) tuple.
            if not isinstance(result, tuple):
                # The name was valid and successfully formatted. Assign it.
                if field_key == 'father_full_name':
                    father_full_name = result
                elif field_key == 'mother_full_name':
                    mother_full_name = result
            # If 'result' is a tuple, it means validation failed.
            # We do nothing, intentionally. The variable (e.g., father_full_name)
            # will keep its default value of None, and no error is raised.


    # --- Gender Standardization ---
    standardized_gender = None
    raw_gender = record.get('gender') # Assuming the source column is 'gender'

    if not raw_gender or not str(raw_gender).strip():
        validation_errors.append("Missing mandatory field: Gender")
    else:
        # Clean the input: remove whitespace and convert to uppercase for consistent matching.
        cleaned_gender = str(raw_gender).strip().upper()

        # Define the mapping from common inputs to the standardized values.
        gender_map = {
            'M': 'MALE',
            'MALE': 'MALE',
            'F': 'FEMALE',
            'FEMALE': 'FEMALE',
            'O': 'OTHER',
            'OTHER': 'OTHER',
        }

        # Look up the cleaned input in the map. .get() returns None if not found.
        standardized_gender = gender_map.get(cleaned_gender)

        # If the lookup failed, the provided gender is not a recognized value.
        if standardized_gender is None:
            validation_errors.append(f"Invalid gender value: '{raw_gender}'. Expected M/F or Male/Female.")


    # --- Roll Number Validation (as Optional Integer) ---
    validated_roll_number = None
    raw_roll_number = record.get('roll_number')

    # Only perform validation if a roll number value is actually present.
    if raw_roll_number is not None and str(raw_roll_number).strip() != '':
        try:
            # We first convert to a float and then to an int. This robustly handles
            # whole numbers that might be formatted as decimals (e.g., 123.0).
            roll_no_as_int = int(float(raw_roll_number))
            # If a roll number is provided, it must be a positive integer.
            if roll_no_as_int > 0:
                validated_roll_number = roll_no_as_int
            else:
                validation_errors.append(f"Invalid Roll Number: '{raw_roll_number}'. If provided, it must be a positive number.")
        except (ValueError, TypeError):
            # This error occurs if the provided value is not a valid number.
            validation_errors.append(f"Invalid Roll Number: '{raw_roll_number}'. If provided, it must be a whole number.")
            
    # If raw_roll_number was empty or None, the code above is skipped,
    # and validated_roll_number correctly remains None.


        # --- 9. Validate and standardize 'admission_category' ---
    student_category = record.get('student_category')
    standardized_student_category = None
    
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
        'GENERAL': 'UR',
    }

    if student_category:
        normalized = str(student_category).strip().upper()
        standardized_student_category = category_mapping.get(normalized)


        # ---Validate 'blood_group' ---
    blood_group = record.get('blood_group')
    standardized_blood_group= None
    ALLOWED_BLOOD_GROUPS = {'A+', 'A-', 'B+', 'B-', 'O+', 'O-', 'AB+', 'AB-'}
    if blood_group:
        normalized = str(blood_group).strip().upper()
        if normalized in ALLOWED_BLOOD_GROUPS:
            standardized_blood_group = normalized
        else:
            standardized_blood_group = 'Unknown' # Default to 'Unknown' if not valid


    # --- Religion Standardization ---
    standardized_religion = None
    raw_religion = record.get('religion') # Assuming the source column is 'religion'

    if not raw_religion or not str(raw_religion).strip():
        validation_errors.append("Missing mandatory field: Religion")
    else:
        # Clean the input: remove whitespace and convert to uppercase for consistent matching.
        cleaned_religion = str(raw_religion).strip().upper()

        # Map common inputs from the dropdown/data to a standard format.
        religion_map = {
            'HINDU': 'HINDUISM',
            'HINDUISM': 'HINDUISM',
            'MUSLIM': 'ISLAM',
            'ISLAM': 'ISLAM',
            'CHRISTIAN': 'CHRISTIANITY',
            'CHRISTIANITY': 'CHRISTIANITY',
            'CATHOLIC': 'CHRISTIANITY',
            'SIKH': 'SIKHISM',
            'SIKHISM': 'SIKHISM',
            'BUDDHIST': 'BUDDHISM',
            'BUDDHISM': 'BUDDHISM',
            'JAIN': 'JAINISM',
            'JAINISM': 'JAINISM',
            # Note: 'Parsi' refers to the people; the religion is 'Zoroastrianism'. 
            # We map it here for better data standardization.
            'PARSI': 'ZOROASTRIANISM',
            'ZOROASTRIANISM': 'ZOROASTRIANISM',
            'OTHERS': 'OTHERS',
            'OTHER': 'OTHERS'
        }

        # Look up the cleaned input in the map. .get() returns None if not found.
        standardized_religion = religion_map.get(cleaned_religion)

        # If the lookup failed, the provided religion is not a recognized value.
        if standardized_religion is None:
            validation_errors.append(f"Invalid or unmapped religion: '{raw_religion}'.")


# --- Goa-Specific City and State Validation (CORRECTED) ---
    standardized_city = None
    standardized_state = 'Goa'

    raw_city = record.get('city')
    cleaned_raw_city = str(raw_city or '').strip()

    if not cleaned_raw_city or cleaned_raw_city == '-':
        standardized_city = 'Unknown'
    else:
        cleaned_city_input = cleaned_raw_city.upper()

        # VERIFIED: An authoritative list of Goa's 14 official Municipal Corporation & Councils.
        GOA_CITIES = [
            'PANAJI', 'MARGAO', 'VASCO DA GAMA', 'MAPUSA', 'PONDA', 'BICHOLIM',
            'CANACONA', 'CUNCOLIM', 'CURCHOREM', 'PERNEM', 'QUEPEM', 'SANGUEM',
            'SANQUELIM', 'VALPOI'
        ]
        
        # VERIFIED: Mapping for common city aliases and alternate spellings.
        GOA_CITY_ALIASES = {
            'PANJIM': 'PANAJI', 'MADGAON': 'MARGAO', 'VASCO': 'VASCO DA GAMA',
            'MORMUGAO': 'VASCO DA GAMA', 'SANKHALI': 'SANQUELIM'
        }

        # VERIFIED: Mapping for important suburbs & census towns to their nearest official city.
        GOA_SUBURB_MAP = {
            'FATORDA': 'MARGAO', 'NAVELIM': 'MARGAO', 'COLVA': 'MARGAO', 'SHIRODA': 'PONDA',
            'CALANGUTE': 'MAPUSA', 'BAGA': 'MAPUSA', 'ANJUNA': 'MAPUSA', 'CANDOLIM': 'MAPUSA'
        }
        
        found_city = None

        # --- Validation Flow ---
        # 1. Direct checks (Alias, Suburb, Official City)
        if cleaned_city_input in GOA_CITY_ALIASES:
            found_city = GOA_CITY_ALIASES[cleaned_city_input].title()
        elif cleaned_city_input in GOA_SUBURB_MAP:
            found_city = GOA_SUBURB_MAP[cleaned_city_input].title()
        elif cleaned_city_input in GOA_CITIES:
            # CORRECTED LINE: Applying .title() to the input string.
            found_city = cleaned_city_input.title()
        
        # 2. Fuzzy matching for typos
        elif not found_city:
            all_known_names = GOA_CITIES + list(GOA_CITY_ALIASES.keys()) + list(GOA_SUBURB_MAP.keys())
            matches = difflib.get_close_matches(cleaned_city_input, all_known_names, n=1, cutoff=0.8)
            if matches:
                matched_name = matches[0]
                if matched_name in GOA_CITY_ALIASES: found_city = GOA_CITY_ALIASES[matched_name].title()
                elif matched_name in GOA_SUBURB_MAP: found_city = GOA_SUBURB_MAP[matched_name].title()
                else: found_city = matched_name.title()

        # 3. Substring check for cases like "Margao Navelim"
        if not found_city:
            for suburb, city in GOA_SUBURB_MAP.items():
                if suburb in cleaned_city_input:
                    found_city = city.title(); break
            if not found_city:
                for city in GOA_CITIES:
                    if city in cleaned_city_input:
                        found_city = city.title(); break
        
        # Final assignment
        if found_city:
            standardized_city = found_city
        else:
            standardized_city = 'Unknown'

# --- Full Address Combination (using standardized city/state) ---
    full_address = None
    address_parts = []

    # 1. Handle Address Lines 1 & 2 from the raw record
    for field in ['address_line_1', 'address_line_2']:
        value = record.get(field)
        if value and str(value).strip():
            address_parts.append(str(value).strip().title())

    # 2. Handle City, using the new standardization rule
    # This logic assumes the 'standardized_city' variable was set in the previous step.
    if standardized_city == 'Unknown':
        # If the city was not recognized, fall back to the original value for the address string.
        original_city = record.get('city')
        if original_city and str(original_city).strip():
            address_parts.append(str(original_city).strip().title())
    elif standardized_city:
        # Otherwise, use the successfully standardized and corrected city name.
        address_parts.append(standardized_city)

    # 3. Handle State, using the standardized value
    # This logic assumes the 'standardized_state' variable was set in the previous step.
    if standardized_state:
        address_parts.append(standardized_state)

    # 4. Handle Pin Code from the raw record
    raw_pincode = record.get('pin_code')
    if raw_pincode and str(raw_pincode).strip():
        pincode_str = str(raw_pincode).strip().replace('.0', '')
        if pincode_str:
            address_parts.append(pincode_str)

    # Join all the collected parts to create the final, clean address.
    if address_parts:
        full_address = ', '.join(address_parts)

    
    pincode = record.get('pin_code')
    if pincode:
        pincode_str = str(pincode).strip().replace('.0', '') # Handle floats like 403602.0
        if not (pincode_str.isdigit() and len(pincode_str) == 6):
            validation_errors.append(f"Invalid pincode format: '{pincode}'. Must be a 6-digit number.")    

    # --- Phone Number Validation (Optional Fields) ---
    mobile_number = validate_and_clean_mobile_number(record.get('mobile'))
    alt_mobile_number = validate_and_clean_mobile_number(record.get('phone'))
    fathers_mobile_number = validate_and_clean_mobile_number(record.get('father_mobile_phone'))
    mothers_mobile_number = validate_and_clean_mobile_number(record.get('mother_mobile_phone'))

    # --- Nationality Standardization ---
    standardized_nationality = None
    raw_nationality = record.get('nationality')

    if not raw_nationality or not str(raw_nationality).strip():
        validation_errors.append("Missing mandatory field: Nationality")
    else:
        cleaned_nationality = str(raw_nationality).strip()
        
        # We can now use a direct equality check since the input is clean.
        if cleaned_nationality.upper() == 'INDIA':
            standardized_nationality = 'Indian'
        
        # For all other countries, the logic remains the same.
        else:
            country_name = cleaned_nationality.split('(')[0].strip()
            if country_name:
                standardized_nationality = country_name.title()
            else:
                validation_errors.append(f"Invalid nationality format: '{raw_nationality}'")

    # --- Previous Institution Name Standardization ---
    institution_attended_earlier = None
    raw_institution_name = None

    # 1. Get the raw value from the correct column based on the institution_code.
    if institution_code == 'RMS':
        raw_institution_name = record.get('name_of_last_school_attended')
    elif institution_code == 'VVA':
        raw_institution_name = record.get('name_of_school_attended_earlier')

    # 2. If a value was found, clean and standardize it.
    if raw_institution_name and str(raw_institution_name).strip():
        # Remove any extra leading/trailing spaces and convert to Title Case.
        institution_attended_earlier = str(raw_institution_name).strip().title()

    # If no value was found in the source data, institution_attended_earlier will correctly remain None.

    # --- Percentage Standardization (Class X) ---
    standardized_percentage_x = None
    raw_percentage = None

# --- Percentage Standardization (Class X only - More Robust) ---
    standardized_percentage_x = None
    raw_percentage = None

    # 1. Get the raw value from the correct column based on the institution_code
    if institution_code == 'RMS':
        raw_percentage = record.get('percentage_obtained_std_x')
    elif institution_code == 'VVA':
        raw_percentage = record.get('percentage_class_x')

    # 2. Only proceed if a value is provided
    if raw_percentage is not None and str(raw_percentage).strip() != '':
        
        try:
            # 3. Clean the input string to isolate the number
            cleaned_string = str(raw_percentage).lower().replace('percent', '').replace('%', '').strip()
            percentage_value = float(cleaned_string)
            
            # --- NEW "Wise" Logic to Handle Different Formats ---
            
            # A. If the number is between 0 and 1 (e.g., 0.75), assume it's a decimal representation.
            if 0 < percentage_value <= 1:
                percentage_value *= 100 # Convert 0.75 to 75.0
            
            # B. If the number is between 1 and 10, assume it's a CGPA/GPA score.
            #    We'll convert it to an approximate percentage by multiplying by 10.
            elif 1 < percentage_value <= 10:
                percentage_value *= 10 # Convert 8.5 to 85.0
            
            # C. A value of 0 is just 0. We don't need to do anything.

            # 4. Final validation: Check if the (potentially converted) value is in the 0-100 range.
            if 0 <= percentage_value <= 100:
                # 5. Standardize the final value to 2 decimal places
                standardized_percentage_x = round(percentage_value, 2)
            
            # If the value is still outside the range (e.g., an input of 11), it will be invalid.

        except (ValueError, TypeError):
            # If conversion fails, it's considered invalid, and the value remains None.
            pass


    # --- Occupation Standardization (Optional) ---
    father_occupation_category = None
    mother_occupation_category = None

    # We use the updated helper function which returns a valid occupation or None.
    father_occupation_category = validate_and_standardize_occupation(record.get('father_occupation'))
    mother_occupation_category = validate_and_standardize_occupation(record.get('mother_occupation'))

    # --- FINAL CHECK: Return all validation errors if any were found ---
    if validation_errors:
        return None, None, validation_errors

    # --- 3. Check for an exact duplicate row in the master table before proceeding ---
    duplicate_check_query = f"""
        SELECT 1 FROM {master_table}
        WHERE institution_code = %s AND email_address = %s
        
    """
    values_for_check = (
        institution_code, record.get('email_address'))
    
    cursor.execute(duplicate_check_query, values_for_check)
    if cursor.fetchone():
        validation_errors.append("Error: A record with this exact data already exists in the master table.")
        return None, None, validation_errors
    
    # --- 4. Check for existing student record before inserting ---
    check_query = f"SELECT master_id FROM {master_table} WHERE institution_code = %s AND student_name = %s"
    cursor.execute(check_query, (institution_code, record.get('student_name'),))
    existing_record = cursor.fetchone()

    if existing_record:
        update_query = f"UPDATE {master_table} SET is_active = 0 WHERE master_id = %s"
        cursor.execute(update_query, (existing_record['master_id'],))

    # --- 5. Prepare the final INSERT query and values ---
    master_insert_query = f"""
        INSERT INTO {master_table} (student_reference_id,uploaded_file_id,institution_code,admission_no,admission_date,class,section,stream,	batch_year,roll_number,date_of_birth,religion,blood_group,gender,student_category,student_name,fathers_name,mothers_name,email_address,full_address,state,city,pin_code,mobile_number,alt_mobile_number,fathers_mobile_number,mothers_mobile_number,nationality,mother_tongue,name_of_the_institution_attended_earlier,	passsing_percentage,fathers_occupation,mothers_occupation,fathers_occupation_category,mothers_occupation_category)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    values = (
         student_reference_id,record.get('uploaded_file_id'),institution_code,admission_no,standardized_admission_date,student_class,section,stream,batch_year,validated_roll_number,date_of_birth,standardized_religion,standardized_blood_group,standardized_gender,standardized_student_category,full_name,father_full_name,mother_full_name,standardized_email,full_address,standardized_state,standardized_city,pincode,mobile_number,alt_mobile_number,fathers_mobile_number,mothers_mobile_number,standardized_nationality,record.get('mother_tongue'),institution_attended_earlier,standardized_percentage_x,record.get('father_occupation'),record.get('mother_occupation'),father_occupation_category,mother_occupation_category
    )
    
    return master_insert_query, values, validation_errors
