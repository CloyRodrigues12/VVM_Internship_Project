# mappings.py

# This file contains the mappings for different data upload formats to the corresponding database table columns and are used to process and insert data into the staging tables and to create user views.

# Mappings for sdcce_grkcl_student_details_upload
# This mapping is used for data from SHREE DAMODAR COLLEGE OF COMMERCE & ECONOMICS (SDCCE) and GOVIND RAMNATH KARE COLLEGE OF LAW (GRKCL).
SDCCE_GRKCL_STUDENT_MAPPING = {
    'FORM NUMBER': 'form_number',
    'Programme Name': 'programme_name',
    'OU': 'ou',
    'Payment Status': 'payment_status',
    'Admission Scheme': 'admission_scheme',
    'Enrollment Number': 'enrollment_number',
    'Payment Start Date': 'payment_start_date',
    'Payment End Date': 'payment_end_date',
    'Admission List': 'admission_list',
    'Admission Status': 'admission_status',
    'Adimission Remark': 'admission_remark',
    'Admission Category': 'admission_category',
    'Admission Fee Category': 'admission_fee_category',
    'Admission Order ID': 'admission_order_id',
    'Admission Transaction Number': 'admission_transaction_number',
    'Admission Bank Reference Number': 'admission_bank_reference_number',
    'Admission Fee To Be Paid': 'admission_fee_to_be_paid',
    'Admission Fee Paid': 'admission_fee_paid',
    'Admission Fee Paid On': 'admission_fee_paid_on',
    'Cancel Request': 'cancel_request',
    'Cancel Requested On': 'cancel_requested_on',
    'Cancel Requested Reason': 'cancel_requested_reason',
    'Do You Want To Accept The Cancellation?': 'do_you_want_to_accept_the_cancellation',
    'Cancel Accepted On': 'cancel_accepted_on',
    'Cancel Remarks': 'cancel_remarks',
    'Withdraw Request': 'withdraw_request',
    'Withdraw Requested On': 'withdraw_requested_on',
    'Withdraw Requested Reason': 'withdraw_requested_reason',
    'DO YOU WANT TO ACCEPT THE WITHDRAWAL?': 'do_you_want_to_accept_the_withrawal',
    'Withdraw Accepted On': 'withdraw_accepted_on',
    'Withdraw Accepted Remarks': 'withdraw_accepted_remarks',
    'Group Applicable': 'group_applicable',
    'Faculty 1': 'faculty_1',
    'Faculty 2': 'faculty_2',
    'Faculty 3': 'faculty_3',
    'Subject 1 (Major)': 'subject_1_major',
    'Subject 2 (Major)': 'subject_2_major',
    'Subject 3 (Major Elective)': 'subject_3_major_elective',
    'Subject 4 (Minor)': 'subject_4_minor',
    'Subject 5 (Vocational)': 'subject_5_vocational',
    'Subject 6 (Co-curricular)': 'subject_6_co_curricular',
    'XII Scaled Marks': 'xii_scaled_marks',
    'Merit Points': 'merit_points',
    'Merit Index': 'merit_index',
    'Order ID': 'order_id',
    'Alias Order Number': 'alias_order_number',
    'Registration Transaction ID': 'registration_transaction_id',
    'Registration Amount Paid': 'registration_amount_paid',
    'Registration Payment Date': 'registration_payment_date',
    'Submission Status': 'submission_status',
    'State': 'state',
    'Name of the Applicant': 'name_of_the_applicant',
    'Gender': 'gender',
    'Marital Status': 'marital_status',
    'Alternate Mobile': 'alternate_mobile',
    'DOB Day': 'dob_day',
    'DOB Month': 'dob_month',
    'DOB Year': 'dob_year',
    'Age (as Referenced)': 'age_as_referenced',
    'Category': 'category',
    'Sub Category': 'sub_category',
    'Certificate Valid/Available': 'certificate_valid_available',
    'Certificate Number': 'certificate_number',
    'Certificate Issuing Date': 'certificate_issuing_date',
    'Family Income(EWS)': 'family_income_ews',
    'Blood Group': 'blood_group',
    'Religion': 'religion',
    'Are you Citizen of India': 'are_you_citizen_of_india',
    'Other Nationality': 'other_nationality',
    'Domicile of State': 'domicile_of_state',
    'Email': 'email',
    'Mobile': 'mobile',
    'ID Proof': 'id_proof',
    'ID Proof Number': 'id_proof_number',
    'Existing University Student': 'existing_university_student',
    'Existing Registration No': 'existing_registration_no',
    'Existing College/Department Name': 'existing_college_department_name',
    'Existing University Student School': 'existing_university_student_school',
    'Existing University Student Programme': 'existing_uni_student_programme',
    'EXISTING UNIVERSITY STUDENT YEAR OF REGISTRATION': 'existing_university_student_year_of_registration',
    'Name of Father': 'name_of_father',
    'Father Qualification': 'father_qualification',
    'Father Occupation': 'father_occupation',
    'Father Mobile': 'father_mobile',
    'Father Office Address': 'father_office_address',
    'Name of Mother': 'name_of_mother',
    'Mother Qualification': 'mother_qualification',
    'Mother Occupation': 'mother_occupation',
    'Mother Mobile': 'mother_mobile',
    'Mother Office Address': 'mother_office_address',
    'Combined Family Income': 'combined_family_income',
    'PWD Category': 'pwd_category',
    'PWD Category(other)': 'pwd_category_other',
    'PwD (PERCENTAGE OF DISABILITY)': 'pwd_percentage_of_disability',
    'Kashmiri_Migrant': 'kashmiri_migrant',
    'PM Scholarship': 'pm_scholarship',
    'Ex-Service Man': 'ex_service_man',
    'SGC Quota': 'sgc_quota',
    'Defence Personal Quota': 'defence_personal_quota',
    'Dependent of Freedom Fighter': 'dependent_of_freedom_fighter',
    'Add Line 1': 'add_line_1',
    'Add Line 2': 'add_line_2',
    'City': 'city',
    'Other City': 'other_city',
    'Other State': 'other_state',
    'Country': 'country',
    'Other Country': 'other_country',
    'Pincode': 'pincode',
    'Permanent Address Line 1': 'permanent_address_line_1',
    'Permanent Address Line 2': 'permanent_address_line_2',
    'Permanent City': 'permanent_city',
    'Other Permanent City': 'other_permanent_city',
    'Permanent State': 'permanent_state',
    'Other Permanent State': 'other_permanent_state',
    'Permanent Country': 'permanent_country',
    'Other Permanent Country': 'other_permanent_country',
    'Permanent Pincode': 'permanent_pincode',
    'X Result Status': 'x_result_status',
    'X Passing Year': 'x_passing_year',
    'X Percentage': 'x_percentage',
    'X Name of the Institution': 'x_name_of_the_institution',
    'X Board': 'x_board',
    'X Subject Combination': 'x_subject_combination',
    'X Division': 'x_division',
    'XII Result Status': 'xii_result_status',
    'XII Passing Year': 'xii_passing_year',
    'XII Stream': 'xii_stream',
    'XII Maximum Marks': 'xii_maximum_marks',
    'XII Marks Obtained': 'xii_marks_obtained',
    'XII Percentage': 'xii_percentage',
    'XII Name of the Institution': 'xii_name_of_the_institution',
    'XII Board': 'xii_board',
    'XII Board Roll Number': 'xii_board_roll_number',
    'XII Subject Combination': 'xii_subject_combination',
    'XII Division': 'xii_division',
    'UG Subject Combination': 'ug_subject_combination',
    'Is UG done with Honors': 'is_ug_done_with_honors',
    'UG Honors Subject Name': 'ug_honors_subject_name',
    'UG Univerisity Name': 'ug_univerisity_name',
    'UG University Name(other)': 'ug_university_name_other',
    'UG Institute/College': 'ug_institute_college',
    'UG Registration Number(if from this University)': 'ug_reg_number_if_from_this_uni',
    'UG Course Name': 'ug_course_name',
    'UG Division': 'ug_division',
    'UG Upload': 'ug_upload',
    'UG Result Type': 'ug_result_type',
    'Any illness': 'any_illness',
    'Nature of illness': 'nature_of_illness',
    'Are you ward of University/College Employee': 'is_ward_of_uni_college_employee',
    'No Objection Certificate from Employer': 'noc_from_employer',
    'Urban/Rural/Semi-urban/Metropolitan Area': 'urban_rural_semi_urban_metro_area',
    'Ward of transferred University/College Employee': 'ward_of_transferred_uni_college_emp',
    'Certificate/Document of transferred employee': 'cert_of_transferred_employee',
    'Son/Daughter/Husband/Wife/Relative brother/sister of para-military': 'son_daughter_husband_wife_relative_of_paramilitary',
    'Son/daughter/husband/wife/relative brother/sister of para-military Certificate': 'paramilitary_relative_certificate',
    'Participating in any recognized sport at the international level': 'sport_international_level',
    'Participating in any recognized sport at the international level Certificate': 'sport_international_level_cert',
    'On receiving medals at Inter-Uni/State/National Level': 'medals_inter_uni_state_national_level',
    'On receiving medals at Inter-Uni/State/National Level Certificate': 'medals_inter_uni_state_national_level_cert',
    'On participation at the national level organized by the government/sports federation': 'participation_national_level_govt_fed',
    'On participation at the national level organized by the government/sports federation Certificate': 'participation_national_level_govt_fed_cert',
    'Participation in recognized sports at the state/inter-university level': 'participation_state_inter_uni_level',
    'Participation in recognized sports at the state/inter-university level Certificate': 'participation_state_inter_uni_level_cert',
    'On participating in recognized competition at inter-university level': 'participation_inter_uni_level',
    'On participating in recognized competition at inter-university level Certificate': 'participation_inter_uni_level_cert',
    'Winner or runner-up in inter-college level recognized competition': 'winner_runner_up_inter_college_level',
    'Winner or runner-up in inter-college level recognized competition Certificate': 'winner_runner_up_inter_college_level_cert',
    'Competition certificate issued by the District Education Officer/Competent Authority on the basis of participation at the district/division level': 'competition_cert_district_division_level',
    'Competition certificate issued by the District Education Officer/Competent Authority on the basis of participation at the district/division level Certificate': 'competition_cert_district_division_level_cert',
    'On being a member of any sports team of the campus/college/institute in the inter-college competition': 'member_of_sports_team_inter_college_comp',
    'On being a member of any sports team of the campus/college/institute in the inter-college competition Certificate': 'member_of_sports_team_inter_college_comp_cert',
    'NCC B-C': 'ncc_b_c',
    'NCC B-C Certificate': 'ncc_b_c_certificate',
    'NSS & Day Camp': 'nss_day_camp',
    'NSS & Day Camp Certificate': 'nss_day_camp_certificate',
    'Serving or retired employees of the defense forces or their sons/daughters/husband/wife/brothers/sisters': 'serving_retired_defense_employees_relatives',
    'Serving or retired employees of the defense forces or their sons/daughters/husband/wife/brothers/sisters Certificate': 'serving_retired_defense_employees_relatives_cert',
    'Do you have Migration Certificate': 'has_migration_certificate',
    'Migration Certificate': 'migration_certificate',
    'Do you have Transfer Certificate': 'has_transfer_certificate',
    'Transfer Certificate': 'transfer_certificate',
    'Photo': 'photo',
    'Signature': 'signature',
    'Category Certificate': 'category_certificate',
    'PWD Certificate': 'pwd_certificate',
    'ID Proof Certificate': 'id_proof_certificate',
    'Domicile Certificate': 'domicile_certificate',
    'Existing University Student Certificate': 'existing_uni_student_certificate',
    'Income Certificate': 'income_certificate',
    'KM Certificate': 'km_certificate',
    'CW Certificate': 'cw_certificate',
    'PM Certificate': 'pm_certificate',
    'Ex-ServiceMan Certificate': 'ex_serviceman_certificate',
    'Dependent Freedom Fighter': 'dependent_freedom_fighter',
    'SGC Certificate': 'sgc_certificate',
    'Defence Personel Certificate': 'defence_personel_certificate',
    'X Upload': 'x_upload',
    'XII Upload': 'xii_upload',
    'PDF Application Form': 'pdf_application_form',
}

# Mappings for rms_vva_student_details_upload
# This mapping is for students data from RAMACRISNA MADEVA SALGAOCAR HIGHER SECONDARY SCHOOL (RMS) and VIDYA VIKAS ACADEMY (VVA).
RMS_VVA_STUDENT_MAPPING = {
    'SL NO': 'sl_no',
    'Admission Date': 'admission_date',
    'Country': 'country',
    'Date of Birth': 'date_of_birth',
    'Nationality': 'nationality',
    'Student Category': 'student_category',
    'Batch': 'batch',
    'Class': 'class', #<-- VVA only
    'Course': 'course', #<-- RMS only
    'Admission No.': 'admission_no',
    'First Name': 'first_name',
    'Middle Name': 'middle_name',
    'Last Name': 'last_name',
    'Full Name': 'full_name',
    'Full Name as per leaving certificate': 'full_name_as_per_leaving_certificate', #<-- for RMS and VVA
    'Name as per leaving certificate': 'name_as_per_leaving_certificate', #<-- RMS only
    'Gender': 'gender',
    'Blood Group': 'blood_group',
    'Mother Tongue': 'mother_tongue',
    'Religion': 'religion',
    'Address Line 1': 'address_line_1',
    'Address Line 2': 'address_line_2',
    'City': 'city',
    'State': 'state',
    'Pin Code': 'pin_code',
    'Birth Place': 'birth_place', #<-- VVA only
    'Place of Birth': 'place_of_birth', #<-- RMS only
    'Phone': 'phone',
    'Mobile': 'mobile',
    'E-mail': 'e_mail',
    'Roll Number': 'roll_number', #<-- VVA only
    'Seat No': 'seat_no', #<-- RMS only
    'Biometric Id': 'biometric_id',
    'Has The Child Attended School Earlier?If Yes Please Specify The School Name': 'has_attended_school_earlier', #<-- VVA only
    'Sibling Studing In Vva? If Yes Specify Name': 'sibling_studying_in_vva', #<-- VVA only
    'Percentage obtained Std X': 'percentage_obtained_std_x', #<-- RMS only
    'Percentage Or Grade Obtained In The Final Examination Of Class IX': 'percentage_class_ix', #<-- VVA only
    'Percentage Or Grade Obtained In The Final Examination Of Class X': 'percentage_class_x', #<-- VVA only
    'Year passing IX': 'year_passing_ix', #<-- VVA only
    'Year passing X': 'year_passing_x', #<-- VVA only
    'Name Of The School Board': 'school_board_name', #<-- VVA only
    'Name of The Board': 'name_of_board', #<-- RMS only
    'Name Of The School Attended Earlier': 'name_of_school_attended_earlier', #<-- VVA only
    'Name of last school attended': 'name_of_last_school_attended', #<-- RMS only
    'Class Xi And Xii Subjects': 'class_xi_xii_subjects', #<-- VVA only
    'Choose Language II': 'choose_language_ii', #<-- RMS only
    'Choose Optional Subject': 'choose_optional_subject', #<-- RMS only
    'House': 'house', #<-- VVA only
    'Age as on 31 May 2017': 'age_as_on_31_may_2017', #<-- RMS only
    'Aadhar Card No': 'aadhar_card_no',
    'Studentuid': 'studentuid',
    'BPL': 'bpl',
    'Extra-curricular/Sports': 'extra_curricular_sports',
    'Club The Student Would Like To Join': 'club_the_student_would_like_to_join',
    'Student Bank Account No': 'student_bank_account_no',
    'Name of Bank': 'name_of_bank',
    'IFSC Code Of The Bank Branch': 'ifsc_code_of_the_bank_branch',
    'Designation': 'designation',
    'Category': 'category',
    'Religion2': 'religion2',
    'Gen Reg No': 'gen_reg_no',
    'Permanent Education Number': 'permanent_education_number',
    'Pen number': 'pen_number',
    'Prospectus Number': 'prospectus_number',
    'Height in cm': 'height_cm',
    'Weight in kg': 'weight_kg',
    'Father Name': 'father_name', #<-- VVA only
    'Mother Name': 'mother_name', #<-- VVA only
    'Father First Name': 'father_first_name', #<-- RMS only
    'Father Last Name': 'father_last_name', #<-- RMS only
    'Father Full Name': 'father_full_name', #<-- RMS only
    'Father Username': 'father_username', #<-- RMS only
    'Father dob': 'father_dob', #<-- RMS only
    'Father Education': 'father_education', #<-- RMS only
    'Father Occupation': 'father_occupation', #<-- RMS only
    'Father Income': 'father_income', #<-- RMS only
    "Father's Email": 'father_email', #<-- RMS only
    'Father Office Address Line1': 'father_office_address_line1', #<-- RMS only
    'Father Office Address Line2': 'father_office_address_line2', #<-- RMS only
    'Father City': 'father_city', #<-- RMS only
    'Father State': 'father_state', #<-- RMS only
    'Father Office Phone1': 'father_office_phone1', #<-- RMS only
    'Father Mobile Phone': 'father_mobile_phone', #<-- RMS only
    'Mother First Name': 'mother_first_name', #<-- RMS only
    'Mother Last Name': 'mother_last_name', #<-- RMS only
    'Mother Full Name': 'mother_full_name', #<-- RMS only
    'Mother Username': 'mother_username', #<-- RMS only
    'Mother dob': 'mother_dob', #<-- RMS only
    'Mother Education': 'mother_education', #<-- RMS only
    'Mother Occupation': 'mother_occupation', #<-- RMS only
    'Mother Income': 'mother_income', #<-- RMS only
    "Mother's Email": 'mother_email', #<-- RMS only
    'Mother Office Address Line1': 'mother_office_address_line1', #<-- RMS only
    'Mother Office Address Line2': 'mother_office_address_line2', #<-- RMS only
    'Mother City': 'mother_city', #<-- RMS only
    'Mother State': 'mother_state', #<-- RMS only
    'Mother Office Phone1': 'mother_office_phone1', #<-- RMS only
    'Mother Mobile Phone': 'mother_mobile_phone', #<-- RMS only
    'Parents First Name': 'parents_first_name', #<-- VVA only
    'Parents Last Name': 'parents_last_name', #<-- VVA only
    'Parents Full Name': 'parents_full_name', #<-- VVA only
    'Parents relation': 'parents_relation', #<-- VVA only
    'Parents username': 'parents_username', #<-- VVA only
    'Parents date of birth': 'parents_dob', #<-- VVA only
    'Parents education': 'parents_education', #<-- VVA only
    'Parents occupation': 'parents_occupation', #<-- VVA only
    'Parents income': 'parents_income', #<-- VVA only
    'Parents email': 'parents_email', #<-- VVA only
    'Parents office address 1': 'parents_office_address1', #<-- VVA only
    'Parents office address 2': 'parents_office_address2', #<-- VVA only
    'Parents city': 'parents_city', #<-- VVA only
    'Parents state': 'parents_state', #<-- VVA only
    'Parents office phone': 'parents_office_phone', #<-- VVA only
    'Parents mobile phone': 'parents_mobile_phone', #<-- VVA only
    'Immediate contact first name': 'immediate_contact_first_name', #<-- VVA only
    'Immediate contact last name': 'immediate_contact_last_name', #<-- VVA only
    'Immediate contact full name': 'immediate_contact_full_name', #<-- VVA only
    'Immediate contact relation': 'immediate_contact_relation', #<-- VVA only
    'Immediate contact username': 'immediate_contact_username', #<-- VVA only
    'Immediate contact date of birth': 'immediate_contact_dob', #<-- VVA only
    'Immediate contact education': 'immediate_contact_education', #<-- VVA only
    'Immediate contact occupation': 'immediate_contact_occupation', #<-- VVA only
    'Immediate contact income': 'immediate_contact_income', #<-- VVA only
    'Immediate contact email': 'immediate_contact_email', #<-- VVA only
    'Immediate contact office address 1': 'immediate_contact_office_address1', #<-- VVA only
    'Immediate contact office address 2': 'immediate_contact_office_address2', #<-- VVA only
    'Immediate contact city': 'immediate_contact_city', #<-- VVA only
    'Immediate contact state': 'immediate_contact_state', #<-- VVA only
    'Immediate contact office phone': 'immediate_contact_office_phone', #<-- VVA only
    'Immediate contact mobile phone': 'immediate_contact_mobile_phone', #<-- VVA only
    'Student previous data institution': 'student_previous_data_institution', #<-- VVA only
    'Student previous data course': 'student_previous_data_course', #<-- VVA only
    'Student previous data year': 'student_previous_data_year', #<-- VVA only
    'Student previous data total mark': 'student_previous_data_total_mark', #<-- VVA only
}

# Mappings for fees_details_upload,This mapping is for fee-related data.
FEES_MAPPING = {
    'Institute': 'institute',
    'Branch': 'branch',
    'Student': 'student',
    'Fees ID': 'fees_id', #<-- VVA-PrePrimary and RMS only
    'Fees Schedule ID': 'fees_scheduled_id',#<-- VVA-PrePrimary and RMS only
    'E-Mail Address': 'e_mail_address',
    'Mobile Number': 'mobile_number',
    'Standard/Course': 'standard_course',
    'Division': 'division',
    'Roll Number': 'roll_number',
    'Registration Code': 'registration_code',
    'Fee Head': 'fee_head',
    'Due Date ': 'due_date',
    'Fees Category': 'fees_category',
    'Total Amount': 'total_amount',
    'Late Payment Charges': 'late_payment_charges',
    'CGST': 'cgst',
    'SGST': 'sgst',
    'IGST': 'igst',
    'Receipt Number': 'remark',
    'Fees Sequence ': 'fees_sequence',#<-- RMS only
    'Receipt Number ': 'receipt_number',
    'RRN ': 'rrn',
    'Student Status ': 'student_status',
    'Payment Status': 'payment_status',
    'Discount Amount': 'discount_amount',
    'Discount Description': 'discount_description',
    'Paid Amount': 'paid_amount',
    'Remaining Amount': 'remaining_amount',
    'Fees Paid Date': 'fees_paid_date',
    'Qfix Reference Number': 'qfix_reference_number',
    'Payment Gateway Transaction ID': 'payment_gateway_transaction_id',
    'Bank Reference No': 'bank_reference_no',
    'Payment Option': 'payment_option',
    'Payment Mode': 'payment_mode',
    'Payment Reference Details': 'payment_reference_details',
    'Payment Details': 'payment_details',
    'Bank Name': 'bank_name',
    'Bank Branch Name': 'bank_branch_name',
    'IFSC Code': 'ifsc_code',
    'Cheque/DD No': 'cheque_dd_no',
    'Settlement Date': 'settlement_date',
    'Refund Amount': 'refund_amount',
    'Refund Date': 'refund_date',
    'Refund Status': 'refund_status',
    'Default Account': 'default_account',
 #--- VVMs- BANK ACCOUNTS ---
    'VVM online/offline LIVE account': 'vvm_online_offline_live_account',#<-- VVA PrimarySecondarySeniorSecondary only
    'VVVA Online (Pre Primary) Online/Offline Live Account': 'vvva_online_pre_primary_online_offline_live_account',#<-- VVA-PrePrimary only
     #below accounts  are for SDCCE only
    'VVMS R M SALGAOCAR HIGHER SECONDARY SCHOOL/76503022': 'rms_secondary_school_76503022',#<--- for RMS also 
    'VVMS SHREE DAMODAR COLLEGE OF COMMERCE AND ECONOMICS/76502948': 'sdcce_commerce_economics_76502948',
    'VVMS SHREE DAMODAR COL OF COM A ECO MCOM/76502950': 'sdcce_mcom_76502950',
    'VVMS SHREE DAMODAR COL OF COM A ECO BBA/76502953': 'sdcce_bba_76502953',
    'VVMS SHREE DAMODAR COL OF C A E BCOM NSA/76502893': 'sdcce_bcom_nsa_76502893',
    'SHREE DAMODAR COLLEGE PGDFT/76078365': 'sdcce_pgdft_76078365',
    'SHREE DAMODAR COLLEGE B VOC/76502967': 'sdcce_b_voc_76502967',

    # -- below are common fee distribution fields
    'Tuition Fees': 'tuition_fees',
    'Term Fees': 'term_fees', #<-- VVA and RMS only
    'Pupils Fund': 'pupils_fund', #<-- RMS only
    'Activity Fees': 'activity_fees', #<-- VVA PrePrimary only
    'Admission Fees': 'admission_fees', #<-- VVA only
    'Registration Fees': 'registration_fees', #<-- VVA PrePrimary only
    'Development Fund': 'development_fund', #<-- VVA PrimarySecondarySeniorSecondary only
    'Refundable Deposit': 'refundable_deposit', #<-- VVA PrimarySecondarySeniorSecondary only
    'General Deposit': 'general_deposit', #<--RMS only
    'Enrolment Fee': 'enrolment_fee', #<--RMS only
    'Laboratory Deposit': 'laboratory_deposit', #<--RMS only
    'Laboratory Fee': 'laboratory_fee', #<--RMS only
    # -- below fee distribution fields are for SDCCE only (includes 1 common for RMS also)
    'Uni Registration Fees': 'uni_registration_fees',
    'Library Fees': 'library_fees',
    'Gymkhana Fees': 'gymkhana_fees',
    'Other Fees': 'other_fees',
    'Student Aid Fees': 'student_aid_fees',
    'Library Deposit': 'library_deposit',
    'Caution Money Deposit': 'caution_money_deposit',
    'Lab Fees': 'lab_fees',
    'Uni Administration Fees': 'uni_administration_fees',
    'Development Fees': 'development_fees',
    'IT Fees': 'it_fees',
    'Examination Fees': 'examination_fees', #<-- RMS also has
    'PTA Fees': 'pta_fees',
    'University Registration Fees': 'university_registration_fees',
    'Laboratory Fees': 'laboratory_fees',
    'University Administration Fees': 'university_administration_fees',
    'Library & ID Card Fees': 'library_id_card_etc',
    'Computer Lab Fees': 'computer_lab_fees',
    'Information Technology Fees': 'information_technology_fees',
    'IAIMS Fees (DHE)': 'iaims_fees_dhe',
    'IAMS Fees': 'iams_fees',
    'Alumni Registration Fees': 'alumni_registration_fees',
    'Academic Restructuring & Development Fees': 'academic_restructuring_and_development_fees',
    'Magazine Academic Diary and Placement bro': 'magazine_academic_diary_placement_brochure',
    'ID Card Fees': 'id_card_fees',
    'IAIMS Fees': 'iaims_fees',
}

# Consolidate all mappings into a single dictionary for easy access in the application
COLUMN_MAPPING = {
    'students_sdcce_grkcl': SDCCE_GRKCL_STUDENT_MAPPING,
    'students_rms_vva': RMS_VVA_STUDENT_MAPPING,
    'fees': FEES_MAPPING
}