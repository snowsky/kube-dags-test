

##CONNECTION_NAME = 'qa-az1-sqlw3-airflowconnection'
CONNECTION_NAME_PROD = 'prd-az1-sqlw2-airflowconnection'

EXAMPLE_DATA_PATH = '/home/jovyan/konza-dags/test/dags/populations/db_tables'
        
CSV_HEADER = {
    'First Name': 'fname',
    'Last Name': 'lname',
    'DOB': 'dob',
    'Sex': 'sex',
    'SSN': 'ssn',
    'Patient SSN': 'ssn',
    'Vault Name': 'respective_vault',
    'MRN': 'respective_mrn',
    'Opt In/Out': 'opt_choice',
    'Status': 'status',
    'Username': 'username',
    'User': 'user',
    'Date Submitted': 'submitted',
    'Date Completed': 'completed',
}        
