import pathlib
import logging
import re
from datetime import datetime
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook

#CCDA_DIR = "/data/biakonzasftp/C-128/archive/XCAIn/"
CCDA_DIR = "/source-biakonzasftp/C-128/archive/XCAIn/"

DEFAULT_DB_CONN_ID = 'qa-az1-sqlw3-airflowconnection'

with DAG(
    'XCAIn_C_128_parse_CCD_mpi_insurance_contact',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
    },
    params={
        "ccda_dir": CCDA_DIR
    },
    schedule_interval=None,
    tags=['C-128'],
    concurrency=5,  # Set the maximum number of tasks that can run concurrently
) as dag:
    
    from lib.konza.parser import read_clinical_document_from_xml_path
    from lib.konza.extracts.extract import KonzaExtract

    @task
    def list_all_files(ccda_dir):
        files = [
            str(x)
            for x in pathlib.Path(ccda_dir).rglob("*")
            if x.is_file()
        ]
        logging.info(f"Total files found: {len(files)}")
        return files

    @task
    def parse_and_insert(files):
        import re
        import pathlib
        from datetime import datetime

        def clean_mysql_value(v):
            if isinstance(v, str):
                return v.replace('%', '%%')  # escape % for MySQL formatting
            if isinstance(v, (dict, list, set, tuple)):
                return str(v).replace('%', '%%')
            return v  # keep None, int, float as-is

        xml_files = [(pathlib.Path(f).stem, f) for f in files if f.endswith(".xml")]
        logging.info(f"Filtered XML files count: {len(xml_files)}")

        for stem, path in xml_files:
            try:
                logging.info(f"Processing file: {path}")
                clinical_document = read_clinical_document_from_xml_path(path)
                extract = KonzaExtract.from_clinical_document(clinical_document)

                # Extract Patient Name Information
                given_name = getattr(extract.patient_name_extract, "given_name", None)
                firstname = given_name.split()[0] if given_name else None
                middlename = getattr(extract.patient_name_extract, "middle_names", None)
                lastname = getattr(extract.patient_name_extract, "family_name", None)
                prefix = getattr(extract.patient_name_extract, "name_prefix", None)
                suffix = getattr(extract.patient_name_extract, "name_suffix", None)

                # Extract Demographic Information
                dob_raw = getattr(extract.patient_demographic_extract, "date_of_birth", None)
                dob = None
                if dob_raw:
                    dob_match = re.match(r"(\d{4})(\d{2})(\d{2})", dob_raw)
                    if dob_match:
                        dob = f"{dob_match[1]}-{dob_match[2]}-{dob_match[3]}"
                    else:
                        dob = "1000-01-01"
    
                sex = getattr(extract.patient_demographic_extract, "sex", None)
                race = getattr(extract.patient_demographic_extract, "race", None)
                ethnicity = getattr(extract.patient_demographic_extract, "ethnicity", None)
                language = getattr(extract.patient_demographic_extract, "language", None)
                marital_status = getattr(extract.patient_demographic_extract, "marital_status", None)
                deceased = str(getattr(extract.patient_demographic_extract, "deceased", None))

                # Extract Address Information
                street1 = getattr(extract.patient_address_extract, "street1", None)
                street2 = getattr(extract.patient_address_extract, "street2", None)
                city = getattr(extract.patient_address_extract, "city", None)
                state = getattr(extract.patient_address_extract, "state", None)
                zip_code = getattr(extract.patient_address_extract, "postal_code", None)

                # Extract Telecom Information
                email = getattr(extract.patient_telecom_extract, "emails", None)
                home_phone_number_id = getattr(extract.patient_telecom_extract, "home_phone_number_id", None)
                business_phone_number_id = getattr(extract.patient_telecom_extract, "business_phone_number_id", None)

                # Metadata
                stable_document_id = None
                created = None
                updated = None
                event_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                index_update_dt_tm = event_timestamp
                collect_registered = '1970-12-31 18:00:00'
                collect_cmp = '-1'
                source = None  # Set this appropriately from the XML extraction, e.g., from `extract.some_field`
                source_type = None  # Similarly, extract this value
                MPI = None  # Extract this value
                id = None  # Extract this value
                accid = '9999'  # Extract this value
                seqnum = '0'  # Extract this value
                planid = None  # Extract this value
                plan_name = None  # Extract this value
                insurer = None  # Extract this value
                co_country = None  # Extract this value
                contact_name = None  # Extract this value
                co_phone = None  # Extract this value
                grpid = None  # Extract this value
                grpname = None  # Extract this value
                emplid = None  # Extract this value
                emplname = None  # Extract this value
                eff_date = None  # Extract this value
                exp_date = None  # Extract this value
                auth_info = None  # Extract this value
                plan_type = None  # Extract this value
                policy_id = None  # Extract this value
                ins_prefix = None  # Extract this value
                ssn = None
                mpi_ssn = None
                co_addr1 = None
                co_addr2 = None
                co_city = None
                co_state = None
                co_zip = None
                co_country = None
                contact_name = None
                co_phone = None
                co_name = None
                patient_id = "9999"
                cell_phone_number_id = None
                mpi_update = None
                vault_pid = None
                id_val = None
                mrn = "999999999"
                citizenship = None
                nationality = None
                religion = None
                hipaa = None
                mother_maiden_name = None
                dlnum = None
                dlstate = None
                alt_id_name = None
                alt_id_num = None
                policy_id = None
                discharge_status_code = None
                discharge_status_description = None
                accid_ref = None
                mil_status = None
                student_status = None
                euid = stem
                
                # Insert into MySQL (Using the available values from `KonzaExtract`)
                sql_insurance = """
                INSERT INTO person_master._patient_insurance (
                    index_update_dt_tm, source, source_type, MPI, id, accid, seqnum, planid, plan_name, insurer,
                    co_addr1, co_addr2, co_city, co_state, co_zip, co_country, contact_name, co_phone,
                    grpid, grpname, emplid, emplname, eff_date, exp_date, auth_info, plan_type, policy_id, ins_prefix,
                    ins_fname, ins_mname, ins_lname, ins_suffix, ins_dob, ins_ssn, relationship, ins_addr1, ins_addr2,
                    ins_city, ins_state, ins_zip, ins_country, ins_email, event_timestamp, home_phone_number_id,
                    business_phone_number_id, mpi_ssn,euid
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s
                )
                """
                
                # Ensure the number of values matches
                values_insurance = (
                    index_update_dt_tm, source, source_type, MPI, id, accid, seqnum, planid, plan_name, insurer,
                    co_addr1, co_addr2, co_city, co_state, co_zip, co_country, contact_name, co_phone,
                    #street1, street2, city, state, zip_code, co_country, contact_name, co_phone,
                    grpid, grpname, emplid, emplname, eff_date, exp_date, auth_info, plan_type, policy_id, ins_prefix,
                    firstname, middlename, lastname, suffix, dob, ssn, marital_status, street1, street2,
                    city, state, zip_code, co_country, email, event_timestamp, home_phone_number_id, 
                    business_phone_number_id, mpi_ssn , euid
                )

                sql_contact = """
                INSERT INTO person_master._patient_contact (
                    index_update_dt_tm, source, source_type, MPI, id, patient_id, street1, street2, city,
                    state, zip, country, email, co_name, co_addr1, co_addr2, co_city, co_state, co_zip, co_country, 
                    event_timestamp, home_phone_number_id, business_phone_number_id, cell_phone_number_id, euid
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                
                values_contact = (
                    index_update_dt_tm, source, source_type, MPI, id, patient_id, street1, street2, city,
                    state, zip_code, co_country, email, co_name, co_addr1, co_addr2, co_city, co_state, co_zip, co_country,
                    event_timestamp, home_phone_number_id, business_phone_number_id, cell_phone_number_id, euid
                )


                sql_mpi = """
                INSERT INTO person_master._mpi (
                    index_update_dt_tm, source, source_type, MPI, mpi_update, vault_pid, id, mrn, prefix,
                    firstname, middlename, lastname, suffix, dob, sex, race, ethnicity, citizenship,
                    nationality, language, religion, marital_status, mil_status, student_status, hipaa,
                    deceased, mother_maiden_name, event_timestamp, created, updated, collect_registered,
                    collect_cmp, stable_document_id, ssn, dlnum, dlstate, alt_id_name, alt_id_num, policy_id,
                    discharge_status_code, discharge_status_description, accid_ref , euid
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s , %s , %s
                )
                """
    
                values_mpi = (
                    index_update_dt_tm, source, source_type, MPI, mpi_update, vault_pid, id_val,
                    mrn, prefix,
                    firstname, middlename, lastname, suffix, dob, sex, race, ethnicity, citizenship,
                    nationality, language, religion, marital_status, mil_status, student_status, hipaa,
                    deceased, mother_maiden_name, event_timestamp, created, updated, collect_registered,
                    collect_cmp, stable_document_id, ssn, dlnum, dlstate, alt_id_name, alt_id_num, policy_id,
                    discharge_status_code, discharge_status_description, accid_ref , euid
                )
    
                
                # Ensure the number of values matches
                #assert len(values_insurance) == 47, f"Value count mismatch for insurance: Expected 47 values, got {len(values_insurance)}"
                #assert len(values_contact) == 46, f"Value count mismatch for contact: Expected 46 values, got {len(values_contact)}"
                
                # Execute the queries
                safe_values_insurance = tuple(clean_mysql_value(v) for v in values_insurance)
                safe_values_contact = tuple(clean_mysql_value(v) for v in values_contact)
                safe_values_mpi = tuple(clean_mysql_value(v) for v in values_mpi)

                
                logging.info(f"Total values for insurance passed: {len(safe_values_insurance)}")
                logging.info(f"Total values for contact passed: {len(safe_values_contact)}")
                logging.info(f"Total values for mpi passed: {len(safe_values_mpi)}")

                
                # Execute the first insert for _patient_insurance
                mysql_hook = MySqlHook(mysql_conn_id=DEFAULT_DB_CONN_ID)
                conn = mysql_hook.get_conn()
                cursor = conn.cursor()
                cursor.execute(sql_insurance, safe_values_insurance)
                conn.commit()
                
                # Execute the second insert for _patient_contact
                cursor.execute(sql_contact, safe_values_contact)
                conn.commit()

                # Execute the third insert for _mpi
                cursor.execute(sql_mpi, safe_values_mpi)
                conn.commit()
                
                cursor.close()
                conn.close()

            except Exception as e:
                logging.error(f"Error processing file {path}: {e}")
                raise

    all_files = list_all_files(CCDA_DIR)
    parse_and_insert(all_files)