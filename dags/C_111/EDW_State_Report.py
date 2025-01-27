from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from datetime import timedelta
import trino
import logging
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
default_args = {
    'owner': 'airflow',
    'retries': 2,  # Set the number of retries to 2
    'retry_delay': timedelta(minutes=5)  # Optional: Set the delay between retries
}

class KonzaTrinoOperator(PythonOperator):

    def __init__(self, query, **kwargs):

        def execute_trino_query(**kwargs):
            ds = kwargs['ds']
            # Retrieve the connection details
            conn = BaseHook.get_connection('trinokonza')
            host = conn.host
            port = conn.port
            user = conn.login
            schema = conn.schema

            # Connect to Trino
            trino_conn = trino.dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog='hive',
                schema=schema,
            )
            cursor = trino_conn.cursor()

            try:
                # the .replace is a no-op if ds not present in query
                cursor.execute(query.replace('<DATEID>', ds))
                print(f"Executed query: {query}")
                logging.info(f'Executed query: {query}')
                # Check the status of the query
                query_id = cursor.query_id
                cursor.execute(f"SELECT state FROM system.runtime.queries WHERE query_id = '{query_id}'")
                status = cursor.fetchone()[0]
                
                if status != 'FINISHED':
                    time.sleep(5)
                    cursor.execute(f"SELECT state FROM system.runtime.queries WHERE query_id = '{query_id}'")
                    status = cursor.fetchone()[0]
                    if status != 'FINISHED':
                        # Get the number of active workers
                        cursor.execute("SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false")
                        active_workers = cursor.fetchone()[0]
                        print(f"Number of active workers: {active_workers}")
                        logging.info(f'Number of active workers: {active_workers}')
                        raise AirflowException(f"Query did not finish successfully. Status: {status} - Query: {query}")
                

            except trino.exceptions.TrinoQueryError as e:
                raise AirflowException(f"Query failed: {str(e)}")
            finally:
                cursor.close()
                trino_conn.close()

        super(KonzaTrinoOperator, self).__init__(
            python_callable=execute_trino_query,
            provide_context=True,
            **kwargs
        )

with DAG(
    dag_id='EDW_State_Report',
    schedule_interval='@monthly',
    tags=['C-111'],
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
) as dag:
    create_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='create_accid_by_state_prep__final',
        query="""
        CREATE TABLE IF NOT EXISTS hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
        ( patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar, 
        index_update varchar) WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['patient_id'], 
    sorted_by = ARRAY['patient_id'],
    bucket_count = 64
)
        """,
    )
    insert_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_prep__final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
      SELECT distinct
    s.patient_id, 
    s.index_update_dt_tm, 
    CASE 
        WHEN s.state IN ('01', '1', 'AL', 'al', 'aL', 'Al', 'Alabama', 'alabama') THEN 'Alabama'
        WHEN s.state IN ('02', '2', 'AK', 'ak') THEN 'Alaska'
        WHEN s.state IN ('AMERICANSAMOA', 'AS') THEN 'American Samoa'
        WHEN s.state IN ('04', 'Ari', 'ARIZONA', 'Arizona', 'AZ', 'Az', 'az', 'AZ ') THEN 'Arizona'
        WHEN s.state IN ('05', 'AR', 'ar', 'aR', 'Ar', 'ARK', 'Ark', 'Arkansas') THEN 'Arkansas'
        WHEN s.state IN ('AA', 'aa', 'AE', 'AP', 'AP ', 'ARMEDFORCESOTHER', 'ARMFORCAMEREXCPTCAN') THEN 'Armed Forces'
        WHEN s.state IN ('06', '95403', '95404', '95476', ' CA', 'CA', 'Ca', 'ca', 'cA', 'CA ', 'California') THEN 'California'
        WHEN s.state IN ('08', '8', 'CO', 'Co', 'co', 'Colorado') THEN 'Colorado'
        WHEN s.state IN ('09', '9', '06114', '06278', '06810', 'CT', 'ct', 'Ct', 'CT ') THEN 'Connecticut'
        WHEN s.state IN ('10', 'DE', 'Delaware') THEN 'Delaware'
        WHEN s.state IN ('11', 'DC') THEN 'District of Columbia'
        WHEN s.state IN ('12', 'FL', 'fl', 'Fl', 'fL', 'FL ', 'Florida') THEN 'Florida'
        WHEN s.state IN ('13', 'GA', 'Ga', 'ga', 'gA', 'GA ', 'Ga ', 'Georgia') THEN 'Georgia'
        WHEN s.state IN ('66', 'GU') THEN 'Guam'
        WHEN s.state IN ('15', 'HI', 'Hi') THEN 'Hawaii'
        WHEN s.state IN ('16', 'ID') THEN 'Idaho'
        WHEN s.state IN ('17', 'IL', 'Il', 'il', 'Ilinois', 'Ill', 'ill', 'Illinois') THEN 'Illinois'
        WHEN s.state IN ('18', 'IN', 'in', 'In', 'Indiana', 'INDIANA') THEN 'Indiana'
        WHEN s.state IN ('19', 'IA', 'ia', 'Iowa', 'iowa') THEN 'Iowa'
        WHEN s.state IN ('20', 'Ka', 'Kan', 'Kansas', 'KANSAS', 'kansas', 'kANSAS', 'Kasnas', 'KS', 'ks', 'Ks', 'kS', 'KS ', 'ks ', 'KS - KANSAS', 'KSks') THEN 'Kansas'
        WHEN s.state IN ('21', 'KY', 'kY') THEN 'Kentucky'
        WHEN s.state IN ('22', '71082', 'LA', 'la', 'La', 'lA', 'LA ') THEN 'Louisiana'
        WHEN s.state IN ('23', 'ME') THEN 'Maine'
        WHEN s.state IN ('MH') THEN 'Marshall Islands'
        WHEN s.state IN ('24', 'MD', 'md', 'mD') THEN 'Maryland'
        WHEN s.state IN ('25', 'MA', 'mA', 'ma', 'Massachusetts') THEN 'Massachusetts'
        WHEN s.state IN ('26', 'MI', 'Mi', 'mi', 'MICH', 'Michigan') THEN 'Michigan'
        WHEN s.state IN ('27', 'Min', 'Minnesota', 'MN', 'mn', 'Mn', 'MN ') THEN 'Minnesota'
        WHEN s.state IN ('28', 'MS', 'ms', 'Ms', 'mS') THEN 'Mississippi'
        WHEN s.state IN ('29', 'Mis', 'Missouri', 'MISSOURI', 'missouri', 'MO', 'mo', 'Mo', 'mO', 'MO ', 'MO 64106', 'MO64030') THEN 'Missouri'
        WHEN s.state IN ('30', 'Mon', 'Montana', 'MT', 'mt', 'MT ') THEN 'Montana'
        WHEN s.state IN ('31', 'NE', 'Ne', 'nE', 'ne', 'Nebraska') THEN 'Nebraska'
        WHEN s.state IN ('32', 'NV', 'nv', 'NV ') THEN 'Nevada'
        WHEN s.state IN ('33', 'NH') THEN 'New Hampshire'
        WHEN s.state IN ('34', 'NJ', 'nj') THEN 'New Jersey'
        WHEN s.state IN ('35', 'New Mexico', 'NM', 'Nm') THEN 'New Mexico'
        WHEN s.state IN ('36', 'New York', 'NY', 'Ny', 'ny', 'NY ') THEN 'New York'
        WHEN s.state IN ('AB', 'ab', 'Alberta', 'ALBERTA', 'B.C.', 'BC', 'British Columbia', 'BRITISHCOLUMBIA', 'CAN', 'Canada', 'CANADA', 'canada', 'Manitoba', 'MANITOBA', 'MB', 'NB', 'NL', 'Northwest Territories', 'Nova Scotia', 'NOVA SCOTIA', 'NOVASCOTIA', 'NS', 'ON', 'On', 'on', 'ONT', 'Ontario', 'ONTARIO', 'PE', 'PQ', 'QC', 'Quebec', 'QUEBEC', 'Saskatchewan', 'SK') THEN 'Canada'
        WHEN s.state IN ('AF') THEN 'Afghanistan'
        WHEN s.state IN ('AG') THEN 'Algeria'
        WHEN s.state IN ('AU', 'Au') THEN 'Australia'
        WHEN s.state IN ('BA') THEN 'Bosnia and Herzegovina'
        WHEN s.state IN ('BB') THEN 'Barbados'
        WHEN s.state IN ('BL') THEN 'Saint Barthélemy'
        WHEN s.state IN ('BR') THEN 'Brazil'
        WHEN s.state IN ('BS') THEN 'Bahamas'
        WHEN s.state IN ('Bulgaria') THEN 'Bulgaria'
        WHEN s.state IN ('BY') THEN 'Belarus'
        WHEN s.state IN ('CH') THEN 'Switzerland'
        WHEN s.state IN ('CHIH') THEN 'Mexico'
        WHEN s.state IN ('CHINA', 'CN') THEN 'China'
        WHEN s.state IN ('CI') THEN 'Cote dIvoire'
        WHEN s.state IN ('CL') THEN 'Chile'
        WHEN s.state IN ('CP') THEN 'Clipperton Island'
        WHEN s.state IN ('CR') THEN 'Costa Rica'
        WHEN s.state IN ('CS') THEN 'Serbia and Montenegro'
        WHEN s.state IN ('CZ', 'Czechia') THEN 'Czech Republic'
        WHEN s.state IN ('DF', 'DG') THEN 'Mexico'
        WHEN s.state IN ('DK') THEN 'Denmark'
        WHEN s.state IN ('DO', 'DR') THEN 'Dominican Republic'
        WHEN s.state IN ('EG') THEN 'Egypt'
        WHEN s.state IN ('England', 'england', 'ENGLAND') THEN 'England'
        WHEN s.state IN ('ES') THEN 'Spain'
        WHEN s.state IN ('ESTADODEMEXICO') THEN 'Mexico'
        WHEN s.state IN ('FEDSTATESMICRONESIA', 'FM') THEN 'Federated States of Micronesia'
        WHEN s.state IN ('FO', 'Fo') THEN 'Faroe Islands'
        WHEN s.state IN ('FR', 'France') THEN 'France'
        WHEN s.state IN ('GB') THEN 'Great Britain'
        WHEN s.state IN ('GE') THEN 'Georgia'
        WHEN s.state IN ('Germany', 'GERMANY', 'germany') THEN 'Germany'
        WHEN s.state IN ('GM') THEN 'Gambia'
        WHEN s.state IN ('GR') THEN 'Greece'
        WHEN s.state IN ('GT') THEN 'Guatamala'
        WHEN s.state IN ('HO') THEN 'Hondurus'
        WHEN s.state IN ('IE') THEN 'Ireland'
        WHEN s.state IN ('IO', 'io') THEN 'British Indian Ocean Territory'
        WHEN s.state IN ('Ireland') THEN 'Ireland'
        WHEN s.state IN ('IS') THEN 'Iceland'
        WHEN s.state IN ('IT') THEN 'Italy'
        WHEN s.state IN ('JA', 'JP') THEN 'Japan'
        WHEN s.state IN ('Jalisco', 'JALISCO') THEN 'Mexico'
        WHEN s.state IN ('Jamaica', 'JM') THEN 'Jamaica'
        WHEN s.state IN ('JC') THEN 'French Southern Territories'
        WHEN s.state IN ('KD', 'Kd') THEN 'Kaduna'
        WHEN s.state IN ('KE') THEN 'Kenya'
        WHEN s.state IN ('KT') THEN 'Christmas Island'
        WHEN s.state IN ('KZ') THEN 'Kazakhstan'
        WHEN s.state IN ('LM') THEN 'Malta'
        WHEN s.state IN ('LO') THEN 'Slovakia'
        WHEN s.state IN ('LS') THEN 'Lesotho'
        WHEN s.state IN ('MG') THEN 'Madagascar'
        WHEN s.state IN ('MJ') THEN 'Montenegro'
        WHEN s.state IN ('MM') THEN 'Myanmar'
        WHEN s.state IN ('mY') THEN 'Malaysia'
        WHEN s.state IN ('N.L.') THEN 'Netherlands'
        WHEN s.state IN ('nA') THEN 'Namibia'
        WHEN s.state IN ('NAY', 'NAYARIT') THEN 'Mexico'
        WHEN s.state IN ('NG') THEN 'Nigeria'
        WHEN s.state IN ('NI') THEN 'Nicaragua'
        WHEN s.state IN ('NL') THEN 'Newfoundland and Labrador'
        WHEN s.state IN ('Northwest Territories') THEN 'Northwest Territories'
        WHEN s.state IN ('Nova Scotia', 'NOVA SCOTIA', 'NOVASCOTIA') THEN 'Nova Scotia'
        WHEN s.state IN ('NP') THEN 'Nepal'
        WHEN s.state IN ('NSW') THEN 'New South Wales'
        WHEN s.state IN ('NT') THEN 'Netherlands Antilles'
        WHEN s.state IN ('NW') THEN 'Northern Mariana Islands'
        WHEN s.state IN ('NZ') THEN 'New Zealand'
        WHEN s.state IN ('ON', 'On', 'on', 'ONT', 'Ontario', 'ONTARIO') THEN 'Ontario'
        WHEN s.state IN ('OO', 'oo') THEN 'Oman'
        WHEN s.state IN ('OS') THEN 'Austria'
        WHEN s.state IN ('Out of Country', 'Out of USA', 'OUTSIDE OF U') THEN 'UNKNOWN'
        WHEN s.state IN ('PE') THEN 'Canada'
        WHEN s.state IN ('PJ') THEN 'Etorofu, Habomai, Kunashiri, and Shikotan Islands'
        WHEN s.state IN ('PN') THEN 'Pitcairn Islands'
        WHEN s.state IN ('PO') THEN 'Portugal'
        WHEN s.state IN ('PQ') THEN 'Canada'
        WHEN s.state IN ('PS') THEN 'Palestine'
        WHEN s.state IN ('QA') THEN 'Qatar'
        WHEN s.state IN ('QC') THEN 'Canada'
        WHEN s.state IN ('QT') THEN 'Oceania'
        WHEN s.state IN ('QU') THEN 'Polynesia'
        WHEN s.state IN ('Quebec', 'QUEBEC') THEN 'Quebec'
        WHEN s.state IN ('RO') THEN 'Romania'
        WHEN s.state IN ('RU', 'RUSSIA') THEN 'Russia'
        WHEN s.state IN ('RW') THEN 'Rwanda'
        WHEN s.state IN ('SA', 'sa') THEN 'South Australia'
        WHEN s.state IN ('Saskatchewan') THEN 'Canada'
        WHEN s.state IN ('SE') THEN 'Sweden'
        WHEN s.state IN ('SF') THEN 'South Africa'
        WHEN s.state IN ('SK') THEN 'Canada'
        WHEN s.state IN ('SL') THEN 'Sierra Leone'
        WHEN s.state IN ('SO', 'So') THEN 'Somalia'
        WHEN s.state IN ('SP', 'Sp') THEN 'Spain'
        WHEN s.state IN ('SV') THEN 'El Salvador'
        WHEN s.state IN ('SW', 'Sweden') THEN 'Sweden'
        WHEN s.state IN ('Switzerlan') THEN 'Switzerland'
        WHEN s.state IN ('TC') THEN 'Turks and Caicos'
        WHEN s.state IN ('TH') THEN 'Thailand'
        WHEN s.state IN ('TL') THEN 'Timor-Leste'
        WHEN s.state IN ('TM') THEN 'Turkmenistan'
        WHEN s.state IN ('uk') THEN 'United Kingdom'
        WHEN s.state IN ('UR') THEN 'Union of Soviet Socialist Republics'
        WHEN s.state IN ('UV') THEN 'Burkina Faso'
        WHEN s.state IN ('ve') THEN 'Venezuela'
        WHEN s.state IN ('WS') THEN 'Samoa'
        WHEN s.state IN ('XF') THEN 'Fujairah'
        WHEN s.state IN ('YT') THEN 'Mayotte'
        WHEN s.state IN ('ZJ') THEN 'Zhejiang Province'
        WHEN s.state IN ('37', 'NC', 'nc', 'nC', 'NC ') THEN 'North Carolina'
        WHEN s.state IN ('38', 'ND') THEN 'North Dakota'
        WHEN s.state IN ('MP') THEN 'Northern Mariana Islands'
        WHEN s.state IN ('39', 'OH', 'oh', 'OH ', 'ohio', 'Ohio') THEN 'Ohio'
        WHEN s.state IN ('40', 'OK', 'ok', 'Ok', 'oK', 'OK ', 'Okl', 'Oklahoma', 'OKLAHOMA') THEN 'Oklahoma'
        WHEN s.state IN ('41', 'OR', 'or', 'oR', 'OR ', 'Oregon') THEN 'Oregon'
        WHEN s.state IN ('42', 'PA', 'pa', 'PA ', 'Pennsylvania') THEN 'Pennsylvania'
        WHEN s.state IN ('PR', 'PUE') THEN 'Puerto Rico'
        WHEN s.state IN ('44', 'RI') THEN 'Rhode Island'
        WHEN s.state IN ('45', 'SC', 'sc', 'Sc', 'sC', 'South Carolina') THEN 'South Carolina'
        WHEN s.state IN ('46', 'SD', 'sd') THEN 'South Dakota'
        WHEN s.state IN ('47', 'Tennessee', 'TN', 'tN', 'tn', 'Tn', 'TN ') THEN 'Tennessee'
        WHEN s.state IN ('48', 'Tex', 'Texas', 'texas', 'TEXAS', 'TX', 'tx', 'Tx', 'tX', 'TX ', 'TX.') THEN 'Texas'
        WHEN s.state IN ('UM') THEN 'United States Minor Outlying Islands'
        WHEN s.state IN ('00', '0', '00000', '0.3', '03', '07', '14', '67', '94', '95', '96', '97', '99', '-', '--', ' ', '  ', '   ', ' K', ' LIVING', ' N', ' Rehab', ' REHAB CENTER', ' REHABILITATION', ' REHABILITATION CENTER', '*', '**', ',', '.', '..', '/', '//', ']]', '__', '0R', '8-', 'A', 'a', 'AGS', 'Aichi', 'ANT', 'ARMA', 'BASEL', 'Bavaria', 'BAVARIA', 'Bel Aire', 'BIRD CITY', 'Blackwell', 'BOP', 'C', 'C9', 'Cambria', 'Cambridges', 'Campeche', 'candada', 'CC', 'CD', 'CD:', 'CD:1409749313', 'CD:1409749429', 'CD:1555486566', 'CD:1555486720', 'CD:1555487113', 'CD:309242', 'CD:309246', 'CD:309247', 'CD:309260', 'CD:309283', 'CD:309292', 'CD:309298', 'CD:36340707', 'CD:36340710', 'CD:36340717', 'CD:36340737', 'CD:36340838', 'CD:36340868', 'CD:670132', 'CD:750761272', 'CD:8339441405', 'CD:845539', 'CD:845542', 'cdmx', 'Chaba', 'CHERRYVALE', 'CHESHIRE', 'Chiapas', 'Chili', 'CHIS', 'CIUDAD DE ME', 'Col', 'Colchester', 'Colorade', 'CONCORDIA', 'Derby', 'DS', 'Du', 'EAST ORANGE', 'EDISON', 'EN', 'EXCELSIOR SPR', 'EXCELSIOR SPRINGS', 'F', 'FA', 'Fair Oaks', 'FC', 'FZ', 'GAGA', 'Geo', 'GO', 'Halstead', 'hanavor', 'Hayfor', 'Hayfork', 'HAYFORK', 'HE', 'HGO', 'HIL', 'Hoxie', 'Hyampom', 'I', 'IW', 'JENNINGS', 'Junction City', 'K', 'k', 'K6', 'KA', 'KC', 'KDS', 'Ken', 'L3', 'Lawrence', 'london', 'M', 'M0', 'm0', 'MANITOPA', 'MCPHERSON', 'METUCHEN', 'MINNEAPOLIS', 'MOR', 'Mulvane', 'N', 'n', 'N.', 'N/A', 'NA', 'Na', 'na', 'NB', 'Neodesha', 'NEODESHA', 'NEW', 'NN', 'NO', 'No', 'NO STATE INDICATED', 'None', 'NOTKNOWN', 'NU', 'null', 'NULL', 'NYC', 'O', 'OA', 'OAXACA', 'Oaxaca', 'OC', 'OHI', 'OT', 'OTH', 'Other', 'PEABODY', 'Portola', 'PRATT', 'QB', 'QD', 'QL', 'QLD', 'Redding', 'RX', 'S', 's', 'SABETHA', 'San Luis Potasi', 'Santiago', 'SED', 'SEOUL', 'Serbia', 'Spearville', 'ST', 'State', 'TAB', 'TABASCO', 'TAMPS', 'TE', 'Tirol', 'TLAX', 'Trinity Pine', 'U', 'u', 'UK', 'UN', 'un', 'Un', 'UNABLE TO CO', 'UNKNOWN', 'Unknown', 'US', 'UU', 'V', 'vauxhall', 'VIENNA', 'WC', 'Weaverville', 'Wes', 'WESTERN', 'WESTMORELAND', 'Wichita', 'WICHITA', 'WILLMARS', 'X', 'x', 'XX', 'xx', 'XXOTHER', 'XX-Other', 'XY', 'Y', 'y', 'YATES CENTER', 'YO', 'YUBA CITY', 'YUC', 'YY', 'Z', 'ZZ') THEN 'UNKNOWN'
        WHEN s.state IN ('49', 'UT', 'ut', 'UT ') THEN 'Utah'
        WHEN s.state IN ('50', 'VT') THEN 'Vermont'
        WHEN s.state IN ('78', 'VI', 'Virgin Islands') THEN 'Virgin Islands'
        WHEN s.state IN ('51', 'VA', 'va') THEN 'Virginia'
        WHEN s.state IN ('53', 'WA', 'wa', 'Wa', 'WA ') THEN 'Washington'
        WHEN s.state IN ('54', 'WV') THEN 'West Virginia'
        WHEN s.state IN ('55', 'WI', 'wi', 'wI', 'Wi', 'WI ', 'Wisconsin') THEN 'Wisconsin'
        WHEN s.state IN ('56', 'WY', 'wy', 'WY ') THEN 'Wyoming'
        ELSE 'UNKNOWN'
    END AS state_standardized,
    s.index_update
FROM patient_contact_parquet_pm s 
        WHERE concat(index_update,'-01') = concat(substring('<DATEID>', 1, length('<DATEID>') - 3),'-01')
        """,
    )
    drop_accid_by_state_final = KonzaTrinoOperator(
        task_id='drop_accid_by_state_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_by_state_final
        """,
    )
    create_accid_by_state_final = KonzaTrinoOperator(
        task_id='create_accid_by_state_final',
        query="""
              CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_by_state_final
      ( patient_id varchar, 
      index_update_dt_tm varchar, 
      state varchar, 
        index_update varchar) WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['patient_id'], 
    sorted_by = ARRAY['patient_id'],
    bucket_count = 64
)
        """,
    )
    insert_accid_by_state_final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_final
        (select * from sup_12760_c59_accid_by_state_prep__final)
        """,
    )
    drop_accid_by_state_distinct__final = KonzaTrinoOperator(
        task_id='drop_accid_by_state_distinct__final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_by_state_distinct__final
        """,
    )
    create_accid_by_state_distinct__final = KonzaTrinoOperator(
        task_id='create_accid_by_state_distinct__final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_by_state_distinct__final
        ( patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar, 
        index_update varchar) WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['patient_id'], 
    sorted_by = ARRAY['patient_id'],
    bucket_count = 64
)
        """,
    )
    insert_accid_by_state_distinct__final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_distinct__final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_distinct__final
      (select distinct * from hive.parquet_master_data.sup_12760_c59_accid_by_state_final)
        """,
    )
    drop_accid_state_distinct_rank_final = KonzaTrinoOperator(
        task_id='drop_accid_state_distinct_rank_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_final
        """,
    )
    create_accid_state_distinct_rank_final = KonzaTrinoOperator(
        task_id='create_accid_state_distinct_rank_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_final
(rank bigint,
patient_id varchar, 
index_update_dt_tm varchar, 
state varchar,
index_update varchar) WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['patient_id'], 
    sorted_by = ARRAY['patient_id'],
    bucket_count = 64
)
        """,
    )
    insert_accid_state_distinct_rank_final = KonzaTrinoOperator(
        task_id='insert_accid_state_distinct_rank_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_final
    (select DENSE_RANK() OVER(Partition by T.patient_id ORDER BY T.index_update_dt_tm DESC) as rank,* from sup_12760_c59_accid_by_state_distinct__final T)
        """,
    )
    drop_accid_state_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='drop_accid_state_distinct_rank_1_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_final
        """,
    )
    create_accid_state_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='create_accid_state_distinct_rank_1_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_final
        (rank bigint,
        patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar,
        index_update varchar) WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['patient_id'], 
    sorted_by = ARRAY['patient_id'],
    bucket_count = 64
)
        """,
    )
    insert_accid_state_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='insert_accid_state_distinct_rank_1_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_final
(select * from sup_12760_c59_accid_state_distinct_rank_final where rank = 1)
        """,
    )
    drop_mpi_accid_prep_final = KonzaTrinoOperator(
        task_id='drop_mpi_accid_prep_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final
        """,
    )
    ## Seems like this is not used initially
    create_mpi_accid_prep_final = KonzaTrinoOperator(
        task_id='create_mpi_accid_prep_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final (
    mpi varchar, 
    accid_ref varchar, 
    index_update varchar
)
WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['accid_ref'], 
    sorted_by = ARRAY['accid_ref'],
    bucket_count = 64
)
        """,
    )
    drop_mpi_accid_no_blanks = KonzaTrinoOperator(
        task_id='drop_mpi_accid_no_blanks',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks
        """,
    )
    create_mpi_accid_no_blanks = KonzaTrinoOperator(
        task_id='create_mpi_accid_no_blanks',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks
        (mpi varchar, 
        accid_ref varchar, 
        index_update varchar)
        """,
    )
    insert_mpi_accid_no_blanks = KonzaTrinoOperator(
        task_id='insert_mpi_accid_no_blanks',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks
        (select * from hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final
        where mpi <> ''
        and accid_ref <> ''
        and accid_ref <> 'None'
        and index_update <> '')
        """,
    )
    drop_mpi_accid_final = KonzaTrinoOperator(
        task_id='drop_mpi_accid_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_accid_final
        """,
    )
    create_mpi_accid_final = KonzaTrinoOperator(
        task_id='create_mpi_accid_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_accid_final
        (accid_ref varchar, 
        mpi varchar) WITH (
        bucketed_by = ARRAY['accid_ref'], 
            sorted_by = ARRAY['accid_ref'],
            bucket_count = 32 )
        """,
    )
    insert_mpi_accid_final = KonzaTrinoOperator(
        task_id='insert_mpi_accid_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_accid_final
SELECT accid_ref, mpi
FROM (
    SELECT accid_ref, mpi, ROW_NUMBER() OVER (PARTITION BY accid_ref) AS row_num
    FROM hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks
) AS subquery
WHERE row_num = 1 """,
    )
    drop_accid_state_distinct_rank_1_mpi_final = KonzaTrinoOperator(
        task_id='drop_accid_state_distinct_rank_1_mpi_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_mpi_final
        """,
    )
    create_accid_state_distinct_rank_1_mpi_final = KonzaTrinoOperator(
        task_id='create_accid_state_distinct_rank_1_mpi_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_mpi_final
        (rank bigint,
        patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar,
        mpi varchar,
        index_update varchar) WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['patient_id'], 
    sorted_by = ARRAY['patient_id'],
    bucket_count = 64
)
        """,
    )
    insert_accid_state_distinct_rank_1_mpi_final = KonzaTrinoOperator(
        task_id='insert_accid_state_distinct_rank_1_mpi_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_mpi_final
        (select ST.*, MPI.mpi, MPI.index_update from sup_12760_c59_accid_state_distinct_rank_1_final ST
        JOIN sup_12760_c59_mpi_accid_final MPI on ST.patient_id = MPI.accid_ref)
        """,
    )
    drop_mpi_state_index = KonzaTrinoOperator(
        task_id='drop_mpi_state_index',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index
        """,
    )
    create_mpi_state_index = KonzaTrinoOperator(
        task_id='create_mpi_state_index',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index
        (mpi varchar,
        state varchar, 
        index_update_dt_tm varchar, 
        index_update varchar)  WITH (
    partitioned_by = ARRAY['index_update'], 
    bucketed_by = ARRAY['mpi'], 
    sorted_by = ARRAY['mpi'],
    bucket_count = 64
)
        """,
    )
    insert_mpi_state_index = KonzaTrinoOperator(
        task_id='insert_mpi_state_index',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index
        (select mpi, state, index_update_dt_tm, index_update from sup_12760_c59_accid_state_distinct_rank_1_mpi_final)
        """,
    )
    drop_mpi_state_index_distinct_final = KonzaTrinoOperator(
        task_id='drop_mpi_state_index_distinct_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_final
        """,
    )
    create_mpi_state_index_distinct_final = KonzaTrinoOperator(
        task_id='create_mpi_state_index_distinct_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_final
        (mpi varchar,
        state varchar, 
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index_distinct_final = KonzaTrinoOperator(
        task_id='insert_mpi_state_index_distinct_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_final
(select DISTINCT * from sup_12760_c59_mpi_state_index)
        """,
    )
    drop_mpi_state_index_distinct_rank_final = KonzaTrinoOperator(
        task_id='drop_mpi_state_index_distinct_rank_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_final
        """,
    )
    create_mpi_state_index_distinct_rank_final = KonzaTrinoOperator(
        task_id='create_mpi_state_index_distinct_rank_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_final
        (rank_per_mpi bigint,
        mpi varchar, 
        state varchar, 
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index_distinct_rank_final = KonzaTrinoOperator(
        task_id='insert_mpi_state_index_distinct_rank_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_final
        (select row_number() OVER(Partition by T.mpi ORDER BY T.index_update_dt_tm DESC) as rank_per_mpi,* from sup_12760_c59_mpi_state_index_distinct_final T)
        """,
    )
    drop_mpi_state_index_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='drop_mpi_state_index_distinct_rank_1_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_1_final
        """,
    )
    create_mpi_state_index_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='create_mpi_state_index_distinct_rank_1_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_1_final
        (rank_per_mpi bigint,
        mpi varchar, 
        state varchar, 
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='insert_mpi_state_index_distinct_rank_1_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_1_final
        (select * from sup_12760_c59_mpi_state_index_distinct_rank_final where rank_per_mpi = 1)
        """,
    )
    create_accid_by_state_prep__final >> insert_accid_by_state_prep__final >> drop_accid_by_state_final >> create_accid_by_state_final >> insert_accid_by_state_final >> drop_accid_by_state_distinct__final >> create_accid_by_state_distinct__final >> insert_accid_by_state_distinct__final >> drop_accid_state_distinct_rank_final >> create_accid_state_distinct_rank_final >> insert_accid_state_distinct_rank_final >> drop_accid_state_distinct_rank_1_final >> create_accid_state_distinct_rank_1_final >> insert_accid_state_distinct_rank_1_final >> drop_mpi_accid_prep_final >> create_mpi_accid_prep_final >> drop_mpi_accid_no_blanks >> create_mpi_accid_no_blanks >> insert_mpi_accid_no_blanks >> drop_mpi_accid_final >> create_mpi_accid_final >> insert_mpi_accid_final >> drop_accid_state_distinct_rank_1_mpi_final >> create_accid_state_distinct_rank_1_mpi_final >> insert_accid_state_distinct_rank_1_mpi_final >> drop_mpi_state_index >> create_mpi_state_index >> insert_mpi_state_index >> drop_mpi_state_index_distinct_final >> create_mpi_state_index_distinct_final >> insert_mpi_state_index_distinct_final >> drop_mpi_state_index_distinct_rank_final >> create_mpi_state_index_distinct_rank_final >> insert_mpi_state_index_distinct_rank_final >> drop_mpi_state_index_distinct_rank_1_final >> create_mpi_state_index_distinct_rank_1_final >> insert_mpi_state_index_distinct_rank_1_final
