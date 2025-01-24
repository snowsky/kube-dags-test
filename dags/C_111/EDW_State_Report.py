from airflow.hooks.base import BaseHook
import trino
import logging
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


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
                
                # Check the status of the query
                query_id = cursor.query_id
                cursor.execute(f"SELECT state FROM system.runtime.queries WHERE query_id = '{query_id}'")
                status = cursor.fetchone()[0]

                if status != 'FINISHED':
                    raise AirflowException(f"Query did not finish successfully. Status: {status}")

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
    drop_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='drop_accid_by_state_prep__final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
        """,
    )
    create_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='create_accid_by_state_prep__final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
        ( patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar)
        """,
    )
    insert_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_prep__final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
      (SELECT 
    patient_id, 
    index_update_dt_tm, 
    CASE 
        WHEN state IN ('01', '1', 'AL', 'al', 'aL', 'Al', 'Alabama', 'alabama') THEN 'Alabama'
        WHEN state IN ('02', '2', 'AK', 'ak') THEN 'Alaska'
        WHEN state IN ('AMERICANSAMOA', 'AS') THEN 'American Samoa'
        WHEN state IN ('04', 'Ari', 'ARIZONA', 'Arizona', 'AZ', 'Az', 'az', 'AZ ') THEN 'Arizona'
        WHEN state IN ('05', 'AR', 'ar', 'aR', 'Ar', 'ARK', 'Ark', 'Arkansas') THEN 'Arkansas'
        WHEN state IN ('AA', 'aa', 'AE', 'AP', 'AP ', 'ARMEDFORCESOTHER', 'ARMFORCAMEREXCPTCAN') THEN 'Armed Forces'
        WHEN state IN ('06', '95403', '95404', '95476', ' CA', 'CA', 'Ca', 'ca', 'cA', 'CA ', 'California') THEN 'California'
        WHEN state IN ('08', '8', 'CO', 'Co', 'co', 'Colorado') THEN 'Colorado'
        WHEN state IN ('09', '9', '06114', '06278', '06810', 'CT', 'ct', 'Ct', 'CT ') THEN 'Connecticut'
        WHEN state IN ('10', 'DE', 'Delaware') THEN 'Delaware'
        WHEN state IN ('11', 'DC') THEN 'District of Columbia'
        WHEN state IN ('12', 'FL', 'fl', 'Fl', 'fL', 'FL ', 'Florida') THEN 'Florida'
        WHEN state IN ('13', 'GA', 'Ga', 'ga', 'gA', 'GA ', 'Ga ', 'Georgia') THEN 'Georgia'
        WHEN state IN ('66', 'GU') THEN 'Guam'
        WHEN state IN ('15', 'HI', 'Hi') THEN 'Hawaii'
        WHEN state IN ('16', 'ID') THEN 'Idaho'
        WHEN state IN ('17', 'IL', 'Il', 'il', 'Ilinois', 'Ill', 'ill', 'Illinois') THEN 'Illinois'
        WHEN state IN ('18', 'IN', 'in', 'In', 'Indiana', 'INDIANA') THEN 'Indiana'
        WHEN state IN ('19', 'IA', 'ia', 'Iowa', 'iowa') THEN 'Iowa'
        WHEN state IN ('20', 'Ka', 'Kan', 'Kansas', 'KANSAS', 'kansas', 'kANSAS', 'Kasnas', 'KS', 'ks', 'Ks', 'kS', 'KS ', 'ks ', 'KS - KANSAS', 'KSks') THEN 'Kansas'
        WHEN state IN ('21', 'KY', 'kY') THEN 'Kentucky'
        WHEN state IN ('22', '71082', 'LA', 'la', 'La', 'lA', 'LA ') THEN 'Louisiana'
        WHEN state IN ('23', 'ME') THEN 'Maine'
        WHEN state IN ('MH') THEN 'Marshall Islands'
        WHEN state IN ('24', 'MD', 'md', 'mD') THEN 'Maryland'
        WHEN state IN ('25', 'MA', 'mA', 'ma', 'Massachusetts') THEN 'Massachusetts'
        WHEN state IN ('26', 'MI', 'Mi', 'mi', 'MICH', 'Michigan') THEN 'Michigan'
        WHEN state IN ('27', 'Min', 'Minnesota', 'MN', 'mn', 'Mn', 'MN ') THEN 'Minnesota'
        WHEN state IN ('28', 'MS', 'ms', 'Ms', 'mS') THEN 'Mississippi'
        WHEN state IN ('29', 'Mis', 'Missouri', 'MISSOURI', 'missouri', 'MO', 'mo', 'Mo', 'mO', 'MO ', 'MO 64106', 'MO64030') THEN 'Missouri'
        WHEN state IN ('30', 'Mon', 'Montana', 'MT', 'mt', 'MT ') THEN 'Montana'
        WHEN state IN ('31', 'NE', 'Ne', 'nE', 'ne', 'Nebraska') THEN 'Nebraska'
        WHEN state IN ('32', 'NV', 'nv', 'NV ') THEN 'Nevada'
        WHEN state IN ('33', 'NH') THEN 'New Hampshire'
        WHEN state IN ('34', 'NJ', 'nj') THEN 'New Jersey'
        WHEN state IN ('35', 'New Mexico', 'NM', 'Nm') THEN 'New Mexico'
        WHEN state IN ('36', 'New York', 'NY', 'Ny', 'ny', 'NY ') THEN 'New York'
        WHEN state IN ('AB', 'ab', 'Alberta', 'ALBERTA', 'B.C.', 'BC', 'British Columbia', 'BRITISHCOLUMBIA', 'CAN', 'Canada', 'CANADA', 'canada', 'Manitoba', 'MANITOBA', 'MB', 'NB', 'NL', 'Northwest Territories', 'Nova Scotia', 'NOVA SCOTIA', 'NOVASCOTIA', 'NS', 'ON', 'On', 'on', 'ONT', 'Ontario', 'ONTARIO', 'PE', 'PQ', 'QC', 'Quebec', 'QUEBEC', 'Saskatchewan', 'SK') THEN 'Canada'
        WHEN state IN ('AF') THEN 'Afghanistan'
        WHEN state IN ('AG') THEN 'Algeria'
        WHEN state IN ('AU', 'Au') THEN 'Australia'
        WHEN state IN ('BA') THEN 'Bosnia and Herzegovina'
        WHEN state IN ('BB') THEN 'Barbados'
        WHEN state IN ('BL') THEN 'Saint Barthélemy'
        WHEN state IN ('BR') THEN 'Brazil'
        WHEN state IN ('BS') THEN 'Bahamas'
        WHEN state IN ('Bulgaria') THEN 'Bulgaria'
        WHEN state IN ('BY') THEN 'Belarus'
        WHEN state IN ('CH') THEN 'Switzerland'
        WHEN state IN ('CHIH') THEN 'Mexico'
        WHEN state IN ('CHINA', 'CN') THEN 'China'
        WHEN state IN ('CI') THEN 'Cote dIvoire'
        WHEN state IN ('CL') THEN 'Chile'
        WHEN state IN ('CP') THEN 'Clipperton Island'
        WHEN state IN ('CR') THEN 'Costa Rica'
        WHEN state IN ('CS') THEN 'Serbia and Montenegro'
        WHEN state IN ('CZ', 'Czechia') THEN 'Czech Republic'
        WHEN state IN ('DF', 'DG') THEN 'Mexico'
        WHEN state IN ('DK') THEN 'Denmark'
        WHEN state IN ('DO', 'DR') THEN 'Dominican Republic'
        WHEN state IN ('EG') THEN 'Egypt'
        WHEN state IN ('England', 'england', 'ENGLAND') THEN 'England'
        WHEN state IN ('ES') THEN 'Spain'
        WHEN state IN ('ESTADODEMEXICO') THEN 'Mexico'
        WHEN state IN ('FEDSTATESMICRONESIA', 'FM') THEN 'Federated States of Micronesia'
        WHEN state IN ('FO', 'Fo') THEN 'Faroe Islands'
        WHEN state IN ('FR', 'France') THEN 'France'
        WHEN state IN ('GB') THEN 'Great Britain'
        WHEN state IN ('GE') THEN 'Georgia'
        WHEN state IN ('Germany', 'GERMANY', 'germany') THEN 'Germany'
        WHEN state IN ('GM') THEN 'Gambia'
        WHEN state IN ('GR') THEN 'Greece'
        WHEN state IN ('GT') THEN 'Guatamala'
        WHEN state IN ('HO') THEN 'Hondurus'
        WHEN state IN ('IE') THEN 'Ireland'
        WHEN state IN ('IO', 'io') THEN 'British Indian Ocean Territory'
        WHEN state IN ('Ireland') THEN 'Ireland'
        WHEN state IN ('IS') THEN 'Iceland'
        WHEN state IN ('IT') THEN 'Italy'
        WHEN state IN ('JA', 'JP') THEN 'Japan'
        WHEN state IN ('Jalisco', 'JALISCO') THEN 'Mexico'
        WHEN state IN ('Jamaica', 'JM') THEN 'Jamaica'
        WHEN state IN ('JC') THEN 'French Southern Territories'
        WHEN state IN ('KD', 'Kd') THEN 'Kaduna'
        WHEN state IN ('KE') THEN 'Kenya'
        WHEN state IN ('KT') THEN 'Christmas Island'
        WHEN state IN ('KZ') THEN 'Kazakhstan'
        WHEN state IN ('LM') THEN 'Malta'
        WHEN state IN ('LO') THEN 'Slovakia'
        WHEN state IN ('LS') THEN 'Lesotho'
        WHEN state IN ('MG') THEN 'Madagascar'
        WHEN state IN ('MJ') THEN 'Montenegro'
        WHEN state IN ('MM') THEN 'Myanmar'
        WHEN state IN ('mY') THEN 'Malaysia'
        WHEN state IN ('N.L.') THEN 'Netherlands'
        WHEN state IN ('nA') THEN 'Namibia'
        WHEN state IN ('NAY', 'NAYARIT') THEN 'Mexico'
        WHEN state IN ('NG') THEN 'Nigeria'
        WHEN state IN ('NI') THEN 'Nicaragua'
        WHEN state IN ('NL') THEN 'Newfoundland and Labrador'
        WHEN state IN ('Northwest Territories') THEN 'Northwest Territories'
        WHEN state IN ('Nova Scotia', 'NOVA SCOTIA', 'NOVASCOTIA') THEN 'Nova Scotia'
        WHEN state IN ('NP') THEN 'Nepal'
        WHEN state IN ('NSW') THEN 'New South Wales'
        WHEN state IN ('NT') THEN 'Netherlands Antilles'
        WHEN state IN ('NW') THEN 'Northern Mariana Islands'
        WHEN state IN ('NZ') THEN 'New Zealand'
        WHEN state IN ('ON', 'On', 'on', 'ONT', 'Ontario', 'ONTARIO') THEN 'Ontario'
        WHEN state IN ('OO', 'oo') THEN 'Oman'
        WHEN state IN ('OS') THEN 'Austria'
        WHEN state IN ('Out of Country', 'Out of USA', 'OUTSIDE OF U') THEN 'UNKNOWN'
        WHEN state IN ('PE') THEN 'Canada'
        WHEN state IN ('PJ') THEN 'Etorofu, Habomai, Kunashiri, and Shikotan Islands'
        WHEN state IN ('PN') THEN 'Pitcairn Islands'
        WHEN state IN ('PO') THEN 'Portugal'
        WHEN state IN ('PQ') THEN 'Canada'
        WHEN state IN ('PS') THEN 'Palestine'
        WHEN state IN ('QA') THEN 'Qatar'
        WHEN state IN ('QC') THEN 'Canada'
        WHEN state IN ('QT') THEN 'Oceania'
        WHEN state IN ('QU') THEN 'Polynesia'
        WHEN state IN ('Quebec', 'QUEBEC') THEN 'Quebec'
        WHEN state IN ('RO') THEN 'Romania'
        WHEN state IN ('RU', 'RUSSIA') THEN 'Russia'
        WHEN state IN ('RW') THEN 'Rwanda'
        WHEN state IN ('SA', 'sa') THEN 'South Australia'
        WHEN state IN ('Saskatchewan') THEN 'Canada'
        WHEN state IN ('SE') THEN 'Sweden'
        WHEN state IN ('SF') THEN 'South Africa'
        WHEN state IN ('SK') THEN 'Canada'
        WHEN state IN ('SL') THEN 'Sierra Leone'
        WHEN state IN ('SO', 'So') THEN 'Somalia'
        WHEN state IN ('SP', 'Sp') THEN 'Spain'
        WHEN state IN ('SV') THEN 'El Salvador'
        WHEN state IN ('SW', 'Sweden') THEN 'Sweden'
        WHEN state IN ('Switzerlan') THEN 'Switzerland'
        WHEN state IN ('TC') THEN 'Turks and Caicos'
        WHEN state IN ('TH') THEN 'Thailand'
        WHEN state IN ('TL') THEN 'Timor-Leste'
        WHEN state IN ('TM') THEN 'Turkmenistan'
        WHEN state IN ('uk') THEN 'United Kingdom'
        WHEN state IN ('UR') THEN 'Union of Soviet Socialist Republics'
        WHEN state IN ('UV') THEN 'Burkina Faso'
        WHEN state IN ('ve') THEN 'Venezuela'
        WHEN state IN ('WS') THEN 'Samoa'
        WHEN state IN ('XF') THEN 'Fujairah'
        WHEN state IN ('YT') THEN 'Mayotte'
        WHEN state IN ('ZJ') THEN 'Zhejiang Province'
        WHEN state IN ('37', 'NC', 'nc', 'nC', 'NC ') THEN 'North Carolina'
        WHEN state IN ('38', 'ND') THEN 'North Dakota'
        WHEN state IN ('MP') THEN 'Northern Mariana Islands'
        WHEN state IN ('39', 'OH', 'oh', 'OH ', 'ohio', 'Ohio') THEN 'Ohio'
        WHEN state IN ('40', 'OK', 'ok', 'Ok', 'oK', 'OK ', 'Okl', 'Oklahoma', 'OKLAHOMA') THEN 'Oklahoma'
        WHEN state IN ('41', 'OR', 'or', 'oR', 'OR ', 'Oregon') THEN 'Oregon'
        WHEN state IN ('42', 'PA', 'pa', 'PA ', 'Pennsylvania') THEN 'Pennsylvania'
        WHEN state IN ('PR', 'PUE') THEN 'Puerto Rico'
        WHEN state IN ('44', 'RI') THEN 'Rhode Island'
        WHEN state IN ('45', 'SC', 'sc', 'Sc', 'sC', 'South Carolina') THEN 'South Carolina'
        WHEN state IN ('46', 'SD', 'sd') THEN 'South Dakota'
        WHEN state IN ('47', 'Tennessee', 'TN', 'tN', 'tn', 'Tn', 'TN ') THEN 'Tennessee'
        WHEN state IN ('48', 'Tex', 'Texas', 'texas', 'TEXAS', 'TX', 'tx', 'Tx', 'tX', 'TX ', 'TX.') THEN 'Texas'
        WHEN state IN ('UM') THEN 'United States Minor Outlying Islands'
        WHEN state IN ('00', '0', '00000', '0.3', '03', '07', '14', '67', '94', '95', '96', '97', '99', '-', '--', ' ', '  ', '   ', ' K', ' LIVING', ' N', ' Rehab', ' REHAB CENTER', ' REHABILITATION', ' REHABILITATION CENTER', '*', '**', ',', '.', '..', '/', '//', ']]', '__', '0R', '8-', 'A', 'a', 'AGS', 'Aichi', 'ANT', 'ARMA', 'BASEL', 'Bavaria', 'BAVARIA', 'Bel Aire', 'BIRD CITY', 'Blackwell', 'BOP', 'C', 'C9', 'Cambria', 'Cambridges', 'Campeche', 'candada', 'CC', 'CD', 'CD:', 'CD:1409749313', 'CD:1409749429', 'CD:1555486566', 'CD:1555486720', 'CD:1555487113', 'CD:309242', 'CD:309246', 'CD:309247', 'CD:309260', 'CD:309283', 'CD:309292', 'CD:309298', 'CD:36340707', 'CD:36340710', 'CD:36340717', 'CD:36340737', 'CD:36340838', 'CD:36340868', 'CD:670132', 'CD:750761272', 'CD:8339441405', 'CD:845539', 'CD:845542', 'cdmx', 'Chaba', 'CHERRYVALE', 'CHESHIRE', 'Chiapas', 'Chili', 'CHIS', 'CIUDAD DE ME', 'Col', 'Colchester', 'Colorade', 'CONCORDIA', 'Derby', 'DS', 'Du', 'EAST ORANGE', 'EDISON', 'EN', 'EXCELSIOR SPR', 'EXCELSIOR SPRINGS', 'F', 'FA', 'Fair Oaks', 'FC', 'FZ', 'GAGA', 'Geo', 'GO', 'Halstead', 'hanavor', 'Hayfor', 'Hayfork', 'HAYFORK', 'HE', 'HGO', 'HIL', 'Hoxie', 'Hyampom', 'I', 'IW', 'JENNINGS', 'Junction City', 'K', 'k', 'K6', 'KA', 'KC', 'KDS', 'Ken', 'L3', 'Lawrence', 'london', 'M', 'M0', 'm0', 'MANITOPA', 'MCPHERSON', 'METUCHEN', 'MINNEAPOLIS', 'MOR', 'Mulvane', 'N', 'n', 'N.', 'N/A', 'NA', 'Na', 'na', 'NB', 'Neodesha', 'NEODESHA', 'NEW', 'NN', 'NO', 'No', 'NO STATE INDICATED', 'None', 'NOTKNOWN', 'NU', 'null', 'NULL', 'NYC', 'O', 'OA', 'OAXACA', 'Oaxaca', 'OC', 'OHI', 'OT', 'OTH', 'Other', 'PEABODY', 'Portola', 'PRATT', 'QB', 'QD', 'QL', 'QLD', 'Redding', 'RX', 'S', 's', 'SABETHA', 'San Luis Potasi', 'Santiago', 'SED', 'SEOUL', 'Serbia', 'Spearville', 'ST', 'State', 'TAB', 'TABASCO', 'TAMPS', 'TE', 'Tirol', 'TLAX', 'Trinity Pine', 'U', 'u', 'UK', 'UN', 'un', 'Un', 'UNABLE TO CO', 'UNKNOWN', 'Unknown', 'US', 'UU', 'V', 'vauxhall', 'VIENNA', 'WC', 'Weaverville', 'Wes', 'WESTERN', 'WESTMORELAND', 'Wichita', 'WICHITA', 'WILLMARS', 'X', 'x', 'XX', 'xx', 'XXOTHER', 'XX-Other', 'XY', 'Y', 'y', 'YATES CENTER', 'YO', 'YUBA CITY', 'YUC', 'YY', 'Z', 'ZZ') THEN 'UNKNOWN'
        WHEN state IN ('49', 'UT', 'ut', 'UT ') THEN 'Utah'
        WHEN state IN ('50', 'VT') THEN 'Vermont'
        WHEN state IN ('78', 'VI', 'Virgin Islands') THEN 'Virgin Islands'
        WHEN state IN ('51', 'VA', 'va') THEN 'Virginia'
        WHEN state IN ('53', 'WA', 'wa', 'Wa', 'WA ') THEN 'Washington'
        WHEN state IN ('54', 'WV') THEN 'West Virginia'
        WHEN state IN ('55', 'WI', 'wi', 'wI', 'Wi', 'WI ', 'Wisconsin') THEN 'Wisconsin'
        WHEN state IN ('56', 'WY', 'wy', 'WY ') THEN 'Wyoming'
        ELSE 'UNKNOWN'
    END AS state_standardized
FROM patient_contact_parquet_pm)
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
      state varchar)
        """,
    )
    insert_accid_by_state_final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_final
        (select * from sup_12760_c59_accid_by_state_prep__final where state <> '')
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
        state varchar)
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
state varchar)
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
        state varchar)
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
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final
        (mpi varchar, 
        accid_ref varchar, 
        index_update varchar
        WITH ( 
        partition_projection_format = 'yyyy-MM', 
        partition_projection_interval = 1, 
        partition_projection_interval_unit = 'DAYS', 
        partition_projection_range = ARRAY['2023-03','2024-10'], --'NOW' --can be used if in the current month EG. if there is no month folder for 2024-08, and it is August, must set a range to the prior month
        partition_projection_type = 'DATE' ) ) WITH 
        ( external_location = 'abfs://content@reportwriterstorage.dfs.core.windows.net/parquet-master-data/mpi',
        format = 'PARQUET', partition_projection_enabled = true, 
        partition_projection_location_template = 'abfs://content@reportwriterstorage.dfs.core.windows.net/parquet-master-data/mpi/${index_update}', 
        partitioned_by = ARRAY['index_update'] )
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
        mpi varchar)
        """,
    )
    insert_mpi_accid_final = KonzaTrinoOperator(
        task_id='insert_mpi_accid_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_accid_final
    (select accid_ref, mpi from hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks)
        """,
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
        mpi varchar)
        """,
    )
    insert_accid_state_distinct_rank_1_mpi_final = KonzaTrinoOperator(
        task_id='insert_accid_state_distinct_rank_1_mpi_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_mpi_final
        (select ST.*, MPI.mpi from sup_12760_c59_accid_state_distinct_rank_1_final ST
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
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index = KonzaTrinoOperator(
        task_id='insert_mpi_state_index',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index
        (select mpi, state, index_update_dt_tm from sup_12760_c59_accid_state_distinct_rank_1_mpi_final)
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
    drop_accid_by_state_prep__final >> create_accid_by_state_prep__final >> insert_accid_by_state_prep__final >> drop_accid_by_state_final >> create_accid_by_state_final >> insert_accid_by_state_final >> drop_accid_by_state_distinct__final >> create_accid_by_state_distinct__final >> insert_accid_by_state_distinct__final >> drop_accid_state_distinct_rank_final >> create_accid_state_distinct_rank_final >> insert_accid_state_distinct_rank_final >> drop_accid_state_distinct_rank_1_final >> create_accid_state_distinct_rank_1_final >> insert_accid_state_distinct_rank_1_final >> drop_mpi_accid_prep_final >> create_mpi_accid_prep_final >> drop_mpi_accid_no_blanks >> create_mpi_accid_no_blanks >> insert_mpi_accid_no_blanks >> drop_mpi_accid_final >> create_mpi_accid_final >> insert_mpi_accid_final >> drop_accid_state_distinct_rank_1_mpi_final >> create_accid_state_distinct_rank_1_mpi_final >> insert_accid_state_distinct_rank_1_mpi_final >> drop_mpi_state_index >> create_mpi_state_index >> insert_mpi_state_index >> drop_mpi_state_index_distinct_final >> create_mpi_state_index_distinct_final >> insert_mpi_state_index_distinct_final >> drop_mpi_state_index_distinct_rank_final >> create_mpi_state_index_distinct_rank_final >> insert_mpi_state_index_distinct_rank_final >> drop_mpi_state_index_distinct_rank_1_final >> create_mpi_state_index_distinct_rank_1_final >> insert_mpi_state_index_distinct_rank_1_final
