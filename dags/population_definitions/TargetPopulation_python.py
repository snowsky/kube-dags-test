try:
    import paramiko
    import time
    import os
    import sys
    import pandas as pd
    import pymysql
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session
    from sqlalchemy.orm import sessionmaker
    import pyhive
    import pyodbc
    import datetime
    import json
    import subprocess
    import numpy as np
    import re
    import imp
    #print("Made it here")
    #time.sleep(4)

    targetTable = str(sys.argv[1])
    clientFileName = str(sys.argv[2])
    FolderName = str(sys.argv[3])
    #targetTable = 'tppopulationpathexample'
    #clientFileName = 'CSVPathExampleupdated.csv'
    #FolderName = '1098'
    #monthStart = '0'
    #fileDateString = monthStart + ''  # Date Run of range start limit is set to date the run was performed
    Login = imp.load_source('Login', '//prd-az1-sqlw2.ad.konza.org/Batch/Login.py')


    #Login = imp.load_source('Login', '//prd-az1-sqlw2.ad.konza.org/Batch/Clients/Login.py') ##ddooley login

    host = Login.host
    port = 3306
    user = Login.user
    passwd = Login.password
    db = Login.dbname
    db = 'temp'
    engine = create_engine(
        "mysql+pymysql://" + user + ":" + passwd + "@" + host + ":3306/" + db + "?charset=utf8&local_infile=1")

    print("Target Table: " + targetTable)

    pg_password = Login.pg_mgmtops_password
    pg_server = Login.pg_mgmtops_server
    pg_user = Login.pg_mgmtops_user

#     pg_user = Login.pg_user
#     pg_server = Login.pg_server
#     pg_password = Login.pg_password
# #
    pgengine = create_engine(
        "postgresql+pg8000://" + pg_user + ":" + pg_password + "@" + pg_server + "/postgres")
    # 'RE: Measure Definition: New ACO Diabetes Poor Control Measure  PMT-000415'
    connection = pgengine.connect()
    dbcreds = connection.execute(
        """select * from sftp_credentials_parent_mgmt where client_security_groupings_admins_name = '""" + FolderName + """'; """)
    dfdbcreds = pd.DataFrame(dbcreds.fetchall())
    connection.close()
    if not dfdbcreds.empty:
        dfdbcreds.columns = dbcreds.keys()

    dbcred_sftp_server = dfdbcreds['sftp_server'][0]
    dbcred_sftp_port = dfdbcreds['sftp_port'][0]
    dbcred_sftp_user = dfdbcreds['sftp_user'][0]
    dbcred_sftp_password = dfdbcreds['sftp_password'][0]
    dbcred_quality_review_location = dfdbcreds['quality_review_location'][0]
    dbcred_sftp_root_directory = dfdbcreds['sftp_root_directory'][0]
    dbcred_state_based_appending = dfdbcreds['state_based_appending'][0]

    # dbcred_sftp_server = 'sftp.konza.org'
    # dbcred_sftp_port = '22'
    # dbcred_sftp_user = '#######'
    # dbcred_sftp_password = '#########'
    # dbcred_quality_review_location = '//PRD-AZ1-OPS1.ad.konza.org/Analytic Reports/Quality Review/1098/'
    # dbcred_sftp_root_directory = '/Test_AA/'
    # dbcred_state_based_appending = 'New Jersey'

    print("Target Table: " + targetTable)

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=dbcred_sftp_server, username=dbcred_sftp_user, password=dbcred_sftp_password)
    ftp_client = ssh_client.open_sftp()
    # cluster_ssh_def.envURL
    sourcePath = dbcred_sftp_root_directory

    #destPath = '\\\\prd-az1-sqlw2.ad.konza.org\\batch\Clients\\1098\\'
    #destPath = '\\\\prd-az1-ops1.ad.konza.org\\Analytic Reports\Quality Review\\Panel Load Checks\\1098\\'
    destPath = dbcred_quality_review_location + '/' + FolderName + '/'
    if not os.path.exists(destPath):
        os.makedirs(destPath)
    dest2Path = '\\\\prd-az1-ops1\\Analytic Reports\Quality Review\\Panel Load Checks\\State Eligible Populations\\' + dbcred_state_based_appending
    # dest2Path = '\\\\qa-az1-ops1\\Analytic Reports\Quality Review\\Panel Load Checks\\State Eligible Populations\\' + dbcred_state_based_appending

    for f in ftp_client.listdir(path=sourcePath):
        print(f)
        attributes = ftp_client.lstat(sourcePath + '/' + f)
    latest = 0
    latestfile = None

    for fileattr in ftp_client.listdir_attr(sourcePath):
        if fileattr.filename.endswith('.csv') and fileattr.st_mtime > latest:
            latest = fileattr.st_mtime
            latestfile = fileattr.filename

    if latestfile is not None:
        ftp_client.get(sourcePath + '/' + latestfile, destPath + '/' + latestfile)

    ftp_client.close()
    ssh_client.close()
    dfClientFile = pd.read_csv(destPath + '/' + latestfile)
    dfClientFile.to_csv('\\\\prd-az1-ops1\\Analytic Reports\Quality Review\\' + FolderName + '/Loaded Population.csv')
    # dfClientFile = pd.read_csv('E:\Clients\1098/EXAMPLEPANEL.csv')
    #dfClientFile = pd.read_excel(destPath + '' + clientFileName)
    #dfClientFile['DOB'] = pd.to_datetime(dfClientFile['Patient Date of Birth'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    #dfClientFile['FirstANDMidName'] = dfClientFile['Patient First Name'].str.lstrip(r' ')
    #dfClientFile['FirstName'] = dfClientFile['FirstANDMidName'].str.split(' ').str[0]
    #dfClientFile['FirstName'] = dfClientFile['FirstName'].str.split(' ').str[0]
    #dfClientFile['LastName'] = dfClientFile['Field Name'].str.split(' ').str[0]
    #dfClientFile['LastName'] = dfClientFile['LastName'].str.split(' ').str[0]
    ## Change Detection Cases
    # If column names have changed - alert quality team
    # dfClientFile = dfClientFile.head(100)
    
    dfClientFile.dropna(subset=['Gender'], inplace=True)
    dfClientFile['Attributed Provider NPI'] = dfClientFile['Attributed Provider NPI'].apply(lambda f: format(f, '.0f'))

    ##AA EDITS :
    # Check for Schedule A Start Here
    ### DAVID REMOVING SCHEDULE A UNTIL WE FIX THE CODE ###
    try:
        titleString = os.path.basename(sys.argv[0])
    except:
        titleString = 'SCHEDULE A PATIENT PANEL'
    try:
        localtime = str(time.asctime(time.localtime(time.time())))
    except:
        localtime = ''
    try:
        lineNumber = str(sys.exc_info()[-1].tb_lineno)
    except:
        lineNumber = ''
    dfClientFile.columns
    # PUT ALL REQUIRED FIELDS IN DATAFRAME
    # dfOldPatientPanel = dfClientFile.columns.values.tolist()
    
    dfOldPatientPanel = ['Patient First Name', 'Patient Last Name', 'DOB', 'Gender']
    # list of column required
    dfNewPatientPanel = ['Patient First Name', 'Patient Last Name', 'DOB', 'Gender']
    for i in dfOldPatientPanel:
        # dfcompareColumns['name'] = [i]
        if (i in dfNewPatientPanel):
            print("The column heading names match")
        else:
            print("The column heading names match do not match")
            errormsg_1 = "The column headers do not match"
            # errorFileLocation = "C:/Users/aagarwal\Desktop\AA- sprint\Run_Errors.txt"
            errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
    
            # trace = traceback.print_tb(limit=2500)
            f = open(errorFileLocation, "a")
            f.write("Time: " + localtime + " | Error Details: " + str(errormsg_1) + " | Line: " + str(
                lineNumber) + " | File: " + titleString + " \n")
            f.close()
    # to check if required field is empty
    
    dfChecks_requiredField_prep = pd.DataFrame(columns=['Patient First Name', 'Patient Last Name', 'DOB', 'Gender'])
    
    # 2. First name is aplha
    # dfChecks_requiredField_prep['Patient First Name'] = dfClientFile['Patient First Name']
    dfChecks_requiredField_prep['Patient First Name'] = dfClientFile['Patient First Name'].str.lower()
    for i in dfChecks_requiredField_prep['Patient First Name']:
        # print (i)
        alpha_regex = "^[a-z A-Z]+$"
        if re.match(alpha_regex, str(i)):
            dfChecks_requiredField_prep['Patient_First_Name_Check'] = 'True'
        else:
            dfChecks_requiredField_prep['Patient_First_Name_Check'] = 'False'
    
    
    ## last name is aplha
    dfChecks_requiredField_prep['Patient Last Name'] = dfClientFile['Patient Last Name'].str.lower()
    # dfChecks_requiredField_prep['Patient_Last_Name_Check'] = dfChecks_requiredField_prep['Patient Last Name'].str.isalpha()
    for i in dfChecks_requiredField_prep['Patient Last Name']:
        # print (i)
        alpha_regex = "^[a-z A-Z]+$"
        if re.match(alpha_regex, str(i)):
            dfChecks_requiredField_prep['Patient_Last_Name_Check'] = 'True'
        else:
            dfChecks_requiredField_prep['Patient_Last_Name_Check'] = 'False'
    # dfChecks_requiredField_prep['Patient_Last_Name_Check'] = dfChecks_requiredField_prep['Patient Last Name'].re.match("^[a-z A-Z]*$")
    
    # 2. format of DOB
    try:
        dfChecks_requiredField_prep['DOB'] = dfClientFile['DOB']
        pd.to_datetime(dfChecks_requiredField_prep['DOB'], format='%Y-%m-%d')
        dfChecks_requiredField_prep['dob_check'] = "True"
    except:
        dfChecks_requiredField_prep['dob_check'] = 'False'
    
    # 3. Format of Gender -only one character
    # csv_read.columns
    dfChecks_requiredField_prep['Gender'] = dfClientFile['Gender']
    dfChecks_requiredField_prep['Gender_len'] = dfChecks_requiredField_prep['Gender'].apply(len)
    # compare_file['Gender_len'].compare_file(str)
    dfChecks_requiredField_prep['Gender_len_check'] = dfChecks_requiredField_prep['Gender_len'].apply(
        lambda x: 'True' if x == 1 else 'False')
    dfChecks_requiredField_prep['Gender_check'] = dfChecks_requiredField_prep['Gender'].str.isalpha()
    
    
    dfChecks = pd.DataFrame()
    dfChecks_requiredField_prep.columns
    dfChecks[['Patient_First_Name_Check', 'Patient_Last_Name_Check', 'dob_check', 'Gender_len_check', 'Gender_check']] = \
        dfChecks_requiredField_prep[
            ['Patient_First_Name_Check', 'Patient_Last_Name_Check', 'dob_check', 'Gender_len_check', 'Gender_check']]
    # dfChecks.to_csv("C:\\Users\\aagarwal\Desktop\AA- sprint\\AA_check.csv")
    dfcompare = dfChecks.drop_duplicates()
    # dfcompare = dfChecks.isnull().values.any()
    
    # for dfcompare.iterrows():
    for index, row in dfcompare.iterrows():
        if dfcompare.any == 'False':
            print("Requirement not Met: Fields do not meet the requirements ")
            errormsg_2 = "Requirement not Met: Fields do not meet the requirements "
            # errorFileLocation = "C:/Users/aagarwal\Desktop\AA- sprint\Run_Errors.txt"
            errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
    
            # trace = traceback.print_tb(limit=2500)
            f = open(errorFileLocation, "a")
            f.write("Time: " + localtime + " | Error Details: " + str(errormsg_2) + " | Line: " + str(
                lineNumber) + " | File: " + titleString + " \n")
            f.close()
            exit()
        else:
            print("Requirement Met: Fields meet the requirements ")
            continue
    
    
    hostGetMPI = Login.etl_runner_prd_host
    portGetMPI = 3306
    charSet = "utf8mb4"  # Character set
    cusrorType = pymysql.cursors.DictCursor
    userGetMPI = Login.etl_runner_prd_user
    passwdGetMPI = Login.etl_runner_prd_password
    db = Login.etl_runner_prd_dbname
    dbGetMPI = 'person_master'
    mySQLConnection = pymysql.connect(host=host, user=user, password=passwd,
                                      charset=charSet, cursorclass=cusrorType)

    for i, row in dfClientFile.iterrows():
        print(i)
        # print(j)
        # Create a cursor object
        # print(row['Patient First Name'])

        cursorInstance = mySQLConnection.cursor()
        # SQL query to retrieve the list of UDFs present
        sqlQuery = "select ROUTINE_SCHEMA, SPECIFIC_NAME, ROUTINE_TYPE, DATA_TYPE from information_schema.routines where ROUTINE_TYPE='FUNCTION';"
        # Execute the SQL Query
        cursorInstance.execute(sqlQuery)
        # Fetch all the rows
        udfList = cursorInstance.fetchall()
        # print("List of User Defined Functions:")
        # for udfName in udfList:
        #    print(udfName)
        #    print("-----")

        # SQL Query to invoke the UDF with parameter
        # 'FRST_NM', 'LAST_NM', 'BRTH_DT', 'GNDR_CD'
        # celsiusValue = 20.51
        # sqlQuery = "select test.CelsiusToFahrenheit(%s);" % celsiusValue
        firstname = str(row['Patient First Name']).replace("'", r"\'")  # '' returns 0
        lastname = str(row['Patient Last Name']).replace("'", r"\'")
        dob = datetime.datetime.strptime(str(row['DOB']), "%Y-%m-%d").strftime("%Y%m%d")  # '19960801'
        gender = str(row['Gender'])  # 'F'
        # print(firstname)
        # print(lastname)
        # print(dob)
        # print(gender)
        ##continue
        # mlastname varchar(100) ,mfirstname varchar(100),mdob varchar(100),msex varchar(100)
        sqlQuery = "select person_master.fn_getmpi('" + lastname + "','" + firstname + "','" + dob + "','" + gender + "');"
        # sqlQuery = "select person_master.fn_getmpi_cert('" + lastname + "','" + firstname + "','" + dob + "','" + gender + "');"


        # Execute the SQL Query
        cursorInstance.execute(sqlQuery)
        cursorInstance.close()
        mySQLConnection.commit()
        # Fetch all the rows
        results = cursorInstance.fetchall()
        print("Result of invoking the MySQL UDF from Python:")
        for result in results:
            print(result)

        # data = json.loads(str(result))

        mpi_val = str(result).split(":")[1][1:-1]
        if mpi_val == '0':
            # print("true")
            mpi_val = ''
        dfClientFile.at[i, 'mpi'] = mpi_val
    mySQLConnection.close()
    # dfClientFile = dfClientFile[~dfClientFile['mpi'].isin(dfOptOutList['MPI'])]
    dfClientFile = dfClientFile.dropna(subset=['mpi'])
    dfClientFile['blank'] = None
    dfClientFile['Managing_Organization_OID'] = '2.16.840.1.113883.3.8312'
    dfClientFile['Managing_Organization_Name'] = 'KONZA'
    dfClientFile['Service_Delivery_Preferences'] = 'submit/receive statewide ADT notifications'
    dfClientFile['DOB'] = pd.to_datetime(dfClientFile['DOB'], format='%Y-%m-%d').dt.strftime("%m/%d/%Y")
    #dfClientFile['Zip'] = dfClientFile['Zip'].astype(str)
    #dfClientFile['Patient Zip'] = np.where(dfClientFile['Zip'].str.len() == 4,
    #                                       '0' + dfClientFile['Zip'].astype(str),
    #                                       dfClientFile['Zip'].str[:5])
    ##os.copy(destPath + '/' + latestfile, dest2Path + '/' + latestfile)



    #connection = engine.connect()
    #result = connection.execute("select * FROM clientresults.opt_out_list")
    #dfOptOutList = pd.DataFrame(result.fetchall())
    #dfOptOutList.columns = result.keys()
    #connection.close()
    #dfOptOutList['MPI'] = dfOptOutList['MPI'].map(lambda x: ('%f' % x).rstrip('0').rstrip('.'))

    dfClientFilealertsprep = dfClientFile[[
        'mpi'
        , 'Patient First Name'
        , 'Patient Last Name'
        , 'DOB'
        , 'Gender'
        , 'Residential Address'
        , 'City'
        , 'State'
        , 'Zip Code'
        , 'Insurance ID'
        , 'CustomText1'
        , 'Attributed Facility Provider First Name'
        , 'Attributed Facility Provider Last Name'
        , 'Attributed Provider NPI'
    ]]

    dfClientFileprep = dfClientFile[[
        'mpi'
        , 'blank'
        , 'Patient First Name'
        , 'blank'
        , 'Patient Last Name'
        , 'blank'
        , 'DOB'
        , 'Gender'
        , 'blank'
        , 'Residential Address'
        , 'blank'
        , 'City'
        , 'State'
        , 'Zip Code'
        , 'blank'
        , 'blank'
        , 'Attributed Provider NPI'
        , 'Attributed Facility Provider First Name'
        , 'Attributed Facility Provider Last Name'
        , 'blank'
        , 'blank'
        , 'Managing_Organization_OID'
        , 'Managing_Organization_Name'
        , 'Service_Delivery_Preferences'
        , 'blank'
    ]]
    columnEndingNames = [
        'Unique Patient ID'
        , 'Secondary Client ID'
        , 'Patient First Name'
        , 'Patient Middle Initial'
        , 'Patient Last Name'
        , 'Patient Name Suffix'
        , 'Patient Date of Birth'
        , 'Gender'
        , 'SSN - Last 4'
        , 'Patient Address 1'
        , 'Patient Address 2'
        , 'Patient City'
        , 'Patient State'
        , 'Patient Zip'
        , 'Patient Phone - Mobile'
        , 'Patient Phone - Home'
        , 'Attributed Physician NPI'
        , 'Attributed Physician First Name'
        , 'Attributed Physician Last Name'
        , 'Attributed Practice Unit OID'
        , 'Attributed Practice Unit Name'
        , 'Managing Organization OID'
        , 'Managing Organization Name'
        , 'Service Delivery Preferences'
        , 'Common Key']
    dfFinal = dfClientFileprep
    dfFinal.columns = columnEndingNames
    if not os.path.exists(dest2Path + '/' + FolderName + '/'):
        os.makedirs(dest2Path + '/' + FolderName + '/')
    dfFinal.to_csv(dest2Path + '/' + FolderName + '/ACRS attribution file.csv', index=False)

    ## CSV Based
    # csv_read = pd.read_csv(
    #    '//prd-az1-sqlw2.ad.konza.org/batch/Clients/' + clientFileName
    #    ,converters={'DOB': lambda x: str(x),'CustomText1': lambda x: str(x), 'CustomText2': lambda x: str(x)})
    # Start CSV Specific alterations

    connection = engine.connect()
    result = connection.execute("select *  FROM clientresults.opt_out_list")
    dfOptOutList = pd.DataFrame(result.fetchall())
    dfOptOutList.columns = result.keys()
    connection.close()
    dfOptOutList['MPI'] = dfOptOutList['MPI'].map(lambda x: ('%f' % x).rstrip('0').rstrip('.'))
    #AA edits 08/04/2022 Ticket refrence 2622 to remove opt out patients
    # targetPopMPIidOptOutRemoved = dfClientFilealertsprep[
    #     ~dfClientFilealertsprep['mpi'].isin(dfOptOutList['MPI'])]
    targetPopMPIidOptOutRemoved = dfClientFilealertsprep

    targetPop = pd.DataFrame()
    targetPop[['firstname'
        , 'lastname'
        , 'dob'
        , 'sex'
        , 'street1'
        , 'City'
        , 'State'
        , 'zip'
        , 'mpi'
        , 'ins_member_id'
        , 'client_identifier']] \
        = targetPopMPIidOptOutRemoved[['Patient First Name'
        , 'Patient Last Name'
        , 'DOB'
        , 'Gender'
        , 'Residential Address'
        , 'City'
        , 'State'
        , 'Zip Code'
        , 'mpi'
        , 'Insurance ID'
        , 'CustomText1']]
    # targetPop['client_identifier'].dtype
    # Add blanks for those not available
    # targetPop = pd.concat([targetPop, pd.DataFrame(columns=list([
    #    'ins_member_id',
    #    'ssn',
    #    'client_identifier',
    #    'client_identifier_2']))])
    # Frame columns into the standard order
    targetPop = targetPop[['firstname'
        , 'lastname'
        , 'dob'
        , 'sex'
        , 'street1'
        , 'City'
        , 'State'
        , 'zip'
        , 'mpi'
        , 'ins_member_id'
        , 'client_identifier']]

    # targetPop['dob'] = targetPop['dob'].astype(str)
    targetPop['sex'] = targetPop['sex'].astype(str).str[0]
    targetPop['dob'] = pd.to_datetime(targetPop['dob'])
    targetPop['dob'] = pd.to_datetime(targetPop['dob'], format='%m/%d/%Y').dt.strftime("%Y-%m-%d")
    targetPop = targetPop.apply(lambda x: x.astype(str).str.lower())
    # End CSV Specific alterations

    connection = engine.connect()
    targetPop.to_sql(targetTable + '_py_to_sql', connection, if_exists='replace', chunksize=5000, index_label='id')
    connection.close()

    sql = "UPDATE `_dashboard_maintenance`.`cross_language_markers` SET `count`=`count` - 1 WHERE `job_name`='" + targetTable + "_TargetPopulation_python.py';"
    connection = engine.connect()
    result = connection.execute(sql)
    connection.close()
    ##dfMPIMasterUpdateListNoNull = pd.DataFrame(dfMPIMasterUpdateList['MPI'].dropna(axis=0, how='all'))
except:
    try:
        titleString = os.path.basename(sys.argv[0])
        errormsg = str(sys.exc_info())
        localtime = str(time.asctime(time.localtime(time.time())))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        lineNumber = str(sys.exc_info()[-1].tb_lineno)
    except:
        titleString= 'Manual - targetpop script'
        errormsg = ''
        localtime = ''
        exc_type = ''
        exc_obj = ''
        exc_tb = ''
        lineNumber = ''
    try:
        table_reference = ''
    except:
        table_reference = ''
        fname = 'No File Name Available'
        errormsgdetail = 'No Error Message Available'
    try:
        sql
    except:
        sql = ''
    try:
        sql = "UPDATE `_dashboard_maintenance`.`cross_language_markers` SET `count`=`count` + 1 WHERE `job_name`='" + targetTable + "_TargetPopulation_python.py';"
        connection = engine.connect()
        result = connection.execute(sql)
        connection.close()
        dbNotified = 'Yes'
    except:
        dbNotified = 'No'

    errorFileLocation = "\\\\prd-az1-sqlw2\\output\\Run_Errors.txt"


    # trace = traceback.print_tb(limit=2500)
    f = open(errorFileLocation, "a")
    f.write("Time: " + localtime + " | Error Details: " + str(errormsg) + " | Line: " + str(
        lineNumber) + " | File: " + titleString + " | Last SQL Query: " + sql + " | db (PHP read point script) Notified of Failure: " + dbNotified + " | Table Reference: " + table_reference + "\n")
    f.close()
