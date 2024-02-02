try:
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session
    from sqlalchemy.orm import sessionmaker
    from stat import S_ISDIR, S_ISREG
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, BigInteger, String, DateTime
    from sqlalchemy.exc import NoSuchTableError, ProgrammingError
    from datetime import datetime
    import time
    import paramiko
    import imp
    import pymssql
    import re
    import sys
    import py7zr
    import socket
    import datetime as dt
    import pandas as pd
    import subprocess
    import glob
    import os
    from os import path
    from multiprocessing.pool import ThreadPool
    import shutil as sh
    from datetime import datetime, timedelta
    import time
    import sys
    import imp
    Login = imp.load_source('Login', '//prd-az1-sqlw2.ad.konza.org/Batch/Login.py')

    folderNameClient = 'Inovalon Inc' #001Dn00000SASj1
    processingPath = '//PRD-AZ1-OPS2/L_216_Data_Curation_Zip_Send_processing/'
    inputPath = '//PRD-AZ1-OPS2/L_216_Data_Curation_Zip_Send_Input/'
    errorPath = '//PRD-AZ1-OPS2/L_216_Data_Curation_Zip_Send_Error/'
    counter = 0
    incrementFolder = 1

    pg_user = Login.pg_user_operations_logger
    pg_password = Login.pg_password_operations_logger
    pg_ops_server = Login.pg_ops_server
    pg_ops_user_db = Login.pg_ops_db_operations_logger

    from sqlalchemy import create_engine
    pgengine = create_engine(
        "postgresql+pg8000://" + pg_user + ":" + pg_password + "@" + pg_ops_server + "/" + pg_ops_user_db)

    connection = pgengine.connect()

    Query = connection.execute(
        """SELECT * FROM restart_trigger;""")
    dfupdateTriggerCheck = pd.DataFrame(Query.fetchall())
    connection.close()
    dfupdateTriggerCheck.columns = Query.keys()

    connection = pgengine.connect()
    Query = connection.execute(
        """SELECT * FROM schedule_jobs ; """)
    dfscheduleJobs = pd.DataFrame(Query.fetchall())
    connection.close()
    dfscheduleJobs.columns = Query.keys()

    scriptName = 'L_216_Data_Curation_Zip_Send_processing'
    dfscheduleJobs_prep = pd.DataFrame()
    dfscheduleJobs_prep = dfscheduleJobs.where(dfscheduleJobs['script_name'] == scriptName)
    dfscheduleJobs_prep = dfscheduleJobs_prep.dropna()

    dfscheduleJobs_prep['trigger_name'] = dfscheduleJobs_prep['server']
    dfscheduleJobs_prep = dfscheduleJobs_prep.merge(dfupdateTriggerCheck, left_on='trigger_name',
                                                    right_on='trigger_name', how='inner')

    if dfscheduleJobs_prep['switch'][0] == 0:
        print("running")

        zipOption = '1'
        sevenZexe = "C:\\7z1900-extra\\7za.exe"
        networkPaths = []
        dayOffset = 0  # Set to -13 for example to start 13 days ago
        connection = pgengine.connect()
        Query = connection.execute("""select * from l_216_Data_Curation_Zip_Send;""")
        dfAuthorized = pd.DataFrame(Query.fetchall())
        connection.close()
        if not dfAuthorized.empty:
            dfAuthorized.columns = Query.keys()





        Login = imp.load_source('Login', '//prd-az1-sqlw2.ad.konza.org/Batch/Login.py')
        # Login = imp.load_source('Login', '//prd-az1-sqlw2.ad.konza.org/Batch/Clients/Login.py') ##ddooley login



        def get_file_info(file_paths):
            folder_summary = {}

            for file_path in file_paths:
                if os.path.isfile(file_path):
                    folder_path = os.path.dirname(file_path)
                    file_size = os.path.getsize(file_path)

                    if folder_path not in folder_summary:
                        folder_summary[folder_path] = {'Size (Bytes)': 0, 'File Count': 0}

                    folder_summary[folder_path]['Size (Bytes)'] += file_size
                    folder_summary[folder_path]['File Count'] += 1

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            year = datetime.now().year

            folder_data = [{'Folder Path': path,
                            'Size (Bytes)': info['Size (Bytes)'],
                            'File Count': info['File Count'],
                            'Timestamp': timestamp,
                            'Year': year} for path, info in folder_summary.items()]

            return folder_data
        def mkdir_p(sftp, remote_directory):
            """Change to this directory, recursively making new folders if needed.
            Returns True if any folders were created."""
            if remote_directory == '/':
                # absolute path so change directory to root
                sftp.chdir('/')
                return
            if remote_directory == '':
                # top-level relative directory must exist
                return
            try:
                sftp.chdir(remote_directory)  # sub-directory exists
            except IOError:
                dirname, basename = os.path.split(remote_directory.rstrip('/'))
                mkdir_p(sftp, dirname)  # make parent directories
                sftp.mkdir(basename)  # sub-directory missing, so created it
                sftp.chdir(basename)
                return True

        qualityCheckDelivery = '0'
        pg_user = Login.pg_user_operations_logger
        pg_password = Login.pg_password_operations_logger
        pg_ops_server = Login.pg_ops_server
        pg_ops_user_db = Login.pg_ops_db_operations_logger
        pgengine = create_engine(
            "postgresql+pg8000://" + pg_user + ":" + pg_password + "@" + pg_ops_server + "/" + pg_ops_user_db)

        pgconnection = pgengine.connect()

        host = Login.host
        port = 3306
        user = Login.user
        passwd = Login.password
        db = Login.dbname
        db = 'temp'
        engine = create_engine(
            "mysql+pymysql://" + user + ":" + passwd + "@" + host + ":3306/" + db + "?charset=utf8&local_infile=1")
        pgconnection = pgengine.connect()
        overLapCheckQuery = pgconnection.execute("""select *
                from job_triggers where associated_table = 'l_216_Data_Curation_Zip_Send'
                ;""")
        dfoverLapCheckQuery = pd.DataFrame(overLapCheckQuery.fetchall())
        pgconnection.close()
        if not dfoverLapCheckQuery.empty:
            dfoverLapCheckQuery.columns = overLapCheckQuery.keys()

        if dfoverLapCheckQuery['trigger_status'][0] == '0' or (pd.Timestamp(dfoverLapCheckQuery['updated_at'][0]).tz_localize(None) <= pd.Timestamp(datetime.today() + timedelta(days=-1))):
            print("Running")
            if (pd.Timestamp(dfoverLapCheckQuery['updated_at'][0]).tz_localize(None) <= pd.Timestamp(
                datetime.today() + timedelta(days=-1))):
                localtime = str(time.asctime(time.localtime(time.time())))
                titleString = os.path.basename(sys.argv[0])
                errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
                # trace = traceback.print_tb(limit=2500)
                f = open(errorFileLocation, "a")
                f.write(
                    "Time: " + localtime + " | Error Details: l_216_Data_Curation_Zip_Send.py Job Stalled for over a day | File: BCBSKS_Verinovum__L_87 - " + titleString + " | Last HSQL Query: " + "\n")
                f.close()
                pgconnection = pgengine.connect()
                overLapOffQuery = pgconnection.execute("""update job_triggers set trigger_status = '0' where associated_table = 'l_216_Data_Curation_Zip_Send'
                                                                ;""")
                pgconnection.close()
                print("Updated table as state of not running")
            print("Running")
            pgconnection = pgengine.connect()
            overLapOnQuery = pgconnection.execute("""update job_triggers set trigger_status = '1' where associated_table = 'l_216_Data_Curation_Zip_Send'
                                                                            ;""")
            pgconnection.close()
            print("Updated table as state of running")

            def get_filepaths(directory):
                """
                This function will generate the file names in a directory
                tree by walking the tree either top-down or bottom-up. For each
                directory in the tree rooted at directory top (including top itself),
                it yields a 3-tuple (dirpath, dirnames, filenames).
                """
                file_paths = []  # List which will store all of the full filepaths.

                # Walk the tree.
                for root, directories, files in os.walk(directory):
                    for filename in files:
                        # Join the two strings in order to form the full filepath.
                        filepath = os.path.join(root, filename)
                        file_paths.append(filepath)  # Add it to the list.

                return file_paths  # Self-explanatory.


            def chunks(lst, n):
                """Yield successive n-sized chunks from lst."""
                for i in range(0, len(lst), n):
                    yield lst[i:i + n]

            def sftp_delivery(sourceFile):
                FolderName = folderNameClient
                #sourceFile = target
                #FolderName = clientFolderName
                pg_password_mgmt = Login.pg_mgmtops_password
                pg_server_mgmt = Login.pg_mgmtops_server
                pg_user_mgmt = Login.pg_mgmtops_user
                pgengine_mgmt = create_engine(
                    "postgresql+pg8000://" + pg_user_mgmt + ":" + pg_password_mgmt + "@" + pg_server_mgmt + "/postgres")
                # 'RE: Measure Definition: New ACO Diabetes Poor Control Measure  pg_server_mgmt-000415'
                try:
                    connection_mgmt = pgengine_mgmt.connect()
                except:
                    time.sleep(1800)  # wait 30 minutes and try again
                    connection_mgmt = pgengine_mgmt.connect()
                if qualityCheckDelivery == '1':
                    FolderName = 'KonzaTest'
                dbcreds = connection_mgmt.execute(
                    """select * from sftp_credentials_parent_mgmt where client_security_groupings_admins_name = '""" + FolderName + """'; """)
                dfdbcreds = pd.DataFrame(dbcreds.fetchall())
                connection_mgmt.close()

                if not dfdbcreds.empty:
                    dfdbcreds.columns = dbcreds.keys()
                try:
                    dbcred_sftp_server = dfdbcreds['sftp_server'][0]
                    dbcred_sftp_port = dfdbcreds['sftp_port'][0]
                    dbcred_sftp_user = dfdbcreds['sftp_user'][0]
                    dbcred_sftp_password = dfdbcreds['sftp_password'][0]
                    dbcred_quality_review_location = dfdbcreds['quality_review_location'][0]
                    dbcred_sftp_root_directory = dfdbcreds['sftp_root_directory'][0]
                except:
                    print("Authorization row has invalid credential reference")
                    assert False

                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                try:
                    for q in range(0, retryRange):
                        try:
                            ssh_client.connect(hostname=dbcred_sftp_server, port=dbcred_sftp_port,
                                               username=dbcred_sftp_user,
                                               password=dbcred_sftp_password)
                            ftp_client = ssh_client.open_sftp()
                            # time.sleep(1)
                        except BaseException as e:
                            print("Error - Retrying" + str(e))
                            time.sleep(60)
                            try:
                                ssh_client.connect(hostname=dbcred_sftp_server, port=dbcred_sftp_port,
                                                   username=dbcred_sftp_user,
                                                   password=dbcred_sftp_password)
                                ftp_client = ssh_client.open_sftp()
                            except BaseException as e:
                                print("Error - Could never connect" + str(e))
                                print("Error - Retrying")
                                time.sleep(60)
                                try:
                                    ssh_client.connect(hostname=dbcred_sftp_server, port=dbcred_sftp_port,
                                                       username=dbcred_sftp_user,
                                                       password=dbcred_sftp_password)
                                    ftp_client = ssh_client.open_sftp()
                                except BaseException as e:
                                    print("Error - Could never connect" + str(e))
                                    print("Error - Retrying")
                                    time.sleep(60)
                                    try:
                                        ssh_client.connect(hostname=dbcred_sftp_server, port=dbcred_sftp_port,
                                                           username=dbcred_sftp_user,
                                                           password=dbcred_sftp_password)
                                        ftp_client = ssh_client.open_sftp()
                                    except BaseException as e:
                                        print("Error - Could never connect" + str(e))
                                        print("Error - Retrying")
                                        time.sleep(7500)
                                        try:
                                            ssh_client.connect(hostname=dbcred_sftp_server, port=dbcred_sftp_port,
                                                               username=dbcred_sftp_user,
                                                               password=dbcred_sftp_password)
                                            ftp_client = ssh_client.open_sftp()
                                        except BaseException as e:
                                            print("Error - Could never connect - waiting prescribed 120 + 5 minutes")
                                            time.sleep(60)
                        break
                except:
                    print("ERTA - Check credentials and paramiko access, will not talk - stopping entire script!")
                    localtime = str(time.asctime(time.localtime(time.time())))
                    titleString = os.path.basename(sys.argv[0])
                    errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
                    # trace = traceback.print_tb(limit=2500)
                    f = open(errorFileLocation, "a")
                    f.write(
                        "Time: " + localtime + " | Error Details: L_216_Data_Curation_Zip_Send\py7zip_to_client.py Job has a failed SFTP Connection! | \n")
                    f.close()
                    assert False
                if qualityCheckDelivery != '1':
                    ftp_client.put(sourceFile,dbcred_sftp_root_directory + '/' + sourceFile.split('/')[-1])
                    #ftp_client.put(sourceFile,sourceFile.replace(dbcred_sftp_root_directory))
                    ftp_client.close()
                if qualityCheckDelivery == '1':
                    ssh_client.connect(hostname=dbcred_sftp_server, port=dbcred_sftp_port,
                                       username=dbcred_sftp_user,
                                       password=dbcred_sftp_password)
                    ftp_client_qa = ssh_client.open_sftp()
                    ftp_client_qa.put(sourceFile,dbcred_sftp_root_directory + '/' + sourceFile.split('/')[-1])
                    ftp_client_qa.close()


            #= processingPath + '/' + processingPath + '.7z'
            def zipSend(fileListIn):
                global incrementFolder
                try:
                    #fileListIn  = aList
                    #incrementFolder = 0
                    incrementFolderString = str(incrementFolder)
                    fHash = str(abs(hash(fileListIn[0])))
                    t = 0
                    s = 0
                    date_time = datetime.today()
                    batch = str(time.mktime(date_time.timetuple()))
                    source = processingPath + '/prep_source_' + batch + fHash + '/'  # + ParticipantClientName + '/' + today_date_marker + '/'
                    while path.exists(source):
                        print('File Exists in: ' + source + ' Incrementing')
                        batch2 = batch + "_s" + str(s)
                        source = processingPath + '/prep_source_' + batch2 + fHash + '/'
                        s = s + 1
                    if not os.path.exists(source):
                        os.makedirs(source)
                    file_info = get_file_info(fileListIn)
                    #file_info = get_file_info(testPaths)
                    #df = pd.DataFrame(file_info)


                    numberOfFilesNumeric = str(len(fileListIn))
                    for f in fileListIn:
                        #print(f)
                        sh.move(f, f.replace(inputPath, source))


                    target = processingPath + '/prep_target_' + batch + incrementFolderString + '/' + batch + fHash + '.7z'
                    targetDIR = processingPath + '/prep_target_' + batch + incrementFolderString + '/'
                    if not os.path.exists(targetDIR):
                        os.makedirs(targetDIR)
                    while path.exists(target):
                        print('File Exists in: ' + target + ' Incrementing Further')
                        batch2 = batch + "_t" + str(t)
                        target = processingPath + '/prep_target_' + batch2 + incrementFolderString + '/' + batch + fHash + '.7z'
                        targetDIR = processingPath + '/prep_target_' + batch + incrementFolderString + '/'
                        if not os.path.exists(targetDIR):
                            os.makedirs(targetDIR)
                        t = t + 1
                    files_to_archive = [f for f in os.listdir(source) if os.path.isfile(os.path.join(source,f))]
                    with py7zr.SevenZipFile(target, 'w') as archive:
                        for file in files_to_archive:
                            file_path = os.path.join(source, file)
                            archive.write(file_path, arcname=file)
                        #archive.writeall(source)

                    #target = processingPath + '/et_test.7z'
                    #source = r'//prd-az1-ops2/L_216_Data_Curation_Zip_Send_processing/prep_source_1696296607.03392372560411760610/*.*'
                    #cmdFull = sevenZexe + " a -t7z \"" + target + "\" \"" + source + "*.*\" -m0=PPMd"
                    #output = subprocess.run(cmdFull, capture_output=True)
                    zipStatus = 'Successful Processing '
                    #if output.returncode != 0:
                    #    zipStatus = str('Failed CLI Command!: ' + str(output.stdout))
                    ### SFTP SEND GOES HERE IF ALL LOOKS GOOD
                    #target = 'E:/to deliver/1703015084.02929008583277783518.7z'
                    #target = 'E:/to deliver/1703015893.03264690043816372739.7z'
                    #target = '//PRD-AZ1-OPS2/L_216_Data_Curation_Zip_Send_processing\prep_target_1703122357.01/1703122357.01027149439102851343.7z'

                    #sftp_delivery(target)
                    results = zipStatus + ' On this many files: ' + str(numberOfFilesNumeric) + ' From: ' + source + ' To: ' + target + ' with command: '
                    incrementFolder = incrementFolder + 1

                    # Login credentials and server address
                    user = Login.formuser_user
                    password = Login.formuser_password
                    serverAddress = Login.formuser_server

                    # Create an engine to the SQL server
                    mssqlEngine = create_engine(f"mssql+pymssql://{user}:{password}@{serverAddress}:1433/formoperations")
                    table_name = 'l_216_zipSend_metadata'
                    metadata = MetaData()

                    # Define the table structure
                    file_info_table = Table(table_name, metadata,
                                            Column('Folder Path', String(255), nullable=False),
                                            Column('Size (Bytes)', BigInteger, nullable=False),
                                            Column('File Count', Integer, nullable=False),
                                            Column('Timestamp', DateTime, nullable=False),
                                            Column('Year', Integer, nullable=False))

                    # Check if the table exists and create it if it does not
                    # Assuming the schema is 'dbo', modify if it's different
                    schema_name = 'dbo'

                    # Check if the table exists and create it if it does not
                    try:
                        file_info_table = Table(table_name, metadata, autoload_with=mssqlEngine, schema=schema_name)
                        print("Table exists.")
                    except NoSuchTableError:
                        print("Table does not exist. Creating table...")
                        metadata.create_all(mssqlEngine)
                        print("Table created.")

                    # Raw SQL for insertion using positional placeholders
                    insert_query = f"""
                    INSERT INTO {schema_name}.{table_name} ([Folder Path], [Size (Bytes)], [File Count], [Timestamp], [Year]) 
                    VALUES (%s, %s, %s, %s, %s)
                    """

                    # Insert the data using raw SQL
                    try:
                        with mssqlEngine.connect() as conn:
                            first_record = file_info[0]
                            conn.execute(insert_query, (
                            first_record['Folder Path'], first_record['Size (Bytes)'], first_record['File Count'],
                            first_record['Timestamp'], first_record['Year']))
                        print("Data inserted successfully.")
                    except ProgrammingError as e:
                        print(f"Error occurred: {e}")
                    return results
                except:
                    sh.copytree(source, source.replace(inputPath,errorPath))
                    localtime = str(time.asctime(time.localtime(time.time())))
                    titleString = os.path.basename(sys.argv[0])
                    errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
                    # trace = traceback.print_tb(limit=2500)
                    f = open(errorFileLocation, "a")
                    f.write(
                        "Time: " + localtime + " | Error Details: L_216_Data_Curation_Zip_Send\py7zip_to_client.py Job has a failed batch | Batch - " + source + " | \n")
                    f.close()



            #zipSend(aList) #len(aList)
            # if '1.2.840.114350.1.13.307.2.7.8.688883.134415692.xml' in list_of_files:
            #    print("OK")
            # res = [i for i in full_file_paths if '1.2.840.114350.1.13.307.2.7.8.688883.134415692.xml' in i]

            for r in range(100):
                print("On Loop: " + str(r))
                zipBatch = 0
                sourceDirectoryInitial = inputPath
                # Run the above function and store its results in a variable.
                #sourceDirectoryInitial = '//prd-az1-ops2/L_216_Data_Curation_Zip_Send_processing/prep_source_1696296607.03392372560411760610/'
                testPaths = get_filepaths(f'//PRD-AZ1-OPS2/L_216_Data_Curation_Zip_Send_processing/prep_source_1701216008.05671763533373080954')
                full_file_paths = get_filepaths(sourceDirectoryInitial)
                numberOfFilesNumeric = str(len(full_file_paths))
                start = time.process_time()
                chunkSize = 12500 #Contract states less than 13K is required, default = 12500
                retryRange = 5 #10 is default
                for q in range(0, retryRange):
                    try:
                        def handler(error):
                            print(f'Error: {error}', flush=True)
                            errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
                            # trace = traceback.print_tb(limit=2500)
                            f = open(errorFileLocation, "a")
                            f.write(
                                "Time: " + localtime + " | Error Details: L_216_Data_Curation_Zip_Send\py7zip_to_client.py Job has a thread def handler error | Batch - " + error + "  | \n")
                            f.close()

                        if __name__ == '__main__':
                            print('main')
                            pool = ThreadPool(20)  # Originally was 20, stable at 15 ,testing 30 (appeared to stall out)
                            # sourcePaths = [
                            #    '//prd-az1-ops2/CCD-IN-CloverDX-1/'
                            # ]
                            # results = pool.map(send_folder_multithread, sourcePaths)
                            for aList in chunks(full_file_paths, chunkSize):
                                len(aList)
                                counter = counter + chunkSize
                                res = [i for i in aList if
                                       '1.2.840.114350.1.13.307.2.7.8.688883.134415692.xml' in i]
                                if res:
                                    print("OK - on " + str(counter))
                                    break
                                prepList = []
                                fullList = prepList.append(aList)
                                # print(prepList)
                                # print(len(prepList))

                                # print("Sending List of: " + str(len(aList)) + " files")
                                # zipSend(aList)
                                results = pool.map_async(zipSend, prepList, error_callback=handler)
                                # results = pool.map(send_files_multithread(sourceFileList),a)
                                # send_files_multithread(a)
                                print(results)
                                print(str((counter / int(numberOfFilesNumeric)) * 100) + '% or ' + str(counter) + ' / ' + str(
                                    numberOfFilesNumeric))
                                incrementFolder = incrementFolder + 1
                            pool.close()
                            pool.join()
                        if __name__ != '__main__':
                            exit('L_216_Data_Curation_Zip_Send.py is not running on __main__ - this is required - exiting')
                        duration = time.process_time() - start
                        print("Finished " + str(numberOfFilesNumeric) + " in " + str(duration / 60) + " minutes")
                        break
                        # wrapup_list_of_files = glob.glob('//prd-az1-ops2/CCD-IN-CloverDX-1/**/*.xml', recursive=True)
                        # print("Remaining Files: " + str(len(wrapup_list_of_files)) )
                        # oldest_file = min(wrapup_list_of_files, key=os.path.getctime)
                        # send_files_multithread(wrapup_list_of_files)


                    except FileNotFoundError:
                        errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
                        # trace = traceback.print_tb(limit=2500)
                        f = open(errorFileLocation, "a")
                        f.write(
                            "Time: " + str(time.asctime(time.localtime(
                                time.time()))) + "Warning: FileNotFoundError on L_216_Data_Curation_Zip_Send\py7zip_to_client.py - - job_trigger l_216_Data_Curation_Zip_Send RETRYING AFTER 600 seconds\n")
                        f.close()
                        time.sleep(600)
                        continue
                time.sleep(10)
            print("Updated table to indicate the job has completed and a new round can being")
            pgconnection = pgengine.connect()
            overLapOffQuery = pgconnection.execute("""update job_triggers set trigger_status = '0' where associated_table = 'l_216_Data_Curation_Zip_Send'
                                                            ;""")
            pgconnection.close()
            ##Test changes - 20231226

except:
    try:
        titleString = os.path.basename(sys.argv[0])
    except:
        titleString = 'manual error - L_216_Data_Curation_Zip_Send - py7zip_to_client.py'
    try:
        errormsg = str(sys.exc_info())
    except:
        errormsg = ''
    try:
        localtime = str(time.asctime(time.localtime(time.time())))
    except:
        localtime = ''
    try:
        lineNumber = str(sys.exc_info()[-1].tb_lineno)
    except:
        lineNumber = ''

    print(errormsg)

    errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
    # trace = traceback.print_tb(limit=2500)
    errorString = str("Time: " + localtime + " | Line Number: " + lineNumber + " | File - L_87: " + titleString + " | Error: " + errormsg + "\n")

    try:
        print("Running Write to DB")
        pg_user = Login.pg_user_operations_logger
        pg_password = Login.pg_password_operations_logger
        pg_ops_server = Login.pg_ops_server
        pg_ops_user_db = Login.pg_ops_db_operations_logger
        pgengine = create_engine(
            "postgresql+pg8000://" + pg_user + ":" + pg_password + "@" + pg_ops_server + "/" + pg_ops_user_db)

        pgconnection = pgengine.connect()
        Query = pgconnection.execute(
            """insert into public.run_errors  (error_message) VALUES  ('""" + errorString + """')
                                                                                            ;""")
        pgconnection.close()
        print("Finished Write to DB")

    except:
        f = open(errorFileLocation, "a")
        f.write(
            errorString)
        f.close()


