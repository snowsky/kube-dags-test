try:
    from bs4 import BeautifulSoup
    import subprocess
    import sys
    import pandas as pd
    import glob
    import subprocess
    import os
    import shutil
    import time
    import pymssql
    import boto3
    import json
    import datetime as dt
    from datetime import datetime
    from sqlalchemy import create_engine
    import imp
    from time import gmtime, strftime

    priority = ''
    optionRemoveHTML = False
    Login = imp.load_source('Login', '//prd-az1-sqlw2.ad.konza.org/Batch/Login.py')
    pg_user = Login.pg_user_operations_logger_ops3
    pg_password = Login.pg_password_operations_logger_ops3
    pg_ops_server = Login.pg_ops_server_ops3
    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
    pgengine = create_engine(
        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")

    connection = pgengine.connect()
    Query = connection.execute(
        """SELECT * FROM restart_trigger;""")
    dfupdateTriggerCheck = pd.DataFrame(Query.fetchall())
    connection.close()
    dfupdateTriggerCheck.columns = Query.keys()
    specialCenturyKSText = ''

    def xstr(s):
        if s is None:
            return ''
        return str(s)
    connection = pgengine.connect()
    Query = connection.execute(
        """SELECT * FROM schedule_jobs ; """)
    dfscheduleJobs = pd.DataFrame(Query.fetchall())
    connection.close()
    dfscheduleJobs.columns = Query.keys()

    scriptName = 'Internal Ticketing'
    dfscheduleJobs_prep = pd.DataFrame()
    dfscheduleJobs_prep = dfscheduleJobs.where(dfscheduleJobs['script_name'] == scriptName)
    dfscheduleJobs_prep = dfscheduleJobs_prep.dropna()

    dfscheduleJobs_prep['trigger_name'] = dfscheduleJobs_prep['server']
    dfscheduleJobs_prep = dfscheduleJobs_prep.merge(dfupdateTriggerCheck, left_on='trigger_name',
                                                    right_on='trigger_name', how='inner')

    if dfscheduleJobs_prep['switch'][0] == 0:
        print("running")

        gidPrefix = 'SUP-'
        pgconnection = pgengine.connect()
        overLapCheckQuery = pgconnection.execute("""select *
                            from job_triggers where associated_table = 'card_addition_erta_com_konza_support_curl'
                            ;""")
        dfoverLapCheckQuery = pd.DataFrame(overLapCheckQuery.fetchall())
        pgconnection.close()
        if not dfoverLapCheckQuery.empty:
            dfoverLapCheckQuery.columns = overLapCheckQuery.keys()
        # 'RE: Measure Definition: New ACO Diabetes Poor Control Measure  PMT-000415'
        #if dfoverLapCheckQuery['trigger_status'][0] == '1':
        #    print("Overlap Query Activated - waiting 600 seconds")
        #    time.sleep(600)
#
        #    pgconnection = pgengine.connect()
        #    overLapCheckQuery = pgconnection.execute("""select *
        #                            from job_triggers where associated_table = 'card_addition_erta_com_konza_support_curl'
        #                            ;""")
        #    dfoverLapCheckQuery = pd.DataFrame(overLapCheckQuery.fetchall())
        #    pgconnection.close()
        #    if not dfoverLapCheckQuery.empty:
        #        dfoverLapCheckQuery.columns = overLapCheckQuery.keys()
        #    if dfoverLapCheckQuery['trigger_status'][0] == '1':
        #        print("Updated table to indicate the job has completed and a new round can being")
        #        pgconnection = pgengine.connect()
        #        overLapOffQuery = pgconnection.execute("""update job_triggers set trigger_status = '0' where associated_table = 'card_addition_erta_com_konza_support_curl'
        #                                                                        ;""")
        #        pgconnection.close()
        #        errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
        #        # trace = traceback.print_tb(limit=2500)
        #        f = open(errorFileLocation, "a")
        #        f.write(
        #            "Time: " + str(time.asctime(time.localtime(
        #                time.time()))) + "Warning: MAIN CARD GENERATION SCRIPT STILL MARKED RUNNING - Card Addition - ERTA COM - konza_support - curl.py - RETRYING AFTER 600 seconds\n")
        #        f.close()

        if dfoverLapCheckQuery['trigger_status'][0] == '0':
            print("Running")
            pgconnection = pgengine.connect()
            pgconnection.execute("""update job_triggers set trigger_status = '1' where associated_table = 'card_addition_erta_com_konza_support_curl'
                                                                    ;""")
            pgconnection.close()
            # authorid = sys.argv[2] # erta_robot = KJkuCf5wXdRjYHrcb
            # boardid = sys.argv[3]
            # swimlaneid = sys.argv[4]
            # listid = sys.argv[5]
            # cardtitle = sys.argv[6]
            # carddescription = sys.argv[7]

            # lastIDLoaded = '13'

            # user = 'ERTA_card_creation_reader@server.database.windows.net'
            # user = 'ERTA_card_creation_reader'
            # password = 'ef0eaa6a1a4ce7811ae9b690134f8d9c!'
            user = Login.formuser_user
            password = Login.formuser_password
            serverAddress = Login.formuser_server
            # serverAddress = 'prd-az1-opssql.database.windows.net:1433'

            try:
                conn = pymssql.connect(server=serverAddress, user=user, password=password, database='formoperations')
            except:
                time.sleep(30)
                conn = pymssql.connect(server=serverAddress, user=user, password=password, database='formoperations')
            sql = "select * from all_projects;"
            dfAllProjects = pd.read_sql(sql, conn)
            conn.close()


            try:
                conn = pymssql.connect(server=serverAddress, user=user, password=password, database='formoperations')
                time.sleep(30)
            except:
                conn = pymssql.connect(server=serverAddress, user=user, password=password, database='formoperations')
            sql = """
            select data_extract_description,escalation_option,project_ids,L_120__authorized_identifier,L_120__authorized_identifier_type,L_120__hl7v2_client_delivery_paused,L_120__authorization_party,L_120__id,L_102__authorized_identifier,L_120__hl7v2_client_name,L_68__id,L_68__authorization_reference,L_68__authorized_identifier_oid,L_68__authorized_filter_file_beginning_pattern,L_68__authorizing_party,L_68__delivery_paused,L_68__participant_client_name,L_68__source_base_path,L_68__destination_path_override,L_68__client_folder_name,audit_type,L_69__id, L_69__source_file_zipped, L_102__authorized_identifier_type,L_102__hl7v2_client_delivery_paused,L_102__authorization_party,L_102__id,L_102__authorized_identifier,L_102__hl7v2_client_name,L_87__client_folder_name,L_87__destination_path_override,L_87__source_base_path,L_87__participant_client_name,L_87__authorization_reference,L_87__delivery_paused,L_87__authorizing_party,L_87__authorized_filter_file_beginning_pattern,L_87__authorized_identifier_oid,L_87__id,id,behalf_of_email,client_legal_name,ticket_reason,ticket_reference_other_system,requestor_info, ehx__ID ,ehx__ssi_payor_client_name ,ehx__authorized_identifier ,ehx__authorization_party ,ehx__dh_fusion_delivery_paused ,ehx__authorized_identifer_type ,ehx__konza_client_destination_delivery_paused ,ehx__dh_raw_ehx_ccd_delivery_location ,L_10__id ,L_10__hl7v2_client_name ,L_10__authorized_identifier ,L_10__authorization_party ,L_10__authorized_identifier_type ,L_10__crawler_id_last_loaded, L_10__hl7v2_client_delivery_paused ,L_69__emr_client_name ,L_69__authorized_identifier ,L_69__authorization_party ,L_69__emr_client_delivery_paused ,L_69__authorized_identifier_type ,L_69__authorization_reference ,L_69__participant_client_name ,ticket_reason_extended ,attachment,ehx__ssi_prefix,ehx__auto_submit_panel_to_corepoint 
            from konza_support 
            where added_card_reference is null
            order by id desc;
            """

            # cursor = conn.cursor()
            # cursor.execute(sql)
            # data = cursor.fetchall()
            dfSupportTickets = pd.read_sql(sql, conn)
            conn.close()
            for row in dfSupportTickets.iterrows():
                # rowList = list(rowDB)
                # rowListReplaced = [str(item).replace("'", "") for item in rowList]
                # int_map = map(int,rowListReplaced)
                # list_map = list(int_map)
                # row = tuple(rowListReplaced)


                row[1]['L_68__authorization_reference']

                memberids = []
                id = row[1]['id']
                gid = gidPrefix + str(id)
                for a in row:
                    print(a)
                projectIds = []
                projectIds.append(str(row[1]['project_ids']))
                projectIds.append(str(row[1]['ticket_reference_other_system']))
                # Filter the list based on three different filters
                filter1 = [item for item in projectIds if item.startswith('L-')]
                filter2 = [item for item in projectIds if item.startswith('S-')]
                filter3 = [item for item in projectIds if item.startswith('C-')]
                # Create a string with filtered list items separated by commas
                fullList = filter1 + filter2 + filter3
                for f in fullList:
                    try:
                        priority = dfAllProjects[dfAllProjects['ProjectId'] == f]['Priority'].iloc[0]
                    except:
                        priority = '0'
                projectIdsMapped = ''
                d = dict.fromkeys(filter1 + filter2 + filter3)
                print("Here")
                projectIdsMapped = ','.join(d.keys())
                if row[1]['ticket_reason'] is None:
                    print('Ticket Reason is None - likely L-65 PowerApp Needs adjusted')
                    continue
                EscalationOption = xstr(row[1]['escalation_option']).strip()
                TicketReason = xstr(row[1]['ticket_reason']).strip()
                OnBehalfOf = xstr(row[1]['behalf_of_email']).strip()
                ClientLegalName = xstr(row[1]['client_legal_name']).strip()
                description = xstr(row[1]['data_extract_description']) +' Description: '+ xstr(row[1]['ticket_reason_extended'])
                requestorInfo = xstr(row[1]['requestor_info'])
                ExternalTicketRef = xstr(row[1]['ticket_reference_other_system'])
                attachment = xstr(row[1]['attachment'])
                descriptionAddition = ''
                if not len(str(row[1]['ticket_reference_other_system'])) > 1:
                    ExternalTicketRef = ''

                ## Best way to get these IDs is to export board to json and open in Pycharm
                # authorId = 'KJkuCf5wXdRjYHrcb'  # Bot is default #KJkuCf5wXdRjYHrcb
                # boardid = 'ks9JgJZX3Nw7rdunt'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                # swimlaneid = 'NBDpWDW6xYTyoojF4'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                # listid = 'BzJtfFaN3GQ4zr7w5'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                if TicketReason in ['Enhancements - HQ Intellect - Addition or Change to data structure, dashboard or automated report'
                                    ,'Issue - HQ Intellect - Client facing feature in non-production environment broken (provide proof)',
                                    'Infrastructure - HQ Intellect - Software Stack Change']:
                    wekanToUse = 'wekan_boardskonzaorg'
                    wekanTestToUse = 'wekan_test_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'EoDvXoJDqRLQLzsDP'
                    listid = 'AaZCmJcnmHmfdKTPD'
                    swimlaneid = 'ZhzN56MzDjQFzASFx'
                if TicketReason in ['Infrastructure - Report Writer - Software Stack Change',
                                    'Push - Report Writer - Development Push Operations from Environment to Environment',
                                    'Enhancements - Other Product Support - Addition or Change to data structure, dashboard or automated report']:
                    wekanToUse = 'wekan_boardskonzaorg'
                    wekanTestToUse = 'wekan_test_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = '4wsbMowhKhHmeSGhi'
                    listid = '9Lca5gLBPFuZJFwZ5'
                    swimlaneid = 'MxtWxrBMHRwCjGDD4'

                if TicketReason in [
                    'Investigate - Determine reason and possible options for an identified view on a Non-Clinical Dashboard Project',
                    #'Enhancements - Non-Dimensional Insight - Addition or Change to data structure, dashboard or automated report',
                    'Alerts - Delivered - SacValley HL7v2 Raw (Routed Raw Messages) - L-102',
                    'Alerts - Delivered - Corporate HL7v2 Raw (Routed Raw Messages) - L-120',
                    #??not sure where this came from 'Alerts Delivery - Metric - Real Time Delivery - Delivery of aggregated alerts to be enabled for SFTP final delivery. Delivered through Dimensional Insight application.-120',
                    'Alerts - Delivered - KONZA HL7v2 Raw (Routed Raw Messages) - L-52, L-10, L-82',
                    'CDA Retrieval from KONZA Hosted SFTP - L-69',
                    'Data Extract - Predefined data formatting file generation and delivery',
                    'DevOps Estimate - Estimate of resources/cost and timeline',
                    'Infrastructure - Upstream Partner Notification - Data Flow Change',
                    'Infrastructure - Source formatting or data structure changes', 'Routing of CCD to Destination - L-68']:
                    ## KONZA - Data Work - All Non-DI Impacting Work
                    wekanToUse = 'wekan_boardskonzaorg'
                    wekanTestToUse = 'wekan_test_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'wmC23eXvm8yT68Pfw'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    swimlaneid = '5HPLKr7XJYQsCeQ9K'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listid = 'L9vq2gHE4biL9d8kW'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listIdToDo = 'iBBPEESFXT32Dwvoq'  # To Do
                    # Member ID: mZcXKPorDi5KYwjRT
                    # Username: AAgarwal
                    # memberids.append('mZcXKPorDi5KYwjRT')  # adding default member to watch for cards
                if TicketReason in ['Reference Database - Submit manual changes to the references tables used by the business intelligence and analytics team.',
                                    'Population Definition - Security group definition (Patient Panel CSV, No CSV - KONZA ID or Facility Identifier, Zip code based, other method to clearly define patients of interest)',
                                    'Facility Update - Updates to source identifier facility Names, Clinic/Non-Clinic Status, manual entry columns, and reliable distinct MRN data source updates to the facility_info or facility information tracking table',
                                    'Population Definition - Schedule A - CSV', 'Population Definition - Schedule B - CSV', 'Population Definition - Schedule C - CSV'
                                    ]:
                    wekanToUse = 'wekan_boardskonzaorg'
                    ## KONZA - MySQL Database & Client Folder Edits & User and Group Management
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'wmC23eXvm8yT68Pfw'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    swimlaneid = '5HPLKr7XJYQsCeQ9K'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listid = 'L9vq2gHE4biL9d8kW'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    # Member ID: YGczYTiE3wgNoRbxM
                    # Username: cclark@konza.org
                    # Member ID: mZcXKPorDi5KYwjRT
                    # Username: AAgarwal
                    # Member ID: zZHiY68PonopSZpSS
                    # Username: ddooley
                    #memberids.append('mZcXKPorDi5KYwjRT')  # adding default member to watch for cards
                if TicketReason in ['Population Definition - Security group definition (Patient Panel CSV, No CSV - KONZA ID or Facility Identifier, Zip code based, other method to clearly define patients of interest)',
                                    'Population Definition - Schedule A - CSV', 'Population Definition - Schedule B - CSV'
                                    ]:
                    wekanToUse = 'wekan_boardskonzaorg'
                    ## KONZA - MySQL Database & Client Folder Edits & User and Group Management
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'wmC23eXvm8yT68Pfw'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    swimlaneid = '5HPLKr7XJYQsCeQ9K'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listid = 'L9vq2gHE4biL9d8kW'  # General Ticket Board is default #                ctuLBykuGmgt55iPQ

                if TicketReason in ['AcuteAlerts Delivery (Non-SFTP)',
                                    'Alerts Delivery - Metric - Real Time Delivery - Delivery of aggregated alerts to be enabled for  SFTP final delivery. Delivered through Dimensional Insight application.',
                                    #'Enhancements - Dimensional Insight - Addition to data structure, dashboard or automated report',
                                    'Enhancements - HQ Insights - Addition to data structure, dashboard or automated report',
                                    'Investigate - Determine reason and possible options for an identified view on the Clinical Dashboards',
                                    'Limit access - Hide dashboard tiles from specific user(s) or security group(s)']:
                    ## Clinical Dashboard - Data Work - KONZA 4
                    wekanToUse = 'wekan_boardskonzaorg'
                    wekanTestToUse = 'wekan_test_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'zkBvpNGba4opyCrtM'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    swimlaneid = 'MHugnzB3ezMkc7yvK'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listid = 'Hru5Gy8P3kWsxevDa'#'hCdyxk2wL6p8JDEjT'

                if TicketReason in ['Measure definition - Codes and parameters for a metric']:
                    ## Clinical Dashboard - Data Work - KONZA 4
                    wekanToUse = 'wekan_boardskonzaorg'
                    wekanTestToUse = 'wekan_test_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'zkBvpNGba4opyCrtM'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    swimlaneid = 'MHugnzB3ezMkc7yvK'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listid = 'hCdyxk2wL6p8JDEjT'# General Ticket Board is default #ctuLBykuGmgt55iPQ
                    descriptionAddition = ' Developer Estimate Dependencies (2 other card types): Per L-139, Make sure data is available in an extract and verify Extract card associated with the Reference Database for this measure is in Completed state and review extracted data prior providing an estimate'

                if TicketReason in ['Push - Quality Team - Quality Assurance to Production',
                                    'Maintenance - Dimensional Insight Production - One-time',
                                    'Maintenance - Dimensional Insight Production - Recurring']:
                    wekanToUse = 'wekan_boardskonzaorg'
                    wekanTestToUse = 'wekan_test_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'zkBvpNGba4opyCrtM'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    swimlaneid = 'MHugnzB3ezMkc7yvK'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listid = 'Nq5o3nHHCoYfaRjEE'  # General Ticket Board is default #ctuLBykuGmgt55iPQ

                if TicketReason in ['Push - Bi-Weekly Sprints - Development Push Operations from Environment to Environment']:
                    ## Clinical Dashboard - Data Work - KONZA 4
                    wekanToUse = 'wekan_boardskonzaorg'
                    wekanTestToUse = 'wekan_test_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'zkBvpNGba4opyCrtM'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    swimlaneid = 'MHugnzB3ezMkc7yvK'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listid = 'Nq5o3nHHCoYfaRjEE'  # General Ticket Board is default #ctuLBykuGmgt55iPQ
                    listIdToDo = 'zCpgDFNBFbrfqjduN'  # To Do

                if TicketReason in ['Risk Assessment','Internal Audit',
                                    'Issue - Security Project Related Item', ##Fields to include MPI (integer and only entry at a time), exclude_reason (should include the reference/card/project)
                                    'Certificate Management - All Sites Secured with Verified Certificates',
                                    'External Vendor Security Audit Response', 's3 bucket or Azure Storage Account']:
                    ## KONZA Encryption, Security, and Risk Assessments
                    wekanToUse = 'wekan_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'EYhgN5mAsvyFhkxsY'#'eAzLCnYGW2eCgnmRr'  # konza-encryption-security-and-risk-assessments
                    listid = '96eCtn8uXu6Kbbc8c'
                    swimlaneid = 'qSAF8gJw8Xwapvqb4'#'aGhtTiin569i5ekrZ'
                    # Member ID: 8NHbtLwxiZ3vvNisp
                    # Username: jmontgomery@konza.org
                    #memberids.append('8NHbtLwxiZ3vvNisp')  # adding default member to watch for cards

                if TicketReason in ['Issue - VPN or Network Connectivity Errors',
                                    'Network Change Request']:
                    optionRemoveHTML = True
                    specialCenturyKSText = 'Network Change Request'
                    wekanToUse = 'wekan_boardskonzaorg'
                    ## Network Connections
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'hC87vvkuMDXwJAxDp'  #
                    listid = '7jCBn4nsrvNboJ9Zt'
                    swimlaneid = 'zYXy8PY9xkvq8qYr5'
                    # Member ID: 8NHbtLwxiZ3vvNisp
                    # Username: jmontgomery@konza.org
                    #memberids.append('8NHbtLwxiZ3vvNisp')  # adding default member to watch for cards
                    descriptionAddition = f'``` {description} ```'
                if TicketReason in ['Information Request - Entire Organization']:
                    ## KONZA - Decision Support
                    wekanToUse = 'wekan_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'sGyzAxaDDni7wbNF7'
                    listid = 'endRNYGKthW5AjPkF'
                    swimlaneid = 'ypoPDmZbfFKFkSv7d'
                    # memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                    # Member ID: ZgaFSnZS8Do6mZdMb
                    # Username: alandstrom@konza.org
                    # memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                    memberids.append('yNhShJdK8wxSYK3C2') #Aishwarya: yNhShJdK8wxSYK3C2 (8/19/2024) based on SLA-22
                if TicketReason in ['Item not listed Above','Issue - Internal Workstation Support (CenturyKS Logging)', 'Issue - Internal facing feature broken (provide proof)',
                                    'Issue - Client facing feature in non-production environment broken (provide proof)',
                                    'Extract Estimate -  Estimate of resources/cost and timeline',
                                    'Licensing Change Request - BIA KONZA Managed Licenses Only',
                                    'Equipment - New',
                                    'Client Projection - Estimated Volume Increase Notification',
                                    'Equipment - Workstation Change','Equipment - Replacement', 'External Access Change Request - Non-Employee',
                                    'Internal Access Change Request',
                                    'Infrastructure - Rhapsody FHIR - Software Stack Change',
                                    'Infrastructure as a Service - Azure - VM Support',
                                    'Investigate - System Failure - Five Whys or Fishbone Diagram',
                                    'Issue - Patient Privacy - Hide Records from Display',
                                    'New Hire Equipment - New Laptop Needed - Fully ready 1 week in advance',
                                    'New Hire Equipment - No New Laptop - Fully ready 1 week in advance']:
                    ## KONZA - Help Desk
                    wekanToUse = 'wekan_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'iPZ5T3uRQoRqqXrM3' #'ZtFXLZ2pjP6f3MzKW'
                    listid = 'PKzpTmE8StJnkSeeA'
                    swimlaneid = '3oJWqY8PwQL5E4ywX'
                    #memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                    # Member ID: ZgaFSnZS8Do6mZdMb
                    # Username: alandstrom@konza.org
                    # memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                    memberids.append('sBn96DEKTDZ85un2D') #Colton = sBn96DEKTDZ85un2D 8/19/2024 - SLA-38
                if TicketReason in ['Issue - Internal Workstation Support (CenturyKS Logging)']:
                    ## KONZA - Help Desk
                    wekanToUse = 'wekan_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'iPZ5T3uRQoRqqXrM3'  # 'ZtFXLZ2pjP6f3MzKW'
                    listid = 'PKzpTmE8StJnkSeeA'
                    swimlaneid = '3oJWqY8PwQL5E4ywX'
                    #memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                    # Member ID: ZgaFSnZS8Do6mZdMb
                    # Username: alandstrom@konza.org
                    # memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                    #memberids.append('YGczYTiE3wgNoRbxM')  # adding default member to watch for cards
                if TicketReason in ['None']: #Just a placeholder, there are no Nones
                    wekanToUse = 'wekan_boardskonzaorg'
                    authorId = 'KwMkTND63bbwYCqew'  # Bot is default #KJkuCf5wXdRjYHrcb
                    boardid = 'iPZ5T3uRQoRqqXrM3'  # 'ZtFXLZ2pjP6f3MzKW'
                    listid = 'PKzpTmE8StJnkSeeA'
                    swimlaneid = '3oJWqY8PwQL5E4ywX'
                    # memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards

                clientOrParticiantName = str(row[1]['ehx__ssi_payor_client_name']).strip() + '|' + str(row[1]['L_10__hl7v2_client_name']).strip() + '|' + str(row[1]['L_69__emr_client_name']).strip() + '|' + str(row[1]['L_69__participant_client_name']).strip() + '|' + str(row[1]['L_68__participant_client_name']).strip() + '|' + str(row[1]['L_68__client_folder_name']).strip() + '|' + str(row[1]['client_legal_name']).strip() + '|' + str(row[1]['L_87__participant_client_name']).strip() + '|' + str(row[1]['L_87__client_folder_name']).strip() + '|' + str(row[1]['L_102__hl7v2_client_name']).strip()
                print("Get Custom Fields General Wekan API Start")
                GetCustomFields = subprocess.run(
                    ["python37", "//prd-az1-sqlw2/batch/py Operations/" + wekanToUse + ".py", "get_all_custom_fields",
                     boardid]
                    , capture_output=True)
                print("Get Custom Fields General Wekan API End")
                if 'dropdown' in str(GetCustomFields):
                    dropdownCustomFieldID = \
                    str(GetCustomFields).split('dropdown')[0].split('{"_id":"')[-1].split('","name":')[0]
                    print("Get Custom Dropdown Wekan API Start")
                    GetCustomFieldDropdown = subprocess.run(
                        ["python37", "//prd-az1-sqlw2/batch/py Operations/" + wekanToUse + ".py", "get_custom_field",
                         boardid, dropdownCustomFieldID]
                        , capture_output=True)
                    while '<title>502 Bad Gateway</title>' in str(GetCustomFieldDropdown):
                        print("Bad Gateway, trying again")
                        GetCustomFieldDropdown = subprocess.run(
                            ["python37", "//prd-az1-sqlw2/batch/py Operations/" + wekanToUse + ".py",
                             "get_custom_field",
                             boardid, dropdownCustomFieldID]
                            , capture_output=True)
                        time.sleep(1)
                    print("Get Custom Dropdown Wekan API End")
                iterations = len(str(GetCustomFields).split('{"_id":"'))
                dfCustomFields = pd.DataFrame(columns=['fieldID', 'fieldName'])
                for i in range(1, iterations):
                    print(i)
                    fieldID = str(GetCustomFields).split('{"_id":"')[i].split('","name":"')[0]
                    fieldName = str(GetCustomFields).split('{"_id":"')[i].split('","name":"')[1].split('","type":"')[0]
                    # if fieldName in dfCustomFields['fieldName'].values:
                    #    print("Already Found")
                    #    continue

                    print(f"Adding Custom Field {fieldName}")
                    dfCustomFields = dfCustomFields.append({
                        'fieldID': fieldID
                        , 'fieldName': fieldName}, ignore_index=True)
                import math

                if ' - ' not in EscalationOption:
                    EscalationOption = 'null'
                    customRoundedAverageImpactRange = -1
                if ' - ' in EscalationOption:
                    customRangeFirst = int(EscalationOption.split(' - ')[1].split(' (')[0].split('-')[0])
                    customRangeSecond = int(EscalationOption.split(' - ')[1].split(' (')[0].split('-')[1])
                    averageImpactRange = (customRangeFirst + customRangeSecond) /2

                    customPriorityFieldID = dfCustomFields[dfCustomFields['fieldName'] == 'Priority'].reset_index()['fieldID'][0]
                    customMatrixFieldID = dfCustomFields[dfCustomFields['fieldName'] == 'Priority Matrix Number'].reset_index()['fieldID'][0]
                    customPriority = EscalationOption.split(' - ')[0]
                    if '["' in EscalationOption:
                        customPriority = EscalationOption.split(' - ')[0].split('["')[1]
                    customRoundedAverageImpactRange = str(math.ceil(averageImpactRange))
                    customRoundedAverageImpactRange = EscalationOption
                    #
                    #
                    #customFieldsDistinct = []
                    #for cf in dfCustomFields.values.tolist():
                    #    customFieldsDistinct.append(cf)
                    #customFieldsString = joined_string = ",".join(customFieldsDistinct)
                    #if not customFieldsString:
                    #    customFieldsString = ''
                cardDescription = description + descriptionAddition #SUP-14087
                if len(descriptionAddition) > 5:
                    cardDescription = descriptionAddition

                cardTitle = str('GID:' + gid + ' ') + str(TicketReason.strip()) + 'Client/Participant Name: ' + clientOrParticiantName
                if len(str(ClientLegalName.strip())) > 1:
                    cardTitle = cardTitle + ' - ' + str(ClientLegalName.strip())
                if len(str(ExternalTicketRef.strip())) > 1:
                    cardTitle = cardTitle + ' - ' + str(ExternalTicketRef.strip())
                if len(str(cardDescription.strip())) > 1:
                    cardTitle = cardTitle + ' - ' + str(cardDescription.strip())[:100] + '...'
                cardTitle = cardTitle + ' - Requestor: ' + str(requestorInfo[:50].strip())
                if len(str(OnBehalfOf.strip())) > 1:
                    cardTitle = cardTitle + " - On Behalf of: " + str(OnBehalfOf.strip())

                if len(str(attachment)) > 1:
                    # cardDescription = description + " | Link: https://konzainc-my.sharepoint.com/personal/form_automation_konza_org/Documents/Apps/Microsoft%20Forms/Untitled%20form/Question%201/" + str(attachment).split('name":"')[1].split('","link')[0].replace(' ','%20') + " | <br><br><br><br><br> Full Attachment Reference: " + str(attachment)# - can tie the attachment 'name' variable to the link eg. Production%20Failure_no%20new%20messages_Tami%20Lamond%201.docx added to https://konzainc-my.sharepoint.com/personal/form_automation_konza_org/Documents/Apps/Microsoft%20Forms/Untitled%20form/Question%201/
                    cardDescription = description + " | Link: https://konzainc-my.sharepoint.com/personal/form_automation_konza_org/Documents/Apps/Microsoft%20Forms/KONZA%20Support/Question/" + \
                                      str(attachment).split('name":"')[1].split('","link')[0].replace(' ',
                                                                                                      '%20') + " | <br><br><br><br><br> Full Attachment Reference: " + str(
                        attachment)  # - can tie the attachment 'name' variable to the link eg. Production%20Failure_no%20new%20messages_Tami%20Lamond%201.docx added to https://konzainc-my.sharepoint.com/personal/form_automation_konza_org/Documents/Apps/Microsoft%20Forms/Untitled%20form/Question%201/
                # cardDescription = "TEST"
                if (TicketReason == 'Reference Database - Submit manual changes to the references tables used by the business intelligence and analytics team.') & (len(ClientLegalName) > 3):
                    print("test")
                    ## Update the PRD-AZ1-SQLW2 DB clients_to_process and client_hierarchy table
                if ((TicketReason == 'Alerts - Delivered - SacValley HL7v2 Raw (Routed Raw Messages) - L-102')
                    & (requestorInfo == 'swarnock@konza.org')) or \
                        ((TicketReason == 'Alerts - Delivered - SacValley HL7v2 Raw (Routed Raw Messages) - L-102')
                         & (requestorInfo == 'tlamond@konza.org')) or \
                        ((TicketReason == 'Alerts - Delivered - SacValley HL7v2 Raw (Routed Raw Messages) - L-102')
                         & (requestorInfo == 'slewis@konza.org')):
                    l102authorizedIdentifier = row[1]['L_102__authorized_identifier']
                    if isinstance(row[1]['L_102__authorized_identifier'], pd.Series):
                        l102authorizedIdentifier = ''
                    #isinstance(row[1]['ticket_reason'], pd.Series)
                    pg_user = Login.pg_user_operations_logger_ops3
                    pg_password = Login.pg_password_operations_logger_ops3
                    pg_ops_server = Login.pg_ops_server_ops3
                    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
                    pgengine = create_engine(
                        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")
                    # print(row[1]['ehx__ID'])
                    # print(row[1]['ehx__ssi_payor_client_name'])
                    # print(row[1]['ehx__authorized_identifier'])
                    # print(row[1]['ehx__authorization_party'])
                    # print(row[1]['ehx__dh_fusion_delivery_paused'])
                    # print(row[1]['ehx__authorized_identifer_type'])
                    # print(row[1]['ehx__konza_client_destination_delivery_paused'])
                    # print(row[1]['ehx__dh_raw_ehx_ccd_delivery_location'])
                    sql = """INSERT INTO public.raw_hl7v2_feed__l_102 (authorized_identifier,
                                     hl7v2_client_name,
                                      id,
                                      authorization_party,
                                      authorized_identifier_type,
                                      hl7v2_client_delivery_paused)
                        VALUES ('""" + l102authorizedIdentifier + """',
                         '""" + row[1]['L_102__hl7v2_client_name'] + """',
                          '""" + row[1]['L_102__id'] + """',
                           '""" + row[1]['L_102__authorization_party'] + """',
                             '""" + row[1]['L_102__authorized_identifier_type'] + """',
                              '""" + row[1]['L_102__hl7v2_client_delivery_paused'] + """')
    
                               ON CONFLICT (id)
                               DO UPDATE SET 
                                      authorized_identifier ='""" + l102authorizedIdentifier + """',
                                      hl7v2_client_name = '""" + row[1]['L_102__hl7v2_client_name'] + """',
                                      authorization_party = '""" + row[1]['L_102__authorization_party'] + """',
                                      authorized_identifier_type = '""" + row[1]['L_102__authorized_identifier_type'] + """',
                                      hl7v2_client_delivery_paused = '""" + row[1][
                        'L_102__hl7v2_client_delivery_paused'] + """'
                               ;"""

                    pgconnection = pgengine.connect()
                    updateRow = pgconnection.execute(sql)
                    pgconnection.close()
                    print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                    newCardID = 'Skipped as automated'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "', project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()
                    continue  # skips the wekan add since the job is done and no work needs assigned
                if ((TicketReason == 'Alerts - Delivered - Corporate HL7v2 Raw (Routed Raw Messages) - L-120')
                        & (requestorInfo == 'tlamond@konza.org')) or \
                        ((TicketReason == 'Alerts - Delivered - Corporate HL7v2 Raw (Routed Raw Messages) - L-120')
                         & (requestorInfo == 'slewis@konza.org')):
                    pg_user = Login.pg_user_operations_logger_ops3
                    pg_password = Login.pg_password_operations_logger_ops3
                    pg_ops_server = Login.pg_ops_server_ops3
                    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
                    pgengine = create_engine(
                        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")

                    sql = """INSERT INTO public.raw_hl7v2_feed__l_120 (authorized_identifier,
                                     hl7v2_client_name,
                                      id,
                                      authorization_party,
                                      authorized_identifier_type,
                                      hl7v2_client_delivery_paused)
                        VALUES ('""" + row[1]['L_120__authorized_identifier'] + """',
                         '""" + row[1]['L_120__hl7v2_client_name'] + """',
                          '""" + row[1]['L_120__id'] + """',
                           '""" + row[1]['L_120__authorization_party'] + """',
                             '""" + row[1]['L_120__authorized_identifier_type'] + """',
                              '""" + row[1]['L_120__hl7v2_client_delivery_paused'] + """')

                               ON CONFLICT (id)
                               DO UPDATE SET 
                                      authorized_identifier ='""" + row[1]['L_120__authorized_identifier'] + """',
                                      hl7v2_client_name = '""" + row[1]['L_120__hl7v2_client_name'] + """',
                                      authorization_party = '""" + row[1]['L_120__authorization_party'] + """',
                                      authorized_identifier_type = '""" + row[1]['L_120__authorized_identifier_type'] + """',
                                      hl7v2_client_delivery_paused = '""" + row[1][
                        'L_120__hl7v2_client_delivery_paused'] + """'
                               ;"""

                    pgconnection = pgengine.connect()
                    updateRow = pgconnection.execute(sql)
                    pgconnection.close()
                    print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                    newCardID = 'Skipped as automated'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "', project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()
                    continue  # skips the wekan add since the job is done and no work needs assigned

                if ((TicketReason == 'Routing of CCD to Destination - L-68')
                        & (requestorInfo == 'swarnock@konza.org')):
                    sql = """INSERT INTO public.archive_delivery__l_68 (id,
                                                         authorized_identifier_oid,
                                                          authorized_filter_file_beginning_pattern,
                                                          authorizing_party,
                                                          delivery_paused,
                                                          authorization_reference,
                                                          participant_client_name,
                                                          source_base_path,
                                                          destination_path_override,
                                                          client_folder_name
                                                          )
                                            VALUES ('""" + row[1]['L_68__id'] + """',
                                             '""" + row[1]['L_68__authorized_identifier_oid'] + """',
                                              '""" + row[1]['L_68__authorized_filter_file_beginning_pattern'] + """',
                                               '""" + row[1]['L_68__authorizing_party'] + """',
                                                 '""" + row[1]['L_68__delivery_paused'] + """',
                                                 '""" + row[1]['L_68__authorization_reference'] + """',
                                                 '""" + row[1]['L_68__participant_client_name'] + """',
                                                 '""" + row[1]['L_68__source_base_path'] + """',
                                                 '""" + row[1]['L_68__destination_path_override'] + """',
                                                  '""" + row[1]['L_68__client_folder_name'] + """')

                                                   ON CONFLICT (id)
                                                   DO UPDATE SET 
                                                          authorized_identifier_oid ='""" + row[1][
                        'L_68__authorized_identifier_oid'] + """',
                                                          authorized_filter_file_beginning_pattern ='""" + row[1][
                        'L_68__authorized_filter_file_beginning_pattern'] + """',
                                                          authorizing_party ='""" + row[1][
                        'L_68__authorizing_party'] + """',
                                                          delivery_paused ='""" + row[1][
                        'L_68__delivery_paused'] + """',
                                                          authorization_reference ='""" + row[1][
                        'L_68__authorization_reference'] + """',
                                                          participant_client_name ='""" + row[1][
                        'L_68__participant_client_name'] + """',
                                                          source_base_path ='""" + row[1][
                        'L_68__source_base_path'] + """',
                                                          destination_path_override ='""" + row[1][
                        'L_68__destination_path_override'] + """',
                                                          client_folder_name = '""" + row[1][
                              'L_68__client_folder_name'] + """'
                                                   ;"""

                    pgconnection = pgengine.connect()
                    updateRow = pgconnection.execute(sql)
                    pgconnection.close()
                    print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                    newCardID = 'Skipped as automated'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "', project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()
                    continue

                if ((TicketReason == 'CDA Retrieval from KONZA Hosted SFTP - L-69')
                        & (requestorInfo == 'swarnock@konza.org'))\
                        or ((TicketReason == 'CDA Retrieval from KONZA Hosted SFTP - L-69') & (requestorInfo == 'tlamond@konza.org')) \
                        or ((TicketReason == 'CDA Retrieval from KONZA Hosted SFTP - L-69') & (
                        requestorInfo == 'jwerner@konza.org')) \
                        or ((TicketReason == 'CDA Retrieval from KONZA Hosted SFTP - L-69') & (
                        requestorInfo == 'mlittlejohn@konza.org')) \
                        :
                    pg_user = Login.pg_user_operations_logger_ops3
                    pg_password = Login.pg_password_operations_logger_ops3
                    pg_ops_server = Login.pg_ops_server_ops3
                    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
                    pgengine = create_engine(
                        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")
                    if (len(row[1]['L_69__emr_client_name']) > 2 and
                            len(row[1]['L_69__authorized_identifier']) > 2 and
                            len(row[1]['L_69__authorization_party']) > 2 and
                            len(row[1]['L_69__authorized_identifier_type']) > 2 and
                            len(row[1]['L_69__authorization_reference']) > 2 and
                            len(row[1]['L_69__participant_client_name']) > 2):
                        sql = """INSERT INTO public.cda_konza_sftp_retrieval__l_69 (id,
                                         emr_client_name,
                                          authorized_identifier,
                                          authorization_party,
                                          emr_client_delivery_paused,
                                          authorized_identifier_type,
                                          authorization_reference,
                                          participant_client_name,
                                          source_files_zipped)
                            VALUES ('""" + row[1]['L_69__id'] + """',
                             '""" + row[1]['L_69__emr_client_name'] + """',
                              '""" + row[1]['L_69__authorized_identifier'] + """',
                               '""" + row[1]['L_69__authorization_party'] + """',
                               '""" + row[1]['L_69__emr_client_delivery_paused'] + """',
                               '""" + row[1]['L_69__authorized_identifier_type'] + """',
                                 '""" + row[1]['L_69__authorization_reference'] + """',
                                 '""" + row[1]['L_69__participant_client_name'] + """',
                                  '""" + row[1]['L_69__source_file_zipped'] + """')
        
                                   ON CONFLICT (id)
                                   DO UPDATE SET 
                                          emr_client_name ='""" + row[1]['L_69__emr_client_name'] + """',
                                          authorized_identifier ='""" + row[1]['L_69__authorized_identifier'] + """',
                                          authorization_party ='""" + row[1]['L_69__authorization_party'] + """',
                                          emr_client_delivery_paused ='""" + row[1]['L_69__emr_client_delivery_paused'] + """',
                                          authorized_identifier_type ='""" + row[1]['L_69__authorized_identifier_type'] + """',
                                          authorization_reference = '""" + row[1]['L_69__authorization_reference'] + """',
                                          participant_client_name = '""" + row[1]['L_69__participant_client_name'] + """',
                                          source_files_zipped = '""" + row[1]['L_69__source_file_zipped'] + """'
                                   ;"""

                    if (len(row[1]['L_69__emr_client_name']) < 2 and
                            len(row[1]['L_69__authorized_identifier']) < 2 and
                            len(row[1]['L_69__authorization_party']) < 2 and
                            len(row[1]['L_69__authorized_identifier_type']) < 2 and
                            len(row[1]['L_69__authorization_reference']) < 2 and
                            len(row[1]['L_69__participant_client_name']) < 2
                    ):
                        sql = """INSERT INTO public.cda_konza_sftp_retrieval__l_69 (id,
                                          emr_client_delivery_paused,
                                          source_files_zipped)
                            VALUES ('""" + row[1]['L_69__id'] + """',
                               '""" + row[1]['L_69__emr_client_delivery_paused'] + """',
                                  '""" + row[1]['L_69__source_file_zipped'] + """')
                                   ON CONFLICT (id)
                                   DO UPDATE SET 
                                          emr_client_delivery_paused ='""" + row[1][
                            'L_69__emr_client_delivery_paused'] + """',
                                          source_files_zipped = '""" + row[1]['L_69__source_file_zipped'] + """'
                                   ;"""
                    if "INSERT INTO public.cda_konza_sftp_retrieval__l_69" in sql:
                        pgconnection = pgengine.connect()
                        updateRow = pgconnection.execute(sql)
                        pgconnection.close()
                        print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                        newCardID = 'Skipped as automated'
                        try:
                            conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                                   database='formoperations')
                        except:
                            time.sleep(30)
                            conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                                   database='formoperations')
                        sql = "update konza_support set added_card_reference = '" + newCardID + "', project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                        cursor = conn.cursor()
                        cursor.execute(sql)
                        # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                        conn.commit()
                        conn.close()
                        continue  # skips the wekan add since the job is done and no work needs assigned
                    if "INSERT INTO public.cda_konza_sftp_retrieval__l_69" not in sql:
                        #pgconnection = pgengine.connect()
                        #updateRow = pgconnection.execute(sql)
                        #pgconnection.close()
                        cardTitle = 'Automated Process Missing by User Input ' + cardTitle
                        print("Not skipping Card Creation - IF conditions above not met")
                        ## KONZA - Help Desk
                        authorId = 'KJkuCf5wXdRjYHrcb'  # Bot is default #KJkuCf5wXdRjYHrcb
                        boardid = 'ZtFXLZ2pjP6f3MzKW'
                        listid = 'Zt9rtpPjrdHnHzrbT'
                        swimlaneid = '6vQwEdCJW55x4pFBd'
                        # memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                        # Member ID: ZgaFSnZS8Do6mZdMb
                        # Username: alandstrom@konza.org
                        # memberids.append('ZgaFSnZS8Do6mZdMb')  # adding default member to watch for cards
                        memberids.append('mZcXKPorDi5KYwjRT')

                if ((TicketReason == 'Alerts - Delivered - KONZA HL7v2 Raw (Routed Raw Messages) - L-52, L-10, L-82')
                    & (requestorInfo == 'swarnock@konza.org')) or \
                        ((TicketReason == 'Alerts - Delivered - KONZA HL7v2 Raw (Routed Raw Messages) - L-52, L-10, L-82')
                         & (requestorInfo == 'tlamond@konza.org')) or \
                        ((TicketReason == 'Alerts - Delivered - KONZA HL7v2 Raw (Routed Raw Messages) - L-52, L-10, L-82')
                         & (requestorInfo == 'slewis@konza.org')):
                    pg_user = Login.pg_user_operations_logger_ops3
                    pg_password = Login.pg_password_operations_logger_ops3
                    pg_ops_server = Login.pg_ops_server_ops3
                    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
                    pgengine = create_engine(
                        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")
                    # print(row[1]['ehx__ID'])
                    # print(row[1]['ehx__ssi_payor_client_name'])
                    # print(row[1]['ehx__authorized_identifier'])
                    # print(row[1]['ehx__authorization_party'])
                    # print(row[1]['ehx__dh_fusion_delivery_paused'])
                    # print(row[1]['ehx__authorized_identifer_type'])
                    # print(row[1]['ehx__konza_client_destination_delivery_paused'])
                    # print(row[1]['ehx__dh_raw_ehx_ccd_delivery_location'])
                    sql = """INSERT INTO public.raw_hl7v2_feed__l_10 (authorized_identifier,
                                     hl7v2_client_name,
                                      id,
                                      authorization_party,
                                      authorized_identifier_type,
                                      hl7v2_client_delivery_paused)
                        VALUES ('""" + row[1]['L_10__authorized_identifier'] + """',
                         '""" + row[1]['L_10__hl7v2_client_name'] + """',
                          '""" + row[1]['L_10__id'] + """',
                           '""" + row[1]['L_10__authorization_party'] + """',
                             '""" + row[1]['L_10__authorized_identifier_type'] + """',
                              '""" + row[1]['L_10__hl7v2_client_delivery_paused'] + """')
    
                               ON CONFLICT (id)
                               DO UPDATE SET 
                                      authorized_identifier ='""" + row[1]['L_10__authorized_identifier'] + """',
                                      hl7v2_client_name = '""" + row[1]['L_10__hl7v2_client_name'] + """',
                                      authorization_party = '""" + row[1]['L_10__authorization_party'] + """',
                                      authorized_identifier_type = '""" + row[1]['L_10__authorized_identifier_type'] + """',
                                      hl7v2_client_delivery_paused = '""" + row[1][
                        'L_10__hl7v2_client_delivery_paused'] + """'
                               ;"""

                    pgconnection = pgengine.connect()
                    updateRow = pgconnection.execute(sql)
                    pgconnection.close()
                    print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                    newCardID = 'Skipped as automated'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "', project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()
                    continue  # skips the wekan add since the job is done and no work needs assigned

                if ((TicketReason == 'SSI eHealth Exchange Distribution - L-65') & (requestorInfo != 'aschlegel@konza.org')) and \
                        ((TicketReason == 'SSI eHealth Exchange Distribution - L-65')
                         & (requestorInfo != 'jdenson@konza.org')) and \
                        ((TicketReason == 'SSI eHealth Exchange Distribution - L-65')
                         & (requestorInfo != 'slewis@konza.org')):
                    newCardID = 'Unauthorized Request'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "', project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "' where  id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()

                    #sql = "update formoperations.dbo.konza_support set board_name_active = '" + currentBoardTitle + "', processing_status = '" + currentList + "' where id = " + str(
                    #    ksID)
                    #cursor = conn.cursor()
                    #cursor.execute(sql)
                    #conn.commit()
                    #conn.close()

                    continue
                if ((TicketReason == 'SSI eHealth Exchange Distribution - L-65') & (requestorInfo == 'aschlegel@konza.org')) or \
                        ((TicketReason == 'SSI eHealth Exchange Distribution - L-65')
                         & (requestorInfo == 'jdenson@konza.org')) or \
                        ((TicketReason == 'SSI eHealth Exchange Distribution - L-65')
                         & (requestorInfo == 'slewis@konza.org')):
                    ## Must add unique contraint on ID field - notify PM
                    pg_user = Login.pg_user_operations_logger_ops3
                    pg_password = Login.pg_password_operations_logger_ops3
                    pg_ops_server = Login.pg_ops_server_ops3
                    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
                    pgengine = create_engine(
                        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")

                    sql = """INSERT INTO public.konza_ssi_ehealth_exchange_distribution (id,
                         ssi_payor_client_name,
                          authorized_identifier,
                          authorization_party,
                          dh_fusion_delivery_paused,
                          authorized_identifier_type,
                          konza_client_destination_delivery_paused,
                          ssi_prefix,
                          auto_submit_panel_to_corepoint,
                          dh_raw_ehx_ccd_delivery_location)
                                VALUES ('""" + row[1]['ehx__ID'] + """',
                                 '""" + row[1]['ehx__ssi_payor_client_name'] + """',
                                  '""" + row[1]['ehx__authorized_identifier'] + """',
                                   '""" + row[1]['ehx__authorization_party'] + """',
                                    '""" + row[1]['ehx__dh_fusion_delivery_paused'] + """',
                                     '""" + row[1]['ehx__authorized_identifer_type'] + """',
                                      '""" + row[1]['ehx__konza_client_destination_delivery_paused'] + """',
                                      '""" + row[1]['ehx__ssi_prefix'] + """',
                                      '""" + row[1]['ehx__auto_submit_panel_to_corepoint'] + """',
                                       '""" + row[1]['ehx__dh_raw_ehx_ccd_delivery_location'] + """')
    
                                       ON CONFLICT (id)
                                       DO UPDATE SET 
                                              ssi_payor_client_name ='""" + row[1]['ehx__ssi_payor_client_name'] + """',
                                              authorized_identifier = '""" + row[1]['ehx__authorized_identifier'] + """',
                                              authorization_party = '""" + row[1]['ehx__authorization_party'] + """',
                                              dh_fusion_delivery_paused = '""" + row[1]['ehx__dh_fusion_delivery_paused'] + """',
                                              authorized_identifier_type = '""" + row[1]['ehx__authorized_identifer_type'] + """',
                                              konza_client_destination_delivery_paused = '""" + row[1][
                        'ehx__konza_client_destination_delivery_paused'] + """',
                                              ssi_prefix = '""" + row[1]['ehx__ssi_prefix'] + """',
                                              auto_submit_panel_to_corepoint = '""" + row[1][
                              'ehx__auto_submit_panel_to_corepoint'] + """',
                                              dh_raw_ehx_ccd_delivery_location = '""" + row[1][
                              'ehx__dh_raw_ehx_ccd_delivery_location'] + """'
                                        ;"""
                    pgconnection = pgengine.connect()
                    updateRow = pgconnection.execute(sql)
                    pgconnection.close()
                    print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                    newCardID = 'Skipped as automated'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "', project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()
                    ## Omitted due to GID:SUP-2333 https://boards.ertanalytics.com/b/rKdyZk9CqyRp3B2vS/konza-data-work-extract-and-pipeline-services-1-1/kzJXYFNRbYpGQtPdS
                    #s3_client = boto3.client('s3',
                    #                         region_name=Login.konzaandssigroup_region_name,
                    #                         aws_access_key_id=Login.konzaandssigroup_aws_access_key_id,
                    #                         aws_secret_access_key=Login.konzaandssigroup_aws_secret_access_key
                    #                         )
                    #result = s3_client.get_bucket_policy(Bucket='konzaandssigroup')
                    #print(result['Policy'])
                    #
                    #authorizedIDAddition = row[1]['ehx__authorized_identifier']
                    ## authorizedIDAddition = 'Test__Test1'
                    #
                    #bucket_policy = json.loads(result['Policy'])
                    #if authorizedIDAddition not in str(bucket_policy):
                    #    bucket_policy['Statement'][0]['Resource'].append(
                    #        'arn:aws:s3:::konzaandssigroup/' + authorizedIDAddition + '/*')
                    #    bucket_policy['Statement'][0]['Resource'].append(
                    #        'arn:aws:s3:::konzaandssigroup/' + authorizedIDAddition + '/')
                    #    bucket_policy['Statement'][1]['Resource'].append(
                    #        'arn:aws:s3:::konzaandssigroup/' + authorizedIDAddition + '/*')
                    #    bucket_policy['Statement'][1]['Resource'].append(
                    #        'arn:aws:s3:::konzaandssigroup/' + authorizedIDAddition + '/')
                    #    bucket_policy['Statement'][2]['Resource'].append(
                    #        'arn:aws:s3:::konzaandssigroup/' + authorizedIDAddition + '/*')
                    #    bucket_policy['Statement'][2]['Resource'].append(
                    #        'arn:aws:s3:::konzaandssigroup/' + authorizedIDAddition + '/')
                    ## bucket_policy['Statement'][0]['Resource'][len(bucket_policy['Statement'][0]['Resource'])-2] = ['arn:aws:s3:::konzaandssigroup/' + authorizedIDAddition + '/*']
                    #json_data_bucket_policy = json.dumps(bucket_policy)
                    ## len(json_data_bucket_policy)
                    #s3_client.put_bucket_policy(Bucket='konzaandssigroup', Policy=json_data_bucket_policy)
                    continue  # skips the wekan add since the job is done and no work needs assigned
                if ((TicketReason == 'Verinovum Approval Form - L-87')
                    & (requestorInfo == 'tthompson@konza.org')) or \
                        ((TicketReason == 'Verinovum Approval Form - L-87')
                         & (requestorInfo == 'jdenson@konza.org')):
                    pg_user = Login.pg_user_operations_logger_ops3
                    pg_password = Login.pg_password_operations_logger_ops3
                    pg_ops_server = Login.pg_ops_server_ops3
                    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
                    pgengine = create_engine(
                        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")

                    sql = """INSERT INTO public.bcbsks_verinovum__l_87 (id,
                                     authorized_identifier_oid,
                                      authorized_filter_file_beginning_pattern,
                                      authorizing_party,
                                      delivery_paused,
                                      authorization_reference,
                                      participant_client_name,
                                      source_base_path,
                                      destination_path_override,
                                      client_folder_name)
                        VALUES ('""" + row[1]['L_87__id'] + """',
                         '""" + row[1]['L_87__authorized_identifier_oid'] + """',
                          '""" + row[1]['L_87__authorized_filter_file_beginning_pattern'] + """',
                           '""" + row[1]['L_87__authorizing_party'] + """',
                             '""" + row[1]['L_87__delivery_paused'] + """',
                             '""" + row[1]['L_87__authorization_reference'] + """',
                             '""" + row[1]['L_87__participant_client_name'] + """',
                             '""" + row[1]['L_87__source_base_path'] + """',
                             '""" + row[1]['L_87__destination_path_override'] + """',
                             '""" + row[1]['L_87__client_folder_name'] + """')
    
                               ON CONFLICT (id)
                               DO UPDATE SET 
                                      authorized_identifier_oid ='""" + row[1]['L_87__authorized_identifier_oid'] + """',
                                      authorized_filter_file_beginning_pattern = '""" + row[1][
                        'L_87__authorized_filter_file_beginning_pattern'] + """',
                                      authorizing_party = '""" + row[1]['L_87__authorizing_party'] + """',
                                      delivery_paused = '""" + row[1]['L_87__delivery_paused'] + """',
                                      authorization_reference = '""" + row[1]['L_87__authorization_reference'] + """',
                                      participant_client_name = '""" + row[1]['L_87__participant_client_name'] + """',
                                      source_base_path = '""" + row[1]['L_87__source_base_path'] + """',
                                      destination_path_override = '""" + row[1]['L_87__destination_path_override'] + """',
                                      client_folder_name = '""" + row[1]['L_87__client_folder_name'] + """'
                               ;"""

                    pgconnection = pgengine.connect()
                    updateRow = pgconnection.execute(sql)
                    pgconnection.close()
                    print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                    newCardID = 'Skipped as automated'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "' , project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "' where id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()
                    continue  # skips the wekan add since the job is done and no work needs assigned
                if ((TicketReason == 'Alerts - Delivered') & (
                        requestorInfo == 'swarnock@konza.org')) or ((
                                                                            TicketReason == 'Alerts  Raw (Routed Raw Messages) - L-52, L-10, L-82') & (
                                                                            requestorInfo == 'tlamond@konza.org')):
                    pg_user = Login.pg_user_operations_logger_ops3
                    pg_password = Login.pg_password_operations_logger_ops3
                    pg_ops_server = Login.pg_ops_server_ops3
                    pg_ops_user_db = Login.pg_ops_db_operations_logger_ops3
                    pgengine = create_engine(
                        "postgresql+psycopg2://operationslogger:" + pg_password + "@" + pg_ops_server + "/sourceoftruth?sslmode=require")

                    sql = """INSERT INTO public.raw_hl7v2_feed__l_10 (authorized_identifier,
                                     hl7v2_client_name,
                                      id,
                                      authorization_party,
                                      authorized_identifier_type,
                                      hl7v2_client_delivery_paused)
                        VALUES ('""" + row[1]['L_10__authorized_identifier'] + """',
                         '""" + row[1]['L_10__hl7v2_client_name'] + """',
                          '""" + row[1]['L_10__id'] + """',
                           '""" + row[1]['L_10__authorization_party'] + """',
                             '""" + row[1]['L_10__authorized_identifier_type'] + """',
                              '""" + row[1]['L_10__hl7v2_client_delivery_paused'] + """')
    
                               ON CONFLICT (id)
                               DO UPDATE SET 
                                      authorized_identifier ='""" + row[1]['L_10__authorized_identifier'] + """',
                                      hl7v2_client_name = '""" + row[1]['L_10__hl7v2_client_name'] + """',
                                      authorization_party = '""" + row[1]['L_10__authorization_party'] + """',
                                      authorized_identifier_type = '""" + row[1]['L_10__authorized_identifier_type'] + """',
                                      hl7v2_client_delivery_paused = '""" + row[1][
                        'L_10__hl7v2_client_delivery_paused'] + """'
                               ;"""

                    pgconnection = pgengine.connect()
                    updateRow = pgconnection.execute(sql)
                    pgconnection.close()
                    print("Skipping Card addition to board as preauthorized - ticket reason: " + TicketReason)
                    newCardID = 'Skipped as automated'
                    try:
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    except:
                        time.sleep(30)
                        conn = pymssql.connect(server=serverAddress, user=user, password=password,
                                               database='formoperations')
                    sql = "update konza_support set added_card_reference = '" + newCardID + "' , project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                    conn.commit()
                    conn.close()
                    continue  # skips the wekan add since the job is done and no work needs assigned
                time.sleep(10)
                wekanUsers = subprocess.run(["python37", "//prd-az1-sqlw2/batch/py Operations/"+wekanToUse+".py", "users"],
                                            capture_output=True)

                for a in str(wekanUsers).split('{"_id":"'):
                    if "username" in a:
                        print(a)
                        member_ID = a.split('","')[0]
                        # print("Member ID: " + member_ID)
                        username = a.split('"')[4]
                        # print("Username: " + username)
                        try:
                            if (username.lower() == requestorInfo.split("@konza.org")[0].lower()) or (
                                    username.split("@konza.org")[0].lower() == requestorInfo.split("@konza.org")[
                                0].lower()):
                                # if (username.lower() == requestorInfo.split("@konza.org")[0].lower()) \
                                #        or (username.lower() == cardDescription.split("@konza.org")[0].split("_")[-1]) \
                                #        or (username.lower().split("@konza.org")[0] == cardDescription.split("@konza.org")[0].split("_")[-1]) \
                                #        or ( cardDescription.split("@konza.org")[0].split("_")[-1].split(" ")[-1] in username.lower().split("@konza.org")[0]):
                                memberids.append(member_ID)
                                print("Adding requestor " + username)
                                # memberids.append('S3tGjofrtCeiKsp8c')
                        except AttributeError:
                            pass
                        try:
                            if (username.lower() == str(OnBehalfOf.strip()).split("@konza.org")[0].lower()) or (
                                    username.lower() == str(OnBehalfOf.strip()).split("@konza.org")[0].lower()):
                                # if (username.lower() == requestorInfo.split("@konza.org")[0].lower()) \
                                #        or (username.lower() == cardDescription.split("@konza.org")[0].split("_")[-1]) \
                                #        or (username.lower().split("@konza.org")[0] == cardDescription.split("@konza.org")[0].split("_")[-1]) \
                                #        or ( cardDescription.split("@konza.org")[0].split("_")[-1].split(" ")[-1] in username.lower().split("@konza.org")[0]):
                                memberids.append(member_ID)
                                print("Adding requestor " + username)
                                # memberids.append('S3tGjofrtCeiKsp8c')
                        except AttributeError:
                            pass
                memberidsDistinct = []
                for member in memberids:
                    if member not in memberidsDistinct:
                        memberidsDistinct.append(member)

                memberString = joined_string = ",".join(memberids)
                if not memberString:
                    memberString = ''#May need to get rid of extra nulls or none values in this list prior to card addition SUP-11484, ideally add correct members
                print("Adding Card to board - based on ticket reason: " + TicketReason)
                if optionRemoveHTML == True:
                    soup = BeautifulSoup(cardDescription,'html5lib')
                    cardDescription = soup.get_text(separator=r"\n\n\n")
                time.sleep(10)
                retries = 3
                for attempt in range(retries):
                    try:
                        addLog = subprocess.run(["python37", "//prd-az1-sqlw2/batch/py Operations/"+wekanToUse+".py", "addcard", authorId,
                                         # boardid, swimlaneid, listid, cardTitle, cardDescription]
                                         boardid, swimlaneid, listid, cardTitle, cardDescription, memberString]
                                        , capture_output=True)
                        # addLog.stdout
                        #addLog = 'CompletedProcess(args=['python37', '//prd-az1-sqlw2/batch/py Operations/wekan_boardskonzaorg.py', 'addcard', 'KwMkTND63bbwYCqew', 'sGyzAxaDDni7wbNF7', 'ypoPDmZbfFKFkSv7d', 'endRNYGKthW5AjPkF', 'GID:SUP-13490 Information Request - Entire OrganizationClient/Participant Name: ||||||||| - S-307 - Description: Mihir has requested all active production configurations.  These must be collected from... - Requestor: ethompson@konza.org', ' Description: Mihir has requested all active production configurations.  These must be collected from the live systems and reviewed by the security team prior to release.  This should be completed prior to 1/6 afternoon.\n\nThese configuration contain known security elements like keys, certificates, etc that should not be sent via email and these elements should be scrubbed prior to delivery', 'yNhShJdK8wxSYK3C2'], returncode=0, stdout=b'# of arguments passed: 8\r\nMember IDs passed in argument 8: yNhShJdK8wxSYK3C2\r\nauthorId=KwMkTND63bbwYCqew&title=GID%3ASUP-13490+Information+Request+-+Entire+OrganizationClient%2FParticipant+Name%3A+%7C%7C%7C%7C%7C%7C%7C%7C%7C+-+S-307+-+Description%3A+Mihir+has+requested+all+active+production+configurations.++These+must+be+collected+from...+-+Requestor%3A+ethompson%40konza.org&description=+Description%3A+Mihir+has+requested+all+active+production+configurations.++These+must+be+collected+from+the+live+systems+and+reviewed+by+the+security+team+prior+to+release.++This+should+be+completed+prior+to+1%2F6+afternoon.%0A%0AThese+configuration+contain+known+security+elements+like+keys%2C+certificates%2C+etc+that+should+not+be+sent+via+email+and+these+elements+should+be+scrubbed+prior+to+delivery&swimlaneId=ypoPDmZbfFKFkSv7d&members=yNhShJdK8wxSYK3C2&members=yNhShJdK8wxSYK3C2\r\n{"id":"faAnkjgZmLRAcFDp3","token":"TpHPpjTIEuptUP_dOllh4uyyQ5wRZzqG1NWlQ3CU6qK","tokenExpires":"2025-03-23T23:29:59.839Z"}{"_id":"hssz5xqPjfC7ZzcEw"}\r\n', stderr=b"//prd-az1-sqlw2/batch/py Operations/wekan_boardskonzaorg.py:4: DeprecationWarning: the imp module is deprecated in favour of importlib; see the module's documentation for alternative uses\r\n  import imp\r\n")'
                        newCardID = str(addLog.stdout.decode("utf-8")).split('{"_id":"')[1].split('"}')[
                            0]  ##addLog - hPuFSNXFQwzht94r6
                        break
                    except IndexError:
                        if attempt < retries - 1:
                            print(f"Attempt {attempt + 1} failed. Retrying...")
                        else:
                            raise
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        raise
                try:
                    conn = pymssql.connect(server=serverAddress, user=user, password=password, database='formoperations')
                except:
                    time.sleep(30)
                    conn = pymssql.connect(server=serverAddress, user=user, password=password, database='formoperations')
                sql = "update konza_support set added_card_reference = '" + newCardID + "' , project_ids = '" + projectIdsMapped.replace("'","") + "', prioritization = '" + priority + "'  where id = " + str(id)
                cursor = conn.cursor()
                cursor.execute(sql)
                # conn.execute_query("update production_failure set added_card_reference = " + newCardID + " where id = " + id)
                conn.commit()
                conn.close()
                # boardid = 'eAzLCnYGW2eCgnmRr'  # konza-encryption-security-and-risk-assessments
                # newCardID = 'kckhP4exSXhszjskf'  # konza-encryption-security-and-risk-assessments
                sql = """INSERT INTO public.global_ticket_crosswalk (global_id, ticketing_system_id, ticketing_system)
                        VALUES (nextval('global_ticket_crosswalk_seq'), 
                                     '""" + newCardID + """',
                                     '""" + boardid + """')
                                           ;"""

                pgconnection = pgengine.connect()
                updateRow = pgconnection.execute(sql)
                pgconnection.close()

                ##Does not work - needs discovery, no error just does not update field.
                editLog = subprocess.run(
                    ["python37", "//prd-az1-sqlw2/batch/py Operations/" + wekanToUse + ".py", "editcard_customfields",
                     # boardid, swimlaneid, listid, cardTitle, cardDescription]
                     newCardID, boardid, swimlaneid, listid, customPriorityFieldID, customMatrixFieldID, customPriority, customRoundedAverageImpactRange]
                    , capture_output=True)
                #import datetime
                #from datetime import timedelta

                #current_time = datetime.datetime.now() + timedelta(hours=6)
                if customPriority == 'Critical':
                    color = 'crimson'
                if customPriority == 'High':
                    color = 'red'
                if customPriority == 'Medium':
                    color = 'orange'
                if customPriority == 'Low':
                    color = 'lime'
                #dueAt = str(current_time)
                #time.sleep(10)
                # newCardID
                # boardid
                # swimlaneid
                # listid
                # color
                # listIdToDo
                # dueAt
                editLog = subprocess.run(
                    ["python37", "//prd-az1-sqlw2/batch/py Operations/" + wekanToUse + ".py", "editcard_color",
                     # boardid, swimlaneid, listid, cardTitle, cardDescription]
                     newCardID, boardid, swimlaneid, listid, color]
                    , capture_output=True)


                if TicketReason in ['Internal Audit']:
                    try:
                        import datetime
                        from datetime import timedelta

                        current_time = datetime.datetime.now() + timedelta(hours=6)
                        color = 'crimson'
                        dueAt = str(current_time)
                        listIdToDo = '489T6a7mkEwJgNva8'
                        time.sleep(10)
                        editLog = subprocess.run(
                            ["python37", "//prd-az1-sqlw2/batch/py Operations/wekan_test.py", "editcard",
                             # boardid, swimlaneid, listid, cardTitle, cardDescription]
                             newCardID, boardid, swimlaneid, listid, color, listIdToDo, dueAt]
                            , capture_output=True) ## May need wekan_test differences
                    except:
                        pass
                    print('edit card here')

            print("Updated table to indicate the job has completed and a new round can being")
            pgconnection = pgengine.connect()
            overLapOffQuery = pgconnection.execute("""update job_triggers set trigger_status = '0' where associated_table = 'card_addition_erta_com_konza_support_curl'
                                                                    ;""")
        pgconnection.close()


except:
    try:
        titleString = os.path.basename(sys.argv[0])
        errormsg = str(sys.exc_info())
        localtime = str(time.asctime(time.localtime(time.time())))
        lineNumber = str(sys.exc_info()[-1].tb_lineno)
        print(errormsg)
    except:
        titleString = ''
        errormsg = ''
        localtime = ''
        lineNumber = ''
        daypathandSub = ''
    errorFileLocation = "\\\\prd-az1-sqlw2.ad.konza.org\\output\\Run_Errors.txt"
    # trace = traceback.print_tb(limit=2500)
    f = open(errorFileLocation, "a")
    f.write(
        "Time: " + localtime + " | Line Number: " + lineNumber + " | File: " + titleString + " | Error: " + errormsg + "\n")
    f.close()
