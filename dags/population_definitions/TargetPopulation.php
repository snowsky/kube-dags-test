<?php

$initialPath = "\\\\prd-az1-sqlw2.ad.konza.org\Batch\\";  require ($initialPath.'\Login.php');
//$initialPath = "\\\\prd-az1-sqlw2.ad.konza.org\Batch\\";  require ("\\\\prd-az1-sqlw2.ad.konza.org\Batch\Clients\\Login.php");

include ('.\ClientTransferLogin.php');
 
$population_name = $folder_name . "_18M_ADMIT";

$independentTable = str_replace(' ', '', strtolower($folder_name));
$StartTime = time();
$StartTimeLog = date("Y-m-d H:i:s",$StartTime);
$sql = "INSERT INTO `clientresults`.`etl_status` (`table_name`, `start_time`) VALUES ('$folder_name','$StartTimeLog');";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql" . trigger_error ('SQL Error: '.$sql));

## Runs once to allow ETL Runner as the user to test a population change, then removes Revised status
print('Frequency set to: '.$frequency);
if ($frequency == 'Revised') {
    $sql = "
    update _dashboard_requests.clients_to_process
    set frequency = ''
    where `folder_name` = '$folder_name'
    ;
    ";
    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());

    $sql = "
    update clientresults.client_hierarchy
    set frequency = ''
    where `folder_name` = '$folder_name'
    ;
    ";
    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());
}

print('Frequency set to: '.$frequency);
if ($frequency == 'Approved') {
    $sql = "
    update _dashboard_requests.clients_to_process
    set frequency = ''
    where `folder_name` = '$folder_name'
    ;
    ";
    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());

    $sql = "
    update clientresults.client_hierarchy
    set frequency = ''
    where `folder_name` = '$folder_name'
    ;
    ";
    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());
}
## End of the Revised Population Status Update

$conn = mysqli_connect($server, $user, $password, $dbname);
$khsdbname = '_dashboard_maintenance';
$ertconn = mysqli_connect($khstestserver, $khstestuser, $khstestpassword, $khsdbname);
//CSV Based Listing

//print('Table Reference: ' . $independentTable . '
//');

$clientFileName = 'CSVSOURCEFILE05302023.csv';

$sql = "
drop table if exists temp.tp$independentTable
;
";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;

$sql = "
create table temp.tp$independentTable like clientresults.client_defined_populations
;
";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;

//$passedTableName = "tp".$independentTable;
echo "Code run 01";
$sql = "SET SQL_SAFE_UPDATES = 0;";
$ertconn = mysqli_connect($khstestserver, $khstestuser, $khstestpassword, '_dashboard_maintenance');
$query = mysqli_query($ertconn, $sql)or die("A MySQL error has occurred.<br />Error: $sql" . trigger_error ('SQL Error: '.$sql));
echo "Code run 02";
$sql = "DELETE FROM `_dashboard_maintenance`.`cross_language_markers` WHERE `job_name`='tp".$independentTable."_TargetPopulation_python.py';";
$query = mysqli_query($ertconn, $sql)or die("A MySQL error has occurred.<br />Error: $sql" . trigger_error ('SQL Error: '.$sql));
$sql = "INSERT INTO `_dashboard_maintenance`.`cross_language_markers` (`job_name`, `count`) VALUES ('tp".$independentTable."_TargetPopulation_python.py', '1');";
$query = mysqli_query($ertconn, $sql)or die("A MySQL error has occurred.<br />Error: $sql" . trigger_error ('SQL Error: '.$sql));
echo "Code run 03";
exec('start python37 ".\TargetPopulation_python.py" "tp'.$independentTable.'" "'.$clientFileName.'" "'.$folder_name.'"');
echo "Code run 04";
$sql = "SELECT `count` FROM `_dashboard_maintenance`.`cross_language_markers` where job_name = 'tp".$independentTable."_TargetPopulation_python.py';";
echo "Code run 05";
$query = mysqli_query($ertconn, $sql)or die("A MySQL error has occurred.<br />Error: $sql" . trigger_error ('SQL Error: '.$sql));
$pool_count = mysqli_fetch_assoc($query);
$currentPoolCount = $pool_count['count'];
echo "Current Pool: $currentPoolCount
";

$ertconn = mysqli_connect($khstestserver, $khstestuser, $khstestpassword, '_dashboard_maintenance');
////waiting for the PY script to finish
while($currentPoolCount <> '0'){
    sleep(1);
    echo "Waiting for ".$independentTable."_TargetPopulation_python.py to finish
";
    $sql = "SELECT `count` FROM `_dashboard_maintenance`.`cross_language_markers` where job_name = 'tp".$independentTable."_TargetPopulation_python.py';";
    $query = mysqli_query($ertconn, $sql)or die("A MySQL error has occurred.<br />Error: $sql" . trigger_error ('SQL Error: '.$sql));
    $pool_count = mysqli_fetch_assoc($query);
    $currentPoolCount = $pool_count['count'];
    echo "Pool Workers Outstanding: ".$currentPoolCount."
";
//echo "Code run 06";
if($currentPoolCount == '2') {die("Python Script Failed - See Run_Errors.txt");}

}

$sql = "
insert into temp.tp$independentTable (first_name
        , last_name
        , dob
        , sex
        , street1
        , City
        , State
        , zip
		, ins_member_id
        , MPI ) (
        select firstname
        , lastname
        , dob
        , sex
        , street1
        , City
        , State
        , zip
		, ins_member_id
        , MPI from temp.tp".$independentTable."_py_to_sql where length(mpi)>1
        )
";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;

//if ($frequency == 'Daily') {
//    $sql = "
//delete from clientresults.client_security_groupings_approved
//where `Client` = '$folder_name'
//;
//";
//    $query = mysqli_query($conn, $sql) or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//}

$sql = "
delete from clientresults.client_security_groupings
where `Client` = '$folder_name'
;
";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;

//$sql = "
//insert into clientresults.client_security_groupings (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname, server_id)
//select '$folder_name', TP.`MPI`, '','','', TP.`provider_first_name`,'$ending_db' from temp.tp$independentTable TP
//group by MPI;
//";
//$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;

//if ($frequency == 'Daily') {
//    $sql = "
//insert into clientresults.client_security_groupings_approved (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname, server_id)
//select '$folder_name', TP.`MPI`, '','','', TP.`provider_first_name`,'$ending_db' from temp.tp$independentTable TP
//group by MPI;
//";
//    $query = mysqli_query($conn, $sql) or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//}

$sql = "
insert into clientresults.client_security_groupings (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname, server_id)
select '$folder_name', TP.`MPI`, '','','', TP.`provider_first_name`,'$ending_db' from temp.tp$independentTable TP
group by MPI;
";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;

$sql = "
delete from clientresults.client_extract_mpi_crosswalk
where `Client` = '$folder_name'
;
";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;


$sql = "
insert into clientresults.client_extract_mpi_crosswalk (`Client`, MPI, server_id,36_month_visit_reference_accid,ins_member_id,client_identifier,client_identifier_2)
select '$folder_name', TP.`MPI`,'$ending_db', '', ins_member_id,client_identifier,client_identifier_2 from temp.tp$independentTable TP
#left join temp.pa_thirtysix_month_lookback TML on TP.MPI = TML.MPI
where TP.MPI is not null and length(TP.MPI) > 1
;
";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;

if ($frequency == 'Approved') {
    $sql = "
    delete from clientresults.client_security_groupings_approved
    where `Client` = '$folder_name'
    ;
    ";
    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
}

if ($frequency == 'Approved') {
    $sql = "
insert into clientresults.client_security_groupings_approved (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname, server_id)
select '$folder_name', TP.`MPI`, '','','', TP.`provider_first_name`,'$ending_db' from temp.tp$independentTable TP
group by MPI;
";
    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
}


//$sql = "
//insert into clientresults.client_security_groupings (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname)
//select '$folder_name_admin_user', TP.`MPI`, '','','', TP.`provider_first_name` from temp.tp$independentTable TP
//group by MPI
//;
//";
//$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;


////Start Provider Lists
//date_default_timezone_set('America/Chicago');
//$StartTime = time();
//$StartTimeLog = date("Y-m-d H:i:s",$StartTime);
//echo("Job Started at " . date("Y-m-d H:i:s", $StartTime) . "
// ");
//
// $sql = "select provider_first_name Provider_Reference  from temp.tp$independentTable
//where length(provider_first_name) > 1
//group by provider_first_name
//;";
// $query = mysqli_query($conn, $sql);
// 
//$proArray = array();
//if (mysqli_num_rows($query) > 0) {
//    // output data of each row
//    while($row = mysqli_fetch_assoc($query)) {
//        $result = array(
//						'Provider_Reference' => $row['Provider_Reference']
//		);
//		$proArray[] = $result;
//    }
//} else {
//    echo "0 results";
//}
//
//foreach ($proArray as $row) {
//	$Provider = $row['Provider_Reference'];
//	$sec_client_grp = $folder_name . ' ' . str_replace(',','',$Provider);
//	$sql = "
//	delete from clientresults.client_security_groupings
//	where `Client` = '$sec_client_grp'
//	;
//	";
//	$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//
//    $sql = "
//	insert into clientresults.client_security_groupings (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname,server_id)
//	select '$sec_client_grp', TP.`MPI`, '','','', TP.`provider_first_name`,'$ending_db' from temp.tp$independentTable TP
//	where provider_first_name = '$Provider'
//	group by MPI;
//	";
//    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//
//
//
//    //Alterations of Requested MPIs
//    echo "
//    Starting DI based PostgreSQL MPI Alterations
//    ";
//
////We start with only doing the deep cut out of MPIs in the security group definitions from Production
//// In another stage, recurring nightly, we will do a superficial add/remove to hasten to impact
//// Dev and Prod will be impacted by the superficial change, but only Prod based changes will impact this deeper cut out of MPI
//
//    $result = pg_query($dbprod_connection, "select mpi, new_client_group, admin_group, action from pa_provider_change_reqs where admin_group = '$folder_name'");
//    if ($result === false) {
//        print pg_last_error($dbprod_connection);
//        exit;
//    } else {
//        print 'PG Query Successful - OK';
//    }
//    $change_list = pg_fetch_all($result);
//    //print_r($change_list);
//    foreach ($change_list as $row) {
//        $pg_mpi = $row['mpi'];
//        $pg_new_client_group = $row['new_client_group'];
//        $pg_admin_group = $row['admin_group'];
//        $pg_action = $row['action'];
//
//        echo $pg_mpi;
//        echo $pg_new_client_group;
//        echo $pg_admin_group;
//        echo $pg_action;
//        if($pg_action == 'Add') {
//            $sql = "
//insert ignore into clientresults.client_security_groupings (client, MPI,server_id)
//(select '$sec_client_grp', '$pg_mpi','$ending_db')
//";
//            $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//            echo $sql;
//        }
//        if($pg_action == 'Remove') {
//            $sql = "
//delete from clientresults.client_security_groupings
//where `MPI` = '$pg_mpi' and Client = '$pg_new_client_group'
//;
//";
//            $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//            echo $sql;
//        }
//
//    }
////  End of alterations
//
//
//}
//

//$query = mysqli_query($conn, $sql1)or die("A MySQL error has occurred.<br />Error: $sql1" . trigger_error ('SQL Error: '.$sql1));

//if ($frequency == 'Approved') {
//    $sql = "
//    delete from clientresults.client_security_groupings_approved
//    where `Client` = '$folder_name'
//	and `server_id` = '$ending_db'
//    ;
//    ";
//    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//	if (strlen($ending_db2) > 1) {
//		$sql = "
//    delete from clientresults.client_security_groupings_approved
//    where `Client` = '$folder_name'
//	and `server_id` = '$ending_db2'
//    ;
//    ";
//    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//	}
//	
//}
//
//if ($frequency == 'Approved') {
//    $sql = "
//insert into clientresults.client_security_groupings_approved (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname, server_id)
//select '$folder_name', TP.`MPI`, '','','', TP.`provider_first_name`,'$ending_db' from temp.tp$independentTable TP
//group by MPI;
//";
//    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//	if (strlen($ending_db2) > 1) {
//		$sql = "
//insert into clientresults.client_security_groupings_approved (`Client`, MPI, Provider_NPI, Provider_DB_ID, Provider_fname, Provider_lname, server_id)
//select '$folder_name', TP.`MPI`, '','','', TP.`provider_first_name`,'$ending_db2' from temp.tp$independentTable TP
//group by MPI;
//";
//    $query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
//	}
//}
echo "Security Group Defined and Submitted
//";
$End_time = time();
$EndTimeLog = date("Y-m-d H:i:s",$End_time);
$sql = "UPDATE `clientresults`.`etl_status` SET `end_time`= '".$EndTimeLog."', `etl_time_elapsed` = TIMEDIFF('".$EndTimeLog."', '".$StartTimeLog."')WHERE `table_name`='".$folder_name."';";
#$sql1 = "INSERT INTO `clientresults`.`etl_status` (`table_name`, `start_time`, `end_time`, `etl_time_elapsed`) VALUES ('".$folder_name."','".$StartTimeLog."','".$EndTimeLog."',TIMEDIFF('".$EndTimeLog."', '".$StartTimeLog."'));";
$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql" . trigger_error ('SQL Error: '.$sql));



//
///////
////$sql = "
////drop table if exists clientdatashare.zip_target_pop;
////;
////";
////$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;
////
////$sql = "
////create table clientdatashare.zip_target_pop
////SELECT left(AL.zip,5) AL_Zip, count(distinct(TP.MPI)) `Total Pop` FROM clientresults.addresses_latest AL
////inner join clientdatashare.target_population TP on TP.MPI = AL.MPI
////group by left(AL.zip,5)
////order by `Total Pop` desc
////;
////";
////$query = mysqli_query($conn, $sql)or die("A MySQL error has occurred.<br />Error: $sql(" . mysqli_errno() . ") " . mysqli_error());;



include ($initialPath.'\c_60_running_count.php');?>
