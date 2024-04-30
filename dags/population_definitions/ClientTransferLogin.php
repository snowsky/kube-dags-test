<?php

$initialPath = "\\\\prd-az1-sqlw2.ad.konza.org\Batch\\";  require ($initialPath.'\Login.php');
//$initialPath = "\\\\prd-az1-sqlw2.ad.konza.org\Batch\\";  require ("\\\\prd-az1-sqlw2.ad.konza.org\Batch\Clients\\Login.php");



//$connqa = mysqli_connect($server_qa, $user, $password_qa, $dbname);

$folder_name = "1098";
$conn = mysqli_connect($server , $user, $password, $dbname);

$sql = "select * from clientresults.client_hierarchy
where folder_name = '$folder_name'
;";
$query = mysqli_query($conn, $sql);


$dbArray = array();
if (mysqli_num_rows($query) > 0) {
    // output data of each row
    while($row = mysqli_fetch_assoc($query)) {
        $result = array(
						'id' => $row['id'],
						'ending_db' => $row['ending_db'],
						'client_name' => $row['client_name'],
						'client_dbs' => $row['client_dbs'],
						'client_tracts' => $row['client_tracts'],
						'client_csv' => $row['client_csv'],
						'facility_ids' => $row['facility_ids'],
						'facility_ids_client_interest' => $row['facility_ids_client_interest'],
						'frequency' => $row['frequency'],
						'target_population_inc_limits' => $row['target_population_inc_limits'],
						'folder_name' => $row['folder_name']
		);
		$dbArray[] = $result;
    }
} else {
    echo "0 results";
}


foreach ($dbArray as $row) {
	
$ending_db = $row['ending_db'];
$client_name = $row['client_name'];
$client_dbs = $row['client_dbs'];
$client_tracts = $row['client_tracts'];
$client_csv = $row['client_csv'];
$facility_ids = $row['facility_ids'];
$facility_ids_client_interest = $row['facility_ids_client_interest'];
$frequency = $row['frequency'];
$target_population_inc_limits = $row['target_population_inc_limits'];
$folder_name = $row['folder_name'];
}

$formatted_facility_ids = "'". str_replace ("," ,"','", $facility_ids) ."'";
$formatted_vaults = "'". str_replace ("," ,"','", $client_dbs) ."'";

//echo $formatted_facility_ids;

$vaults = $formatted_vaults;
$unit_vault_facility_ids = $formatted_facility_ids;

?>
