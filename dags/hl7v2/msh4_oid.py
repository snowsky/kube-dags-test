import hl7
import re
import yaml
import pytest
from typing import Dict, List, Union, Tuple

# todo: this needs to be reverted when landing in PROD
#For Dev :
#MSH4_OID_CONFIG = '/home/jovyan/konza-dags/dags/hl7v2/msh4_oid.yaml'
#For PROD :
#MSH4_OID_CONFIG = 'dags/hl7v2/msh4_oid.yaml'

# AA Testing V1 : 
#MSH4_OID_CONFIG = '/home/jovyan/konza-dags/dags/hl7v2/msh4_oid.yaml'
# AA Testing V2 : 
MSH4_OID_CONFIG = '/konza-dags/dags/hl7v2/msh4_oid.yaml'

def validate_hl7v2_oid(oid: str) -> bool:
    """
    Validates if a string is a valid HL7v2 OID (Object Identifier).
    
    An HL7v2 OID must meet these criteria:
    - Contains only digits and dots
    - Starts and ends with a digit (not a dot)
    - Has at least two nodes (one dot minimum)
    - Each node is a non-negative integer
    - First node must be 0, 1, or 2
    - If first node is 0 or 1, second node must be 0-39
    - If first node is 2, second node can be any non-negative integer
    - No leading zeros in nodes (except single '0')
    - Maximum length is typically 128 characters for HL7v2
    
    Args:
        oid (str): The OID string to validate
        
    Returns:
        bool: True if valid HL7v2 OID, False otherwise
    """
    
    # Check if input is string
    if not isinstance(oid, str):
        return False
    
    # Check if empty or too long
    if not oid or len(oid) > 128:
        return False
    
    # Check basic format: only digits and dots, no consecutive dots
    if not re.match(r'^[0-9.]+$', oid):
        return False
    
    # Must not start or end with dot
    if oid.startswith('.') or oid.endswith('.'):
        return False
    
    # Must not have consecutive dots
    if '..' in oid:
        return False
    
    # Split into nodes
    nodes = oid.split('.')
    
    # Must have at least 2 nodes
    if len(nodes) < 2:
        return False
    
    # Validate each node
    for i, node in enumerate(nodes):
        # Node cannot be empty
        if not node:
            return False
        
        # Check for leading zeros (except single '0')
        if len(node) > 1 and node.startswith('0'):
            return False
        
        # Must be valid integer
        try:
            node_int = int(node)
        except ValueError:
            return False
        
        # Must be non-negative
        if node_int < 0:
            return False
        
        # Special rules for first two nodes
        if i == 0:
            # First node must be 0, 1, or 2
            if node_int not in [0, 1, 2]:
                return False
        elif i == 1:
            # Second node restrictions based on first node
            first_node = int(nodes[0])
            if first_node in [0, 1] and node_int > 39:
                return False
    
    return True

def yaml_to_list(yaml_file_path) -> List[Dict]:
    """
    Read a YAML file and return it as a Python list.
    
    Args:
        yaml_file_path (str): Path to the YAML file
        
    Returns:
        list: The data from the YAML file as a Python list
    """
    try:
        with open(yaml_file_path, 'r', encoding='utf-8') as yamlfile:
            data = yaml.safe_load(yamlfile)
        
        print(f"Successfully loaded {len(data)} items from {yaml_file_path}")
        return data
        
    except FileNotFoundError:
        print(f"Error: YAML file '{yaml_file_path}' not found")
        return []
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {str(e)}")
        return []
    except Exception as e:
        print(f"Error: {str(e)}")
        return []

def load_msh4_oid_config() -> Tuple[Dict[str, str], Dict[str, str]]:
    
    config_list = yaml_to_list(MSH4_OID_CONFIG)
    facility_name_to_oid = {x['facilityName']: x['euidOid'] for x in config_list}
    facility_mnemonic_to_oid = {x['facilityMnemonic']: x['euidOid'] for x in config_list}

    return facility_name_to_oid, facility_mnemonic_to_oid

def get_msh4(file_path: str) -> hl7.Segment:
    with open(file_path) as f:
        message = f.read().replace('\n', '\r')
    hl7v2_message = hl7.parse(message)
    msh_segment = hl7v2_message.segments('MSH')
    if msh_segment is None or len(msh_segment) == 0:
        return None
    if len(msh_segment[0]) < 5:
        return None
    # Note: the MSH segment is 1-indexed
    return str(msh_segment[0][4])

# todo:switch to loading dicts once inside the function  
_facility_name_to_oid, _facility_mnemonic_to_oid = load_msh4_oid_config() 

def get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback(hl7v2_file_path: str) -> str:

    msh4 = get_msh4(hl7v2_file_path) 
    msh4_is_oid = validate_hl7v2_oid(msh4)
    if msh4_is_oid:
        return msh4
    elif msh4 in _facility_name_to_oid:
        return _facility_name_to_oid[msh4]
    elif msh4 in _facility_mnemonic_to_oid:
        return _facility_mnemonic_to_oid[msh4]
    else:
        raise ValueError(f'Cannot find euidOid for facility "{msh4}"')
