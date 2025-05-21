import json

def get_ids_to_delete_impl(ids_to_delete_file, max_delete_rows=None):
    if ids_to_delete_file:
        with open(ids_to_delete_file) as f:
            ids_to_delete = json.load(f)
        delete_list = []
        if ids_to_delete:
            if max_delete_rows is None:
                delete_list.append(ids_to_delete)  # Append the full list as one chunk
            else:
                for i in range(0, len(ids_to_delete), max_delete_rows):
                    delete_list.append(ids_to_delete[i:i + max_delete_rows])
        return delete_list

