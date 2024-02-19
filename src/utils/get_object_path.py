def get_object_path(records):
    """
    Args:
        param1: event['Records']; records from aws event object

    Returns:
        str: bucke_name, object_name  
        
    Raises:
        None

    Logs:
        None

    Extracts bucket and object references from Records field of event.
    """ 
  
    return records[0]['s3']['bucket']['name'], \
        records[0]['s3']['object']['key']