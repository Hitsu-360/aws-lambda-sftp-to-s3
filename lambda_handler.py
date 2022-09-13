from email.policy import default
from stat import S_IMODE, S_ISDIR, S_ISREG
from datetime import datetime, timedelta
from io import BytesIO

import paramiko
import boto3

# Used to improve downloading performance
class FastTransport(paramiko.Transport):
    def __init__(self, sock):
        super(FastTransport, self).__init__(sock)
        self.window_size = 2147483647
        self.packetizer.REKEY_BYTES = pow(2, 40)
        self.packetizer.REKEY_PACKETS = pow(2, 40)

secrets_client = boto3.client(
    'secretsmanager',
    region_name='<your-region>'
)

S3 = boto3.resource(
    's3',
    region_name = '<your-region>'
)

BUCKET = '<your-bucket-name>'

def load_s3_file(s3_path, file):
     # Init bucket object
    bucket = S3.Bucket(BUCKET)
    
    final_path = s3_path
    
    # Write to file    
    bucket.put_object(Key=final_path, Body=file)


def s3_file_exists(s3_path):

    bucket = S3.Bucket(BUCKET)

    if len(list(bucket.objects.filter(Prefix=s3_path))) > 0:
        return True
    
    return False

def s3_list_files():

    bucket = S3.Bucket(BUCKET)

    return list(f.key for f in bucket.objects.all())

def sftp_get_files(sftp_site, path, sftp):

    mode = sftp.stat(path).st_mode

    files_to_load = []

    if S_ISDIR(mode):

        item_list = []

        item_list = [x.filename for x in sorted(sftp.listdir_attr(path), key = lambda f: f.st_mtime)]
     
        for item in item_list:

            item_path = path + '/' + item

            item_mode = sftp.stat(item_path).st_mode

            if not S_ISDIR(item_mode):
            
                file_date_raw = sftp.stat(item_path).st_mtime
                file_date = datetime.fromtimestamp(file_date_raw)

                s3_path = item_path.replace('./', sftp_site + '/')

                files_to_load.append({'file_s3_path': s3_path, 'file_sftp_path': item_path, 'file_sftp_date': file_date})

    return files_to_load

def sftp_get_recursive(sftp_site, path, sftp):
    
    mode = sftp.stat(path).st_mode
    
    if not S_ISDIR(mode):

        file_date_raw = sftp.stat(path).st_mtime
        file_date = datetime.fromtimestamp(file_date_raw)
        
        s3_path = path.replace('./', sftp_site + '/')

        return [{'file_s3_path': s3_path, 'file_sftp_path': path, 'file_sftp_date': file_date}]
            
    else:
        item_list = sftp.listdir(path)

        files_to_load = []

        for item in item_list:
            
            item = str(item)

            files_to_load.extend(sftp_get_recursive(sftp_site, path + '/' + item, sftp))

        return files_to_load

def lambda_handler(request, context):
    
    if request.get('sftp'):
        
        # This name will be the folder name in s3
        sftp_site = '<sftp-site>' 

        # Get sftp credentials
        username = '<your-sftp-username>'
        password = '<your-sftp-password>'
        host = '<your-sftp-host>'
        port = '<your-sftp-port>'
        
        # Create sftp connection
        transport = FastTransport((host,port))
        
        # Connect
        transport.connect(None,username,password)
        
        # Get SFTP Client
        sftp = paramiko.SFTPClient.from_transport(transport)

        files_to_load = []
        
        # The ./ will be replace by the sftp_site name in the s3 bucket same apply for search paths
        default_path = './'

        if request.get('search_paths'):
            
            search_paths = request['search_paths']

            search_paths.sort(reverse=True)

            for search_path in search_paths:
        
                files_to_load.extend(sftp_get_files(sftp_site, search_path, sftp))
            
        else: 

            files_to_load.extend(sftp_get_recursive(sftp_site, default_path, sftp))

        # Sort by modified date
        files_sorted = sorted(files_to_load, key = lambda f: f['file_sftp_date'])

        for file_to_load in files_sorted:

            with sftp.open(file_to_load['file_sftp_path'], "r") as f:
                f.prefetch()
            
                load_s3_file(file_to_load['file_s3_path'], f)

            print(f'[{datetime.now()}]: File ({file_to_load["file_s3_path"]}) - ({file_to_load["file_sftp_date"]}) loaded to s3')

    return {}

# Testing
if __name__ == "__main__": 

    request = {
        "search_paths": [
            "./FOLDER_1/",
            "./FOLDER_2/",
        ]

    }

    lambda_handler(request, None)