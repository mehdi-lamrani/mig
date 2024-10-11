import os
from pyspark.sql import SparkSession
_spark = None

def get_spark():
    global _spark
    if _spark is None:
        _spark = SparkSession.builder.getOrCreate()
    return _spark

def clear():
    _ = os.system('clear')

def load(filename, sep=',', header=True):
    spark=get_spark()
    file_extension = filename.split('.')[-1].lower()

    if file_extension == 'csv':
        return spark.read.option("header", str(header)).option("sep", sep).csv(filename)
    elif file_extension == 'parquet':
        return spark.read.parquet(filename)

def list_files(folder_path):
    for item in os.listdir(folder_path):
            print(item)
            
def list_paths():
    print(conf.REF_FILES.parquet)

class conf:
    class REF_FILES :
        parquet = '/data_enriched/ref/parquet/'
        csv = '/data_enriched/ref/csv/'
    class SFTP :
        user = 'datalab'
        pswd = 'Or0nget2@2*00'
        sondja = '172.21.14.254'

class FILES:
    BASE_PATH = "/data_enriched/ref/parquet"
    CONTRACTS = "contracts"
    CUSTOMERS = "customers"
    LOCATIONS = "locations"
    SITES = "sites"
    TELCO_FILES = "telco_files"

    FOLDERS = [CONTRACTS, CUSTOMERS, LOCATIONS, SITES, TELCO_FILES]

dfs = {}

# Function to load the DataFrames from each folder
def load_parquet_files():
    global dfs
    for folder in FILES.FOLDERS:
        path = f"{FILES.BASE_PATH}/{folder}"
        dfs[folder] = spark.read.parquet(path)

# Function to print commands for accessing DataFrames
def get_all_df_cmd():
    for folder_name in dir(FILES):
        # Get only attributes that are not methods or special variables
        if not folder_name.startswith('__') and folder_name.isupper():
            folder_value = getattr(FILES, folder_name)
            print(f'{folder_value}_df = get_df(tools.FILES.{folder_name})')

# Load all the DataFrames
load_parquet_files()

# Now you can get individual DataFrames using the constants from the FILES class

customers_df = tools.get_df(tools.FILES.CUSTOMERS))

# Function to retrieve a DataFrame by folder constant
def get_df(folder_name):
    if folder_name in dfs:
        return dfs[folder_name]
    else:
        raise ValueError(f"DataFrame for folder '{folder_name}' not found.")

def write(df, path, name):
    df.write.parquet(path+name, mode="overwrite")

from pyspark.sql.functions import year, month

df=load(conf.REF_FILES.parquet+'telco_files')
files = df.filter(year("date") == 2023).filter(month("date") == 5).select("filepath").limit(3)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import gzip
import os

spark = SparkSession.builder.appName("GzipExtractor").getOrCreate()

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def unzip(filepath):
    try:
        output_path = filepath.rsplit('.', 1)[0]  # Remove .gz extension
        with gzip.open(filepath, 'rb') as in_file:
            with open(output_path, 'wb') as out_file:
                out_file.write(in_file.read())
        return f"Extracted to {output_path}"
    except Exception as e:
        return f"Error: {str(e)}"

result_df = filepath_df.withColumn("extraction_result", extract_gzip_to_disk("filepath"))

result_df.show(truncate=False)

import paramiko # type: ignore
import os
from pyspark.sql.functions import year, month

class SFTP :
        user = datalab
        pswd = Or0nget2@2*00
        sondja = '172.21.14.254'

def get_files(df):
    # Setup SFTP client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(conf.SFTP.sondja, username=conf.SFTP.user, password=conf.SFTP.pswd)
    sftp = ssh.open_sftp()

    # Download files
    for row in df.collect():
        remote_path = row['filepath']
        local_path = os.path.join(local_dir, os.path.basename(remote_path))
        sftp.get(remote_path, local_path)
        print(f"Downloaded {remote_path} to {local_path}")

    # Close connections
    sftp.close()
    ssh.close()
#######################################################

import paramiko # type: ignore
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def download_file(sftp, remote_path, local_folder):
    local_path = os.path.join(local_folder, os.path.basename(remote_path))
    file_size = sftp.stat(remote_path).st_size
    with tqdm(total=file_size, unit='B', unit_scale=True, desc=os.path.basename(remote_path)) as pbar:
        sftp.get(remote_path, local_path, callback=lambda _, y: pbar.update(y - pbar.n))
    return file_size
""""
def sftp_parallel_download(remote_files, local_folder, max_workers=5,hostname='172.21.14.254', username='datalab', password='Or0nget2@2*00')):
    total_size = 0
    start_time = time.time()

    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, username=username, password=password)
        with ssh.open_sftp() as sftp:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(download_file, sftp, remote_file, local_folder)
                           for remote_file in remote_files]
                for future in as_completed(futures):
                    total_size += future.result()

    duration = time.time() - start_time
    avg_speed = total_size / duration / 1024 / 1024  # in MB/s

    print(f"\nAll downloads completed.")
    print(f"Total data transferred: {total_size / 1024 / 1024:.2f} MB")
    print(f"Average download speed: {avg_speed:.2f} MB/s")

"""
#######
remote_files = ['/cragz/3/06/2023/RL_CRA_11062023_00000000000000.csv.gz', '/cragz/2/06/2023/RL_CRA_10062023_00000000000000.csv.gz', '/cragz/6/06/2023/RL_CRA_06062023_00000000000000.csv.gz']

sftp_parallel_download(remote_files,'/data_enriched/data',max_workers=3)


# Usage:
# sftp_may_2023_files(file_df, 'sftp.example.com', 'username', 'password', '/path/to/local/directory')
# Usage:
# may_2023_data = load_may_2023_files(file_df)
""""
+----------+----------------+-------------------------------------+------------------------------------------------------+
|date      |folder          |file                                 |filepath                                              |
+----------+----------------+-------------------------------------+------------------------------------------------------+
|2023-06-22|/cragz/6/06/2023|RL_CRA_22062023_00000000000000.csv.gz|/cragz/6/06/2023/RL_CRA_11062023_00000000000000.csv.gz|
|2023-06-11|/cragz/3/06/2023|RL_CRA_11062023_00000000000000.csv.gz|/cragz/3/06/2023/RL_CRA_10062023_00000000000000.csv.gz|
|2023-06-10|/cragz/2/06/2023|RL_CRA_10062023_00000000000000.csv.gz|/cragz/2/06/2023/RL_CRA_06062023_00000000000000.csv.gz|
|2023-06-06|/cragz/6/06/2023|RL_CRA_06062023_00000000000000.csv.gz|/cragz/6/06/2023/RL_CRA_05062023_00000000000000.csv.gz|

# Usage:
# print_files_in_folder("/path/to/your/folder")
"""
