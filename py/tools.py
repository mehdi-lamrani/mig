
import os
import gzip
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
from pyspark.sql.functions import udf
from pyspark.sql.types import *

_spark = None

def get_spark():
    global _spark
    if _spark is None:
        _spark = SparkSession.builder.getOrCreate()
    return _spark

def clear():
    _ = os.system('clear')

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
    spark = get_spark()
    for folder in FILES.FOLDERS:
        path = f"{FILES.BASE_PATH}/{folder}"
        dfs[folder] = spark.read.parquet(path)

# Function to retrieve a DataFrame by folder constant
def get_df(folder_name):
    if folder_name in dfs:
        return dfs[folder_name]
    else:
        raise ValueError(f"DataFrame for folder '{folder_name}' not found.")

# Function to print commands for accessing DataFrames
def get_all_df_cmd():
    for folder_name in dir(FILES):
        if not folder_name.startswith('__') and folder_name.isupper():
        # Get only attributes that are not methods or special variables
            folder_value = getattr(FILES, folder_name)
            if isinstance(folder_value, str):
                if not folder_value.startswith('/'):
                      print(f'{folder_value}_df = get_df(tools.FILES.{folder_name})')

# Load all the DataFrames
load_parquet_files()

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
    list_files(conf.REF_FILES.parquet)

class conf:
    class REF_FILES :
        parquet = '/data_enriched/ref/parquet/'
        csv = '/data_enriched/ref/csv/'
    class SFTP :
        user = 'datalab'
        pswd = 'Or0nget2@2*00'
        sondja = '172.21.14.254'

def write(df, path, name):
    df.write.parquet(path+name, mode="overwrite")

def gunzip(filepath):
    try:
        output_path = filepath.rsplit('.', 1)[0]  # Remove .gz extension
        with gzip.open(filepath, 'rb') as fin:
            with open(output_path, 'wb') as fout:
                fout.write(fin.read())
        return f"Extracted to {output_path}"
    except Exception as e:
        return f"Error: {str(e)}"

def sftp_download(remote_path, local_path, hostname=conf.SFTP.sondja, username= conf.SFTP.user, password=conf.SFTP.pswd):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('sftp_download.log'), logging.StreamHandler()])

    stats_logger = logging.getLogger('download_stats')
    stats_logger.setLevel(logging.INFO)
    stats_handler = logging.FileHandler('download_stats.log')
    stats_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    stats_logger.addHandler(stats_handler)

    start_time = time.time()
    local_path = os.path.join(local_path, os.path.basename(remote_path))

    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, username=username, password=password)
        with ssh.open_sftp() as sftp:
            file_size = sftp.stat(remote_path).st_size
            with tqdm(total=file_size, unit='B', unit_scale=True, desc=remote_path) as pbar:
                sftp.get(remote_path, local_path, callback=lambda bytes_transferred, total_bytes: pbar.update(bytes_transferred - pbar.n))

    end_time = time.time()
    duration = end_time - start_time
    avg_speed = file_size / duration / 1024 / 1024

    stats_logger.info(f"Download completed:  {local_path} Size: {file_size / 1024 / 1024:.2f} MB, Speed: {avg_speed:.2f} MB/s")
