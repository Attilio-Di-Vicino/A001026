import os
import boto3
from netCDF4 import Dataset
import requests

"""
Script to download a WRF NetCDF (.nc) file from a remote URL, extract each variable 
into a separate NetCDF4 (.nc4) file, and upload them to an AWS S3 bucket.

Steps:
1. Download the smallest WRF file from the data.meteo.unip/openda/ archive.
2. Open the NetCDF file and separate each variable into its own NetCDF4 file.
3. For each variable, create an S3 object with the path: bucket/filename/varname/varname.nc4
4. Upload all variable files to the specified S3 bucket using the NETCDF4 format.
"""

# Config AWS
AWS_REGION = "us-west-2"
S3_BUCKET_NAME = "cloudcomputing-nctf4"
LOCAL_DIR = "./tmp" 

session = boto3.Session(
    # Enter your keys
)

# Connect to the S3 services
s3 = session.client("s3")

def download_file(url, local_dir="./"):
    filename = os.path.basename(url)
    local_path = os.path.join(local_dir, filename)
    print(f"Scarico da URL: {url}")
    response = requests.get(url)
    response.raise_for_status()
    with open(local_path, 'wb') as f:
        f.write(response.content)
    print(f"File salvato: {local_path}")
    return local_path

def extract_and_save_variables(nc_path, base_filename):
    dataset = Dataset(nc_path, 'r')
    output_files = []
    for var_name in dataset.variables:
        var = dataset.variables[var_name]
        var_data = var[:]
        # New file NetCDF only for this variable
        output_path = os.path.join(LOCAL_DIR, f"{var_name}.nc4")
        new_ds = Dataset(output_path, 'w', format='NETCDF4')
        # Copy the dimension used for the variable
        for dim_name in var.dimensions:
            new_ds.createDimension(dim_name, len(dataset.dimensions[dim_name]))
        # Create the variable and write the data
        new_var = new_ds.createVariable(var_name, var.datatype, var.dimensions)
        new_var[:] = var_data
        new_ds.close()
        output_files.append((var_name, output_path))
        print(f"Variable '{var_name}' saved in: {output_path}")
    dataset.close()
    return output_files

def upload_to_s3(s3_client, bucket, base_filename, files):
    for var_name, path in files:
        s3_key = f"{base_filename}/{var_name}/{var_name}.nc4"
        s3_client.upload_file(path, bucket, s3_key)
        print(f"Upload on S3: s3://{bucket}/{s3_key}")
        os.remove(path)

def main(nc_local_path):
    base_filename = os.path.basename(nc_local_path)
    # Ceate the bucket if it does not exist
    try:
        s3.head_bucket(Bucket=S3_BUCKET_NAME)
    except:
        s3.create_bucket(Bucket=S3_BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': AWS_REGION})
    files = extract_and_save_variables(nc_local_path, base_filename)
    upload_to_s3(s3, S3_BUCKET_NAME, base_filename, files)

if __name__ == "__main__":
    test_url = "http://193.205.230.6/files/wrf5/d01/archive/2025/04/22/wrf5_d01_20250422Z0000.nc"
    local_file = download_file(test_url)
    main(local_file)