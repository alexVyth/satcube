"""Orchestrate the distributed pipeline process."""
import argparse
import os
import re
import subprocess
import time
import zipfile
from typing import List

import ray
from fmask.cmdline import sentinel2Stacked
from sentinelsat import SentinelAPI

start = time.time()

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('-u', '--username', help='scihub.copernicus.eu/dhus username')
parser.add_argument('-p', '--password', help='scihub.copernicus.eu/dhus password')
parser.add_argument('-i', '--ids', nargs='+', help='list of product IDs')
args = parser.parse_args()

product_ids = args.ids
username = args.username
password = args.password


@ray.remote
def download(user: str, passw: str, prod_id: List) -> str:
    """Download sentinel2 images using sentinalsat package."""
    api = SentinelAPI(user, passw, 'https://scihub.copernicus.eu/dhus')
    product_dict = api.download(prod_id, directory_path='./data/compressed')
    zip_path = product_dict['path']
    return zip_path


@ray.remote
def extract(zip_path: str) -> str:
    """Extract sentinel2 images."""
    extract_dir = './data/l1c'
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        extracted = zip_ref.namelist()
    extracted_file = os.path.join(extract_dir, extracted[0])
    return extracted_file


@ray.remote
def atmospheric_correction(safe_path: str) -> None:
    """
    Process the given L1C images using sen2cor 2.5.5 to generate
    L2A level images.
    """
    # sen2cor path
    sen2cor_path = '../sen2cor'
    sen2cor_bin = os.path.join(sen2cor_path, 'bin', 'L2A_Process')

    # output path
    l2a_path = re.sub('S2A_MSIL1C_', 'S2A_MSIL2A_', safe_path)
    l2a_path = re.sub('l1c', 'l2a', l2a_path)
    if not os.path.exists(l2a_path):
        options = f'--output_dir {l2a_path}'
        subprocess.call(f'{sen2cor_bin} {options} {safe_path} ', shell=True)
    return l2a_path


@ray.remote
def mask_cloud(safe_path: str) -> None:
    """
    Detect and filter areas covered by clouds using fmask v3 algorithm.
    """
    target_dir = re.sub('l1c', 'cloud_masks', safe_path)
    target_dir = re.sub('.SAFE', '.tif', target_dir)[:-1]
    sentinel2Stacked.mainRoutine(["--safedir", safe_path, "-o", target_dir])
    return target_dir


ray.init(num_cpus=4)

zip_dirs = []
for product_id in product_ids:
    zip_dir = download.remote(username, password, product_id)
    zip_dirs.append(zip_dir)

l1c_dirs = []
for compressed_file in ray.wait(zip_dirs):
    l1c_dir = extract.remote(compressed_file[0])
    l1c_dirs.append(l1c_dir)

l2a_dirs = []
for l1c_dir in ray.wait(l1c_dirs):
    l2a_dir = atmospheric_correction.remote(l1c_dir[0])
    l2a_dirs.append(l2a_dir)

cloud_mask_dirs = []
for l1c_dir in ray.wait(l1c_dirs):
    cloud_mask_dir = mask_cloud.remote(l1c_dir[0])
    cloud_mask_dirs.append(cloud_mask_dir)

print(ray.get(cloud_mask_dirs))

print(f'\nFinished in {time.time() - start:.1f} seconds.')
