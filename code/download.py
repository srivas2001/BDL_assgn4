import subprocess
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import random
import ast
import os
import requests
import yaml
from tqdm import tqdm
def download_url(base_url,year,output_file,temp_dir,max_files):
    subprocess.run(["curl", "-L", "-o", output_file, base_url+str(year)]) #Gets the html file having all csv hyperlinks for the year
    with open(output_file, 'r') as file:
        data=file.read()
        #parse this html file
        #get the links
    parse_file = BeautifulSoup(data, 'html.parser') # We now parse the html file using BeautifulSoup
    csv_links_with_memory=[]
    for row in parse_file.find_all('tr')[2:]: # We skip the first two rows as they are not useful
         columns=row.find_all('td')# We get all the columns in the row
         if columns and columns[2].text.strip().endswith('M'): #We check if the third column has a value in MB
              csv_link = urljoin(base_url + str(year) + '/', columns[0].text.strip())
              memory = float(columns[2].text.strip().replace('M', '')) #We extract the memory value
              csv_links_with_memory.append((csv_link, memory))
    csv_links_above_45M = [link for link, memory in csv_links_with_memory if memory > 45][:max_files] #We filter the links with memory greater than 45MB
    os.makedirs(temp_dir, exist_ok=True)
    for link in csv_links_above_45M:
        res = requests.get(link) #We get the response from the link
        block_size = 1024
        total_size=int(res.headers.get('content-length', 0))
        if res.status_code == 200:  # If everything is okay
            filename = os.path.join(temp_dir, os.path.basename(link)) #We get the filename
            progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc=os.path.basename(link)) #initialise the progress bar
            with open(filename, 'wb') as csv_file:
                for data in res.iter_content(1024): #We write the content to the file
                    csv_file.write(data)
                    progress_bar.update(len(data))
            progress_bar.close()
def main():
    params = yaml.safe_load(open("params.yaml")) #Open the params.yaml file
    base_url=params["data_source"]["base_url"] # We take all the required parameters from this file
    year=params["data_source"]["year"]
    output_file=params["data_source"]["output"]
    temp_dir=params["data_source"]["temp_dir"]
    max_files=params["data_source"]["max_files"]
    download_url(base_url,year,output_file,temp_dir,max_files)
if __name__ == "__main__": #Executes the main function
    main()

