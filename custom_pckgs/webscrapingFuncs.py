import requests
import os

def page_requester(url):#Given a url, it returns page content
    attempt= 1
    page = requests.get(url, verify=True,headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0"})
    while(page.status_code != 200 and attempt < 3):
            print("Trying again.Request ended with status code", page.status_code )
            page = requests.get(url, verify=False,headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0"})
            attempt = attempt + 1
            
    if (page.status_code != 200):
        print("!!ERROR: Three unsuccessful attempts to get the page have been made, Response Code: ",page.status_code)
        
    return page

def download_file(url):#Given a url it downloads the file
    file_name = url.split("/")[-1]
    module_directory = os.path.dirname(os.path.abspath(__file__))#Gets path of the current module
    root_directory = os.path.dirname(module_directory)#Gets root path of the project 
    dataset_directory = os.path.join(root_directory, "dataset")
    os.makedirs(dataset_directory, exist_ok=True)#Creates dataset directory, where files will be stored
    file_path = os.path.join(dataset_directory, file_name)
    page = requests.get(url)

    with open(file_path, 'wb') as file:#Creates file and stores content
        file.write(page.content)