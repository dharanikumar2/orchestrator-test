from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import dotenv_values
import os
import csv
import re
from pathlib import Path

class project_creation:
    def __init__(self,storage_account_name,storage_account_key):
        self.file_list = 'file_list.tsv'
        self.__initialize_storage_account(storage_account_name,storage_account_key)

    def __initialize_storage_account(self,storage_account_name, storage_account_key):
        self.account_url = "{}://{}.dfs.core.windows.net".format("https", storage_account_name)
        self.sc = DataLakeServiceClient(account_url=self.account_url, credential=storage_account_key)
        
    def __create_file_system(self,container_name):
        file_systems = self.sc.list_file_systems()
        if(any(map(lambda file_sys:file_sys.name == container_name,file_systems))):
            raise Exception("Container already exists")
        file_system_client = self.sc.create_file_system(container_name)
        return file_system_client


    def __create_directory(self,file_system_client,directory_name):
        file_system_client.create_directory(directory_name)

    def __download_file_from_directory(self):
        file_system_client = self.sc.get_file_system_client(file_system="foxiepoc1")
        directory_client = file_system_client.get_directory_client("LoadApi")
        local_file = open(self.file_list,'wb')
        file_client = directory_client.get_file_client("views.tsv")
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        local_file.write(downloaded_bytes)
        local_file.close()
       
    def __validate_project_name(self,project_name):
            return (bool(re.match("^(?=.{3,63}$)[a-z0-9]+(-[a-z0-9]+)*$",project_name)))
    
    def __normalize(self,s):
        if s is not None:
            return s.strip().lower()
 
    def project_intialization(self,project_name,project_type):
        if(not self.__validate_project_name(project_name)):
            raise Exception('Project name should be in 3 to 63 Characters \n Starts With Letter or Number \nContains Letters, Numbers, and Dash (-)\nEvery Dash (-) Must Be Immediately Preceded and Followed by a Letter or Number')
        print("project_name: ",project_name)
        project_type = self.__normalize(project_type)
        if not(project_type=='parent' or project_type=='child'):
            raise Exception('Project type can only be parent or child')
        #creting container in ADL
        container = self.__create_file_system(project_name)
        # creating directory in the container
        self.__create_directory(container,"raw")
        self.__create_directory(container,"input")
        self.__create_directory(container,"vm")
        if project_type!='parent':
            self.__create_directory(container,"output")

        # self.__download_file_from_directory()
        with open(self.file_list, mode ='r') as file:
            #skipping header
            next(file,None)
            for lines in file:
                row = lines.split('\t')
                if(project_type=='parent' and self.__normalize(row[1])=='child'):
                    continue
                folder_name = self.__normalize(row[0])
                #creating folder for all child files in input directory 
                self.__create_directory(container,self.__normalize(row[2])+"/"+folder_name)
        # os.remove("file_list.tsv")
        return self.account_url+"/"+project_name