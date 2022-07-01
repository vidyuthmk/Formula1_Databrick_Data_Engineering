# Formula1_Databrick_Data_Engineering
The project Formula_1 is build on databricks in Azure platfomrs and is in production quality 
Project Details
Formula 1 data is Extracted From Ergast Api and the link to the database schema is available at http://ergast.com/images/ergast_db.png and also it is possible to download the csv file from http://ergast.com/mrd/

Git Hub branch infor Info
1. Master Branch 
The master bracnh consist of FULL LOAD solution architecture i.e the data is read every time and overwrites the data from ingestion to transform to presentation layer. this is not an ideal solution for huge data , then comes the second solution.

2. Formula 1 incremental load branch.
This solution consist of incremental load to the tranformed data i.e the initial data (cutout data) is loaded fully and the new data is compared and overwritten only on MATCHING records and modify data by cerating pratitioned data. In this project all the raw data from differnt file have to read and tranformed into PARQUET files and stored in DATA LAKE ADLS blob storage.


3. ELT branch
To make the solution more better , i have used delta lake service that sits top on the datalake storage and provide tonnes of optmization over the datalake , such as ACID TRANSATION , better data tranformation ability and so on. this ETL solution is complete solution where i have created formula1 PIPLINE over DATAFactory and monitored the jobs.

Azure Architecture.

![Screenshot 2022-07-01 at 10 11 32 PM](https://user-images.githubusercontent.com/10596580/176962450-69ed05a6-4646-46ad-80e5-cbbaada257fe.png)

I have followed the above architecture to build this project 

Security:
All the connection in configuration file are created hand handled by Azure Key Vault and created a secure connection between the datastorge and register the project

Project Informtion
There are 3 Main folders to consider in this project 
1. ingetion folder :
this folder contains files such as circuit_file,driver_file,race_file_result_file,pitstop,laptime,and qualifying file. this are in raw structure extracted from different file format and stored in delta table under processed folder 

2. Processed Folder
In this folder all the processed files are there such as Race_results,diver_standings,constructor_standing and calculated results 

3. presentation folder
this table consist of final processed data for data analysis and more.

DataFactory.

After all the setup was ready i have created a Azure datafactory service to create piplines to run this process by weekly and also added tumbling Triggers to schedules the jobs. 
