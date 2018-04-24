The Process of Building a Postgresql Database:

step 1: download and setup postgresql environment

Add user in postgresql with:
	
username:postgres 
password:123456

step 2: run the code "database_process.py"

modify the parameters of three functions in main:

(1)database_create('unc_data',<db_name>) :the function to create database

eg.database_create('unc_data','unc_data_1')

//parameter 1: no need to change
//parameter 2:<db_name> the database name that you want to create in postgresql

(2)database_process(<file_path>,'',<db_name>,'unc_data')

the function to create tables(event,patient,patient_event_connect) and insert the data into tables.

eg: database_process('./data/agg_cohort_1_fake.csv','','unc_data_1','unc_data')

//parameter 1: <file_path> the filepath of csv file 
//parameter 2: no need to change,set as null,it's other dataset's parameter.
//parameter 3:<db_name> the database name you create in step 2(1)
//parameter 4: no need to change,it's database_type

(3)add_code_string(<db_name>)

the function to add sequence data in patient table for every patient.

sequence data is the string of events order by time.

eg.add_code_string('unc_data_1')

//parameter 1:<db_name> the database name you create in step 2(1)
