��ѯICD_code����

1.��װpostgresql

  �����û�
	
	CREATE USER postgres WITH PASSWORD 'postgres';

2.�������ݿ��ļ�

 (1)�������ݿ�
	
	CREATE DATABASE "icd_database"
  		WITH OWNER = "postgres"
      		

 (2)���������
	
	psql  -U postgres -W -d icd_database -f Ŀ¼\icd.sql

3.ִ��icd_info.py 
