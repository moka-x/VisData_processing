查询ICD_code流程

1.安装postgresql

  创建用户
	
	CREATE USER postgres WITH PASSWORD 'postgres';

2.导入数据库文件

 (1)创建数据库
	
	CREATE DATABASE "icd_database"
  		WITH OWNER = "postgres"
      		

 (2)导入表数据
	
	psql  -U postgres -W -d icd_database -f 目录\icd.sql

3.执行icd_info.py 
