import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import csv

#maxInt = int(sys.maxsize)
maxInt = 999999999
print(maxInt)
decrement = True

while decrement:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt / 10)
        decrement = True

def database_create(db_type,db_name):
    if db_type == 'unc_data':
        conn = psycopg2.connect(user="postgres", password="123456", host="127.0.0.1",
                                port="5432")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute('create database ' + db_name)
        conn.commit()
        conn.close()
        print('create database successfully')
        conn = psycopg2.connect(database=db_name, user="postgres", password="123456", host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        cur.execute('create table patient (pid integer not null)')
        cur.execute('create table event (event_type varchar not null,event_code varchar not null)')
        cur.execute(
            'create table patient_event_connect (pid integer not null, event_time varchar not null,event_code varchar not null)')
        conn.commit()
    elif db_type == 'mimic_data':
        conn = psycopg2.connect(database=db_name, user="postgres", password="123456", host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        cur.execute('create table patient (pid integer not null,gender varchar not null,code_string varchar not null)')
        cur.execute('create table event (pid integer not null,event_time varchar not null,event_code varchar not null,event_type varchar not null)')
        cur.execute(
            'create table patient_event_connect (event_code varchar not null,pid integer not null, event_time varchar not null)')
        conn.commit()

def database_precess(filepath,filetype,db_name,db_type):
    csvfile = open(filepath, 'r')
    reader = csv.reader(csvfile)
    patient_key = {}
    patient_data = []
    event_key={}
    event_data=[]
    connect_data=[]
    if db_type == 'unc_data':
        for row in reader:
            if not(row[0] in patient_key.keys()):
                patient_key[row[0]] = ''
                patient_data.append(row[0])
            if not(row[3] in event_key.keys()):
                event_key[row[3]] = ''
                event_data.append({"event_type":str(row[2]),"event_code":str(row[3])})
            connect_data.append({"pid":row[0],"event_time":row[1],"event_code":row[3]})
        conn = psycopg2.connect(database=db_name, user="postgres", password="123456", host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        for i in range(0,len(patient_data)):
            cur.execute('insert into patient(pid) values('+str(patient_data[i])+')')
        print('patient_table process successfully')
        for j in range(0,len(event_data)):
            cur.execute("insert into event(event_type,event_code) values('" + str(event_data[j]['event_type']) + "','" +str(event_data[j]['event_code'])+"')")
        print('event_table process successfully')
        for m in range(0, len(connect_data)):
            cur.execute("insert into patient_event_connect(pid,event_time,event_code) values(" + str(connect_data[m]['pid']) + ",'"+ str(connect_data[m]['event_time']) + "','"+ str(
                connect_data[m]['event_code']) + "')")
        print('connect_table process successfully')
        conn.commit()
    elif db_type == 'mimic_data':
        conn = psycopg2.connect(database=db_name, user="postgres", password="123456", host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        count = 0
        for row in reader:
            if filetype == 'patient':
                if row[1] == '':
                    if len(patient_data) != 0 and row[0] != '':
                        patient_data[last_row_len]['code_string'] += row[0]
                else:
                    if not(row[0] in patient_key.keys()):
                        patient_key[row[0]] = ''
                        patient_data.append({'pid':row[0],'gender':row[1],'code_string':row[2]})
                        last_row_len = len(patient_data)-1
            elif filetype == 'event':
                #if not(row[3] in event_key.keys()):
                #    event_key[row[3]] = ''
                    event_data.append({"pid":str(row[0]),"event_time":row[1],"event_code":str(row[2]),"event_type":str(row[3])})
            elif filetype == 'connect':
                connect_data.append({"event_code":row[0],"pid":row[1],"event_time":row[2]})
            elif filetype == 'count':
                count +=1
        if len(patient_data) != 0:
            for i in range(0,len(patient_data)):
                cur.execute("insert into patient(pid,gender,code_string) values("+str(patient_data[i]['pid'])+",'"+str(patient_data[i]['gender'])+"','"+str(patient_data[i]['code_string'])+"')")
            print('patient_table process successfully')
        if len(event_data) != 0:
            for j in range(0,len(event_data)):
                cur.execute("insert into event(pid,event_time,event_code,event_type) values("+str(event_data[j]['pid'])+",'" + str(event_data[j]['event_time'])+"','" + str(event_data[j]['event_code']) + "','" +str(event_data[j]['event_type'])+"')")
            print('event_table process successfully')
        if len(connect_data) != 0:
            for m in range(0, len(connect_data)):
                cur.execute("insert into patient_event_connect(pid,event_time,event_code) values(" + str(connect_data[m]['pid']) + ",'"+ str(connect_data[m]['event_time']) + "','"+ str(connect_data[m]['event_code']) + "')")
            print('connect_table process successfully')
        conn.commit()
        return count
def add_code_string(db_name):
    conn = psycopg2.connect(database=db_name, user="postgres", password="123456", host="127.0.0.1",
                            port="5432")
    cur = conn.cursor()
    cur.execute('select pid from patient')
    pid_set = cur.fetchall()
    cur.execute('alter table patient add code_string varchar')
    print('pid'+str(len(pid_set)))
    for i in range(0,len(pid_set)):
        pid = str(pid_set[i][0])
        cur.execute('select * from patient_event_connect where pid ='+ pid + ' ORDER BY event_time,event_code')
        event_code = cur.fetchall()
        code_string = ""
        print('event_code'+str(len(event_code))+' pid'+ str(pid))
        for j in range(0,len(event_code)):
            code_string += str(event_code[j][2])
            if 0 <= j < len(event_code)-1:
                code_string += "|"
       # print(code_string)
        cur.execute("update patient set code_string = '"+ code_string+"'where pid ="+ pid)
    conn.commit()
    print(db_name +'finished')

def event_info(db_name):
    if db_name == 'mimic_demo_rest' or db_name == 'mimic_database':
        event_infos = []
        conn = psycopg2.connect(database=db_name, user="postgres", password="123456", host="127.0.0.1",port="5432")
        cur = conn.cursor()
        cur.execute('select distinct event_code,event_type from event order by event_code')
        result = cur.fetchall()
        for i in range(0, len(result)):
            event_infos.append({'event_code': str(result[i][0]), 'event_type': str(result[i][1])})
            label_code = ""
            with open('./item_label.json', 'r') as f1:
                data_1 = json.load(f1)
            with open('./other_item.json', 'r') as f2:
                data_2 = json.load(f2)
            with open('./lab_label.json', 'r') as f3:
                data_3 = json.load(f3)
                if label_code == "":
                    for j in range(len(data_1)):
                        if 'ITEMID' in data_1[j].keys():
                            if data_1[j]['ITEMID'] == str(result[i][0]):
                               label_code = data_1[j]['LABEL']
                    if label_code == "":
                        for j in range(len(data_2)):
                            if 'ITEMID' in data_2[j].keys():
                                if data_2[j]['ITEMID'] == str(result[i][0]):
                                    label_code = data_2[j]['ITEMID']
                        if label_code == "":
                            for j in range(len(data_3)):
                                if 'ITEMID' in data_3[j].keys():
                                    if data_3[j]['ITEMID'] == str(result[i][0]):
                                        label_code = data_3[j]['LABEL']
            event_infos[len(event_infos)-1]['event_label'] = str(label_code)
        cur.execute('create table event_info_table(event_code varchar not null,event_type varchar not null,event_label varchar not null)')
        print(len(event_infos))
        for j in range(0, len(event_infos)):
            print('process '+ str(j))
            cur.execute("insert into event_info_table(event_code,event_type,event_label) values('"+event_infos[j]['event_code'].replace("'", "''")+"','"+event_infos[j]['event_type'].replace("'", "''")+"','"+event_infos[j]['event_label'].replace("'", "''")+"')")
        conn.commit()

if __name__ == '__main__':
    #database_precess('./data/agg_cohort_3_fake.csv','','unc_data_3','unc_data')
    #database_precess('./data/agg_cohort_1_fake.csv','', 'unc_data_1','unc_data')
    #database_precess('./data/agg_cohort_2_fake.csv','', 'unc_data_2','unc_data')
    #database_precess('./data/aggregate_fake.csv','', 'unc_data_aggregate_fake','unc_data')
    #database_precess('./data/fake.csv','', 'unc_data_fake','unc_data')
    #add_code_string('unc_data_1')
    #add_code_string('unc_data_2')
    #add_code_string('unc_data_3')
    #add_code_string('unc_data_aggregate_fake')
    #add_code_string('unc_data_fake')
    #event_info('mimic_demo_rest')

    #database_create('mimic_data','mimic_database')
    count1=0
    count2 = 0
    for i in range(0,200):
        if 0<=i<10:
            str_len = '00'+ str(i)
        elif 10<=i<100:
            str_len = '0'+str(i)
        else:
            str_len = str(i)
        print i
        #database_precess('..\\'+'rest_api\mimic_data\mimic_database\patient_table\part-00'+str_len+'-956aaed2-f457-4788-adc7-d391fe58980c-c000.csv','patient','mimic_database','mimic_data')
        #database_precess('..\\'+'rest_api\mimic_data\mimic_database\event_table\part-00'+str_len+'-8cedbb32-d06e-4f16-8a5b-45291011cc01-c000.csv','event','mimic_database','mimic_data')
        #database_precess('..\\'+'rest_api\mimic_data\mimic_database\connect\part-00'+str_len+'-d4621a0f-553c-4363-aa33-097d22d13539-c000.csv','connect','mimic_database','mimic_data')
        count1 += database_precess('..\\'+'rest_api\mimic_data\mimic_database\connect\part-00'+str_len+'-d4621a0f-553c-4363-aa33-097d22d13539-c000.csv','count','mimic_database','mimic_data')
        count2 += database_precess('..\\'+'rest_api\mimic_data\mimic_database\event_table\part-00'+str_len+'-8cedbb32-d06e-4f16-8a5b-45291011cc01-c000.csv','count','mimic_database','mimic_data')

    print count1,count2
    #event_info('mimic_database')
