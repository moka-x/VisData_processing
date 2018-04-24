from flask import request
import psycopg2
import json

db_name_set = {'mimic_data':{'mimic_database':'mimic_database','mimic_demo':'mimic_demo_rest'},
           'unc_data':{'agg_cohort_1_fake':'unc_data_1','agg_cohort_2_fake':'unc_data_2','agg_cohort_3_fake':'unc_data_3',
            'fake': 'unc_data_fake','aggregate_fake':'unc_data_aggregate_fake'}}
def db_name_str(name_str,db_type):
    db_str = db_name_set[db_type][name_str]
    return db_str

def regex(pattern_str):
    count = pattern_str.count('|')
    pattern_set = pattern_str.split('|')
    print(count == len(pattern_set) - 1)
    set_len = len(pattern_set)
    regex_match = "\|.*"
    regex_str = ""
    for i in range(0, set_len):
        regex_str += pattern_set[i]
        if 0 <= i < set_len-1:
            regex_str += regex_match
    

    regex_str = regex_str.upper()
    print(regex_str)
    return regex_str

def range_str(set,type):
    set_len = len(set)
    range_str = "("
    for i in range(0,set_len):
        if type == 'string':
            range_str += "'"+ str(set[i]) + "'"
        else:
            range_str += str(set[i])
        if 0 <= i < set_len-1:
            range_str += ","
    range_str += ")"
    return range_str
def patient_id_list(patient_info,event_info,db_type,db_name):
    data_len = len(patient_info)
    event_len =  len(event_info)
    patient = {}
    if data_len == 1:
        if db_type == 'mimic_data':
            if db_name == 'mimic_demo':
                pid = int(patient_info[0][1])
                gender = str(patient_info[0][2])
                birth_data = str(patient_info[0][3])
                # seq_string = str(patient_info[0][4])
                # patient = {"patient_id": pid, "gender": gender, "birth_data": birth_data, "seq_string": seq_string,"sequence": []}
                patient = {"patient_id": pid, "gender": gender, "birth_data": birth_data,"sequence": []}
            elif db_name == 'mimic_database':
                pid = int(patient_info[0][0])
                gender = str(patient_info[0][1])
                patient = {"patient_id": pid, "gender": gender,"sequence": []}
        elif db_type == 'unc_data':
            pid = int(patient_info[0][0])
            patient = {"patient_id": pid, "sequence": []}
        for i in range(0,event_len):
            if db_type == 'mimic_data':
                if db_name == 'mimic_demo':
                    event_code = str(event_info[i][1])
                    event_time = str(event_info[i][3])
                    event_type = str(event_info[i][4])
                elif db_name == 'mimic_database':
                    event_code = str(event_info[i][0])
                    event_time = str(event_info[i][2])
                    event_type = str(event_info[i][3])
            elif db_type == 'unc_data':
                event_code = str(event_info[i][0])
                event_time = str(event_info[i][2])
                event_type = str(event_info[i][3])
            patient['sequence'].append({"event_code":event_code, "event_time":event_time,"event_type":event_type})
    return patient

def select_sequence(pattern_str,patient_ids,event_types,sequence_len, db_name, db_type):
    # pattern_str = request.args.get('pattern_str')
    # patient_ids = request.args.get('patient_ids')
    # event_types = request.args.get('event_types')
    print('pattern_str' + pattern_str)
    print('patient_ids' + patient_ids)
    print('event_types' + event_types)
    print('sequence_len'+ sequence_len)
    patient_list = []
    patient_id_set = patient_ids.split('|')
    sequence_set = sequence_len.split('|')
    patient_id_str = range_str(patient_id_set,'integer')
    event_type_str = range_str(event_types.split('|'),'string')
    database_name = db_name_str(db_name,db_type)
    conn = psycopg2.connect(database= database_name, user="postgres", password="123456", host="202.120.188.26",port="5432")
    cur = conn.cursor()
    pattern_set = pattern_str.split('|')
    len_pattern = len(pattern_set)
    pattern_code_str = ""
    if pattern_str != '':
        if db_type == 'mimic_data':
            for i in range(0,len_pattern):
                pattern_code = ""
                with open('../data/item_label.json', 'r') as f1:
                    data_1 = json.load(f1)
                with open('../data/other_item.json', 'r') as f2:
                    data_2 = json.load(f2)
                with open('../data/lab_label.json', 'r') as f3:
                    data_3 = json.load(f3)
                    if pattern_code == "":
                        print(str(pattern_set[i]))
                        for j in range(len(data_1)):
                            if 'LABEL' in data_1[j].keys():
                                if data_1[j]['ITEMID'] == str(pattern_set[i]):
                                    pattern_code = data_1[j]['ITEMID']
                        if pattern_code == "":
                            for j in range(len(data_2)):
                                if 'ITEMID' in data_2[j].keys():
                                    if data_2[j]['ITEMID'] == str(pattern_set[i]):
                                        pattern_code = data_2[j]['ITEMID']
                            if pattern_code == "":
                                for j in range(len(data_3)):
                                    if 'ITEMID' in data_2[j].keys():
                                        if data_3[j]['ITEMID'] == str(pattern_set[i]):
                                            pattern_code = data_3[j]['ITEMID']
                pattern_code_str += pattern_code
                if 0 <= i < len_pattern-1:
                    pattern_code_str += "|"
        else:
            pattern_code_str = pattern_str
    regex_str = regex(pattern_code_str)
    result_info = []
    if pattern_str == '':
        if patient_ids == '':
            if event_types == '':
                cur.execute("select pid from patient")
                rows = cur.fetchall()
                patient_len = len(rows)
                print patient_len
                print "length"

                for i in range(0, patient_len):
                    if sequence_len == '':
                        cur.execute("select * from patient where pid = " + str(rows[i][0]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from patient_event_connect NATURAL join event where pid = " + str(
                            rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|')+1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = " + str(rows[i][0]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from patient_event_connect NATURAL join event where pid = " + str(
                                rows[i][0]) + " ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})

                conn.close()
                print('query successfully')
                for j in range(0,len(result_info)):
                    patient_list.append(patient_id_list(result_info[j]['patient'],result_info[j]['event'], db_type, db_name))

            else:
                cur.execute("select * into temp_query from patient_event_connect NATURAL join event where event.event_type in " + str(event_type_str) + " ORDER BY event_time")
                cur.execute("select distinct pid from temp_query")
                rows = cur.fetchall()
                patient_len = len(rows)
                print patient_len
                print "length"
                for i in range(0, patient_len):
                    if sequence_len == '':
                        cur.execute("select * from patient where pid = " + str(rows[i][0]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|') + 1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = "+ str(rows[i][0]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from temp_query where pid = "+str(rows[i][0])+" ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})
                cur.execute("drop table temp_query")
                conn.commit()
                conn.close()
                print('query successfully')
                for j in range(0, len(result_info)):
                    patient_list.append(patient_id_list(result_info[j]['patient'], result_info[j]['event'], db_type, db_name))
        else:
            if event_types == '':
                cur.execute("select * into temp_query from patient_event_connect NATURAL join event where pid in " + str(
                    patient_id_str) + " ORDER BY event_time")
                patient_len = len(patient_id_set)
                print patient_len
                print "length"
                for i in range(0,patient_len):
                    if sequence_len == '':
                        cur.execute("select * from patient where pid = " + str(patient_id_set[i]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|') + 1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = " + str(patient_id_set[i]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})
                cur.execute("drop table temp_query")
                conn.commit()
                conn.close()
                print('query successfully')
                for j in range(0, len(result_info)):
                    patient_list.append(
                        patient_id_list(result_info[j]['patient'], result_info[j]['event'], db_type, db_name))
            else:
                cur.execute("select * into temp_query from patient_event_connect NATURAL join event where event.event_type in "
                            + str(event_type_str) + " and pid in " + str(patient_id_str))
                cur.execute("select distinct pid from temp_query")
                rows = cur.fetchall()
                patient_len = len(rows)
                print patient_len
                print "length"

                for i in range(0,patient_len):
                    if sequence_len == '':
                        cur.execute("select * from patient where pid = " + str(rows[i][0]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|') + 1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = " + str(rows[i][0]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})
                cur.execute("drop table temp_query")
                conn.commit()
                conn.close()
                print('query successfully')
                for j in range(0, len(result_info)):
                    patient_list.append(
                        patient_id_list(result_info[j]['patient'], result_info[j]['event'], db_type, db_name))

    else:
        print "yes"
        if patient_ids == '':
            if event_types == '':
                cur.execute("select pid from patient where upper(code_string) ~ '" + str(regex_str) + "'")
                rows = cur.fetchall()
                patient_len = len(rows)
                patient_range = '('
                for m in range(0, patient_len):
                    patient_range += str(rows[m][0])
                    if 0 <= m < patient_len - 1:
                        patient_range += ","
                patient_range += ')'
                print patient_len
                print "length"
                cur.execute("select * into temp_query from patient_event_connect NATURAL join event where pid in "
                            + str(patient_range) + " ORDER BY event_time")
                for i in range(0,patient_len):
                    if sequence_len == '':
                        print i
                        cur.execute("select * from patient where pid = " + str(rows[i][0]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|') + 1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = "+ str(rows[i][0]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})
                cur.execute("drop table temp_query")
                conn.commit()
                conn.close()
                print('query successfully')
                for j in range(0, len(result_info)):
                    patient_list.append(
                        patient_id_list(result_info[j]['patient'], result_info[j]['event'], db_type, db_name))
            else:
                cur.execute("select * into temp_query from patient_event_connect NATURAL join event where event.event_type in "+str(event_type_str)
                            +" and pid in (select pid from patient where code_string ~ '" + str(regex_str) + "')")
                cur.execute("select distinct pid from temp_query")
                rows = cur.fetchall()
                patient_len = len(rows)
                print patient_len
                print "length"

                for i in range(0,patient_len):
                    if sequence_len == '':
                        cur.execute("select * from patient where pid = " + str(rows[i][0]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|') + 1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = "+ str(rows[i][0]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})
                cur.execute("drop table temp_query")
                conn.commit()
                conn.close()
                print('query successfully')
                for j in range(0, len(result_info)):
                    patient_list.append(
                        patient_id_list(result_info[j]['patient'], result_info[j]['event'], db_type, db_name))
        else:
            if event_types == '':
                cur.execute("select pid from patient where code_string ~ '" + str(regex_str) + "' and pid in " + str(patient_id_str))
                rows = cur.fetchall()
                patient_len = len(rows)
                print patient_len
                print "length"
                patient_range = '('
                for m in range(0, patient_len):
                    patient_range += str(rows[m][0])
                    if 0 <= m < patient_len - 1:
                        patient_range += ","
                patient_range += ')'
                cur.execute("select * into temp_query from patient_event_connect NATURAL join event where pid in "
                            + str(patient_range) + " ORDER BY event_time")
                for i in range(0, patient_len):
                    if sequence_len == '':
                        cur.execute("select * from patient where pid = " + str(rows[i][0]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|') + 1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = " + str(rows[i][0]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})
                cur.execute("drop table temp_query")
                conn.commit()
                conn.close()
                print('query successfully')
                for j in range(0, len(result_info)):
                    patient_list.append(
                        patient_id_list(result_info[j]['patient'], result_info[j]['event'], db_type, db_name))
            else:
                cur.execute("select * into temp_query from patient_event_connect NATURAL join event where event.event_type in " + str(event_type_str)
                            + " and pid in (select pid from patient where code_string ~ '" + str(regex_str) + "' and pid in " + str(patient_id_str)+")")
                cur.execute("select distinct pid from temp_query")
                rows = cur.fetchall()
                patient_len = len(rows)
                print patient_len
                print "length"
                for i in range(0, patient_len):
                    if sequence_len == '':
                        cur.execute("select * from patient where pid = " + str(rows[i][0]))
                        patient_info = cur.fetchall()
                        cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                        event_info = cur.fetchall()
                        result_info.append({"event": event_info, "patient": patient_info})
                    else:
                        cur.execute("select code_string from patient where pid = " + str(rows[i][0]))
                        result = cur.fetchall()
                        result_num = str(result[0][0]).count('|') + 1
                        print result_num
                        if int(sequence_set[0]) <= int(result_num) <= int(sequence_set[1]):
                            cur.execute("select * from patient where pid = " + str(rows[i][0]))
                            patient_info = cur.fetchall()
                            cur.execute("select * from temp_query where pid = " + str(rows[i][0]) + " ORDER BY event_time")
                            event_info = cur.fetchall()
                            result_info.append({"event": event_info, "patient": patient_info})
                cur.execute("drop table temp_query")
                conn.commit()
                conn.close()
                print('query successfully')
                for j in range(0, len(result_info)):
                    patient_list.append(patient_id_list(result_info[j]['patient'], result_info[j]['event'], db_type, db_name))
    return patient_list

if __name__ == '__main__':
    pj1 = select_sequence('Dextrose 5%|Fresh Frozen Plasma','','','0|1000','mimic_demo','mimic_data')
    # pj2 = select_sequence('', '', '','', 'mimic_demo', 'mimic_data')
    # pj3 = select_sequence('', '10013|10017', '', '','mimic_demo', 'mimic_data')
    # pj4 = select_sequence('', '', 'LAB|INPUT','', 'mimic_demo', 'mimic_data')
    # pj5 = select_sequence('', '10013', 'LAB|INPUT', '','mimic_demo', 'mimic_data')
    # pj6 = select_sequence('Dextrose 5%|Fresh Frozen Plasma', '', 'LAB', '','mimic_demo', 'mimic_data')
    # pj7 = select_sequence('Dextrose 5%|Fresh Frozen Plasma', '40177', 'LAB', '','mimic_demo', 'mimic_data')
    # pj8 = select_sequence('SPECIAL', '', '', '200|300','agg_cohort_1_fake', 'unc_data')
    # pj9 = select_sequence('', '', '','', 'agg_cohort_2_fake', 'unc_data')
    # pj10 = select_sequence('', '2|3', 'B|C', '','agg_cohort_2_fake', 'unc_data')
    # pj11 = select_sequence('', '2|3', '', 'agg_cohort_2_fake', 'unc_data')
    # pj12 = select_sequence('SPECIAL|29', '4023', '', 'agg_cohort_2_fake', 'unc_data')
    # pj13 = select_sequence('', '', 'B|C','', 'agg_cohort_2_fake', 'unc_data')
    # pj14 = select_sequence('SPECIAL|29', '4023', 'B|C', '0|500','agg_cohort_2_fake', 'unc_data')
    # pj15 = select_sequence('Dextrose 5%|Fresh Frozen Plasma', '40177', 'LAB','', 'mimic_database', 'mimic_data')
    print json.dumps(pj1)