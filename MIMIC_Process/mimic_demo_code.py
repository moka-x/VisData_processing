import csv
import json

def patient_file_read(filepath, sequence_data):
    csvfile = open(filepath, 'r')
    reader = csv.DictReader(csvfile)
    middle = {}
    for row in reader:
        seq_size = len(sequence_data)
        if seq_size > 0:
            for m in range(0, seq_size):
                patient_id = sequence_data[m]['HADM_ID']
                middle[str(patient_id)] = m
            if not (row['HADM_ID'] in middle.keys()):
                sequence_data.append({"HADM_ID": row['HADM_ID'],"ADMISSIONS_EVENT": []})
                size = len(sequence_data)
                flag_1 = size-1
                flag_2 = 0
                sequence_data[size-1]['ADMISSIONS_EVENT'].append({"SUBJECT_ID": row['SUBJECT_ID'], "SEQUENCE": {}})
            else:
                index = middle[row['HADM_ID']]
                sequence_data[index]['ADMISSIONS_EVENT'].append({"SUBJECT_ID": row['SUBJECT_ID'], "SEQUENCE": {}})
                event_size = len(sequence_data[index]['ADMISSIONS_EVENT'])
                flag_1 = index
                flag_2 = event_size-1

        else:
            sequence_data.append({"HADM_ID": row['HADM_ID'], "ADMISSIONS_EVENT": []})
            flag_1 = 0
            flag_2 = 0
            sequence_data[flag_1]['ADMISSIONS_EVENT'].append({"SUBJECT_ID": row['SUBJECT_ID'], "SEQUENCE": {}})
        if not ('PATIENT_ADMISSION_EVENT' in sequence_data[flag_1]['ADMISSIONS_EVENT'][flag_2]['SEQUENCE'].keys()):
            sequence_data[flag_1]['ADMISSIONS_EVENT'][flag_2]['SEQUENCE']['PATIENT_ADMISSION_EVENT']=[]
        sequence_data[flag_1]['ADMISSIONS_EVENT'][flag_2]['SEQUENCE']['PATIENT_ADMISSION_EVENT'].append({"ADMITTIME": row['ADMITTIME'],"DISCHTIME": row['DISCHTIME'],
            "DEATHTIME": row['DEATHTIME'], "ADMISSION_TYPE":row['ADMISSION_LOCATION'],"DISCHARGE_LOCATION": row['DISCHARGE_LOCATION'],
            "EDREGTIME":row['EDREGTIME'], "EDOUTTIME":row['EDOUTTIME'],"DIAGNOSIS":row['DIAGNOSIS'],"HOSPITAL_EXPIRE_FLAG":row['HOSPITAL_EXPIRE_FLAG']
            , "HAS_CHARTEVENTS_DATA": row['HAS_CHARTEVENTS_DATA']})

        #if len(sequence_data)< 4:
         #  print sequence_data
    return sequence_data

def def_file_read(filepath, data):
    csvfile = open(filepath, 'r')
    reader = csv.DictReader(csvfile)
    for row in reader:
        if filepath == './database/csv/D_LABITEMS.csv':
            data.append({"ITEMID": row['ITEMID'],"LABEL": row['LABEL'],"FLUID": row['FLUID'],"CATEGORY": row['CATEGORY'],"LOINC_CODE": row['LOINC_CODE']})
        elif filepath == './database/csv/D_ITEMS.csv':
            data.append({"ITEMID": row['ITEMID'],"LABEL": row['LABEL']})
        elif filepath == './database/csv/D_ICD_PROCEDURES.csv':
            data.append({"ICD9_CODE": row['ICD9_CODE'],"SHORT_TITLE": row['SHORT_TITLE'],"LONG_TITLE": row['LONG_TITLE'] })
        elif filepath == './database/csv/D_ICD_DIAGNOSES.csv':
            data.append({"ICD9_CODE": row['ICD9_CODE'], "SHORT_TITLE": row['SHORT_TITLE'], "LONG_TITLE": row['LONG_TITLE']})
        elif filepath == './database/csv/D_CPT.csv':
            data.append({"SECTIONHEADER": row['SECTIONHEADER'],"SECTIONRANGE": row['SECTIONRANGE'],"SUBSECTIONHEADER": row['SUBSECTIONHEADER'],
                         "MINCODEINSUBSECTION": row['MINCODEINSUBSECTION'],"MAXCODEINSUBSECTION": row['MAXCODEINSUBSECTION']})
    return data

def event_file_read(filepath, sequence_data):
    csvfile = open(filepath, 'r')
    reader = csv.DictReader(csvfile)
    middle = {}
    for row in reader:
        seq_size = len(sequence_data)
        if seq_size > 0:
            for m in range(0, seq_size):
                patient_id = sequence_data[m]['HADM_ID']
                middle[str(patient_id)] = m
            if not (row['HADM_ID'] in middle.keys()):
                sequence_data.append({"HADM_ID": row['HADM_ID'], "ADMISSIONS_EVENT": []})
                size = len(sequence_data)
                size_flag_1 = size-1
                size_flag_2 = 0
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'].append({"SUBJECT_ID": row['SUBJECT_ID'], "SEQUENCE": {}})

            else:
                index = middle[row['HADM_ID']]
                size_subject = len(sequence_data[index]['ADMISSIONS_EVENT'])
                for i in range(0, size_subject):
                    subject_id = sequence_data[index]['ADMISSIONS_EVENT'][i]['SUBJECT_ID']
                    middle[str(subject_id)] = i
                if not(row['SUBJECT_ID'] in middle.keys()):
                    sequence_data[index]['ADMISSIONS_EVENT'].append({"SUBJECT_ID": row['SUBJECT_ID'], "SEQUENCE": {}})
                    size_flag_1 = index
                    size_flag_2 = 0
                else:
                    size_flag_1 = index
                    size_flag_2 = middle[row['SUBJECT_ID']]

        else:
            sequence_data.append({"HADM_ID": row['HADM_ID'], "ADMISSIONS_EVENT": []})
            size_flag_1 = 0
            size_flag_2 = 0
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'].append({"SUBJECT_ID": row['SUBJECT_ID'], "SEQUENCE": {}})
        if filepath == './database/csv/CALLOUT.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CALLOUT_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CALLOUT_EVENT'].append({
                "CURR_CAREUNIT": row['CURR_CAREUNIT'],"CALLOUT_SERVICE": row['CALLOUT_SERVICE'], "CALLOUT_STATUS": row['CALLOUT_STATUS'],
                "CALLOUT_OUTCOME": row['CALLOUT_OUTCOME'],"ACKNOWLEDGE_STATUS": row['ACKNOWLEDGE_STATUS'], "CREATETIME": row['CREATETIME'],
                "UPDATETIME": row['UPDATETIME'],"ACKNOWLEDGETIME": row['ACKNOWLEDGETIME'], "OUTCOMETIME": row['OUTCOMETIME']})
        elif filepath == './database/csv/CHARTEVENTS.csv':

            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CHART_EVENT']=[]
            seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CHART_EVENT'])
            if seq_size>0:
                for m in range(0, seq_size):
                    icustay_id = sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CHART_EVENT'][m]['ICUSTAY_ID']
                    middle[str(icustay_id)] = m
                if not (row['ICUSTAY_ID'] in middle.keys()):
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CHART_EVENT'].append({
                        "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_CHART_EVENT":[]})
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CHART_EVENT'][seq_size]['ICU_CHART_EVENT'].append(
                        {"ITEMID":row['ITEMID'],"CHARTTIME":row['CHARTTIME'],"STORETIME":row['STORETIME'],"CGID":row['CGID'],"VALUENUM":row['VALUENUM'],
                         "VALUEUOM":row['VALUEUOM']}
                    )
                else:
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CHART_EVENT'][
                        middle[row['ICUSTAY_ID']]]['ICU_CHART_EVENT'].append({"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "VALUENUM": row['VALUENUM'],"VALUEUOM": row['VALUEUOM']})
            else:
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_CHART_EVENT'].append({
                    "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_CHART_EVENT": []})
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CHART_EVENT'][
                    seq_size]['ICU_CHART_EVENT'].append(
                    {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                     "CGID": row['CGID'], "VALUENUM": row['VALUENUM'],
                     "VALUEUOM": row['VALUEUOM']}
                )

        elif filepath == './database/csv/CPTEVENTS.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CPT_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CPT_EVENT'].append({
              "COSTCENTER":row['COSTCENTER'],"CPT_CD":row['CPT_CD'],"CPT_NUMBER":row['CPT_NUMBER'],"TICKET_ID_SEQ":row['TICKET_ID_SEQ'],
                "SECTIONHEADER":row['SECTIONHEADER'],"SUBSECTIONHEADER":row['SUBSECTIONHEADER']
            })
        elif filepath == './database/csv/DATETIMEEVENTS.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_DATETIME_EVENT']=[]
            seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_DATETIME_EVENT'])
            if seq_size>0:
                for m in range(0, seq_size):
                    icustay_id = sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_DATETIME_EVENT'][m]['ICUSTAY_ID']
                    middle[str(icustay_id)] = m
                if not (row['ICUSTAY_ID'] in middle.keys()):
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_DATETIME_EVENT'].append({
                        "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_DATE_EVENT":[]})
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_DATETIME_EVENT'][seq_size]['ICU_DATE_EVENT'].append(
                        {"ITEMID":row['ITEMID'],"CHARTTIME":row['CHARTTIME'],"STORETIME":row['STORETIME'],"CGID":row['CGID'],"VALUE":row['VALUE'],
                         "VALUEUOM":row['VALUEUOM']}
                    )
                else:
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_DATETIME_EVENT'][
                        middle[row['ICUSTAY_ID']]]['ICU_DATE_EVENT'].append({"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "VALUE": row['VALUE'],"VALUEUOM": row['VALUEUOM']})
            else:
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_DATETIME_EVENT'].append({
                    "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_DATE_EVENT": []})
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_DATETIME_EVENT'][
                    seq_size]['ICU_DATE_EVENT'].append(
                    {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                     "CGID": row['CGID'], "VALUE": row['VALUE'],"VALUEUOM": row['VALUEUOM']}
                )
        elif filepath == './database/csv/DIAGNOSES_ICD.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_DIAGNOSES_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_DIAGNOSES_EVENT'].append({
                "SEQ_NUM":row['SEQ_NUM'],"ICD9_CODE":row['ICD9_CODE']
            })

        elif filepath == './database/csv/DRGCODES.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_DRGCODES_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_DRGCODES_EVENT'].append({
               "DRG_TYPE":row['DRG_TYPE'],"DRG_CODE":row['DRG_CODE']
            })
        elif filepath == './database/csv/ICUSTAYS.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICUSTAY_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICUSTAY_EVENT'].append({
                "ICUSTAY_ID":row['ICUSTAY_ID'],"FIRST_CAREUNIT":row['FIRST_CAREUNIT'],"LAST_CAREUNIT":row['LAST_CAREUNIT'],
                "INTIME":row['INTIME'],"OUTTIME":row['OUTTIME'],"LOS":row['LOS']
            })
        elif filepath == './database/csv/INPUTEVENTS_CV.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_INPUT_EVENT']=[]
            seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                               'PATIENT_ICU_INPUT_EVENT'])
            if seq_size > 0:
                for m in range(0, seq_size):
                    icustay_id = sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][m]['ICUSTAY_ID']
                    middle[str(icustay_id)] = m
                if not (row['ICUSTAY_ID'] in middle.keys()):
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'].append({
                        "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_INPUT_EVENT": []})
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][seq_size]['ICU_INPUT_EVENT'].append(
                        {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "AMOUNT": row['AMOUNT'],
                         "AMOUNTUOM": row['AMOUNTUOM'],"ORDERID":row['ORDERID'],"LINKORDERID":row['LINKORDERID']}
                    )
                else:
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][
                        middle[row['ICUSTAY_ID']]]['ICU_INPUT_EVENT'].append(
                        {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "AMOUNT": row['AMOUNT'],
                         "AMOUNTUOM": row['AMOUNTUOM'],"ORDERID":row['ORDERID'],"LINKORDERID":row['LINKORDERID']})
            else:
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_INPUT_EVENT'].append({
                    "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_INPUT_EVENT": []})
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_INPUT_EVENT'][
                    seq_size]['ICU_INPUT_EVENT'].append(
                    {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "AMOUNT": row['AMOUNT'],
                         "AMOUNTUOM": row['AMOUNTUOM'],"ORDERID":row['ORDERID'],"LINKORDERID":row['LINKORDERID']}
                )
        elif filepath == './database/csv/INPUTEVENTS_MV.csv':
            if not ('PATIENT_ICU_INPUT_EVENT' in sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'].keys()):
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_INPUT_EVENT']=[]
            seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                               'PATIENT_ICU_INPUT_EVENT'])
            if seq_size > 0:
                for m in range(0, seq_size):
                    icustay_id = sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][m]['ICUSTAY_ID']
                    middle[str(icustay_id)] = m
                if not (row['ICUSTAY_ID'] in middle.keys()):
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'].append({
                        "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_INPUT_EVENT": []})
                    seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                                       'PATIENT_ICU_INPUT_EVENT'])
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][seq_size-1]['ICU_INPUT_EVENT'].append(
                        {"ITEMID": row['ITEMID'],"STARTTIME": row['STARTTIME'], "ENDTIME": row['ENDTIME'],"AMOUNT": row['AMOUNT'],
                         "AMOUNTUOM": row['AMOUNTUOM'],"ORDERID": row['ORDERID'], "LINKORDERID": row['LINKORDERID']}
                    )
                else:
                    size_input = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][middle[row['ICUSTAY_ID']]]['ICU_INPUT_EVENT'])
                    if row['ITEMID'] == sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][middle[row['ICUSTAY_ID']]]['ICU_INPUT_EVENT'][size_input-1]['ITEMID']:

                        sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_INPUT_EVENT'][middle[row['ICUSTAY_ID']]]['ICU_INPUT_EVENT'][size_input-1]['STARTTIME']=row['STARTTIME']
                        sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                            'PATIENT_ICU_INPUT_EVENT'][middle[row['ICUSTAY_ID']]]['ICU_INPUT_EVENT'][size_input - 1][
                            'ENDTIME'] = row['ENDTIME']

                    else:
                        sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                            'PATIENT_ICU_INPUT_EVENT'][
                            middle[row['ICUSTAY_ID']]]['ICU_INPUT_EVENT'].append(
                            {"ITEMID": row['ITEMID'],"STARTTIME": row['STARTTIME'], "ENDTIME": row['ENDTIME'],"AMOUNT": row['AMOUNT'],
                         "AMOUNTUOM": row['AMOUNTUOM'],"ORDERID": row['ORDERID'], "LINKORDERID": row['LINKORDERID']})
            else:
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_INPUT_EVENT'].append({
                    "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_INPUT_EVENT": []})
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_INPUT_EVENT'][
                    seq_size]['ICU_INPUT_EVENT'].append(
                    {"ITEMID": row['ITEMID'],"STARTTIME": row['STARTTIME'], "ENDTIME": row['ENDTIME'],"AMOUNT": row['AMOUNT'],
                         "AMOUNTUOM": row['AMOUNTUOM'],"ORDERID": row['ORDERID'], "LINKORDERID": row['LINKORDERID']}
                )
        elif filepath == './database/csv/LABEVENTS.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_LAB_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_LAB_EVENT'] .append({
                "ITEMID": row['ITEMID'],"CHARTTIME":row['CHARTTIME'], "VALUE": row['VALUE'],"VALUEUOM": row['VALUEUOM'],
                "FLAG":row['FLAG']
            })
        elif filepath == './database/csv/MICROBIOLOGYEVENTS.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_MICROBIOLOGY_EVENT']=[]
            size_micro = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_MICROBIOLOGY_EVENT'])
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_MICROBIOLOGY_EVENT'].append({})
            if row['CHARTDATE']!='':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_MICROBIOLOGY_EVENT'][size_micro]['CHARTDATE']=row['CHARTDATE']
            if row['CHARTTIME'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['CHARTTIME'] = row['CHARTTIME']
            if row['SPEC_ITEMID'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['SPEC_ITEMID'] = row['SPEC_ITEMID']
            if row['SPEC_TYPE_DESC'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['SPEC_TYPE_DESC'] = row['SPEC_TYPE_DESC']
            if row['ORG_ITEMID'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['ORG_ITEMID'] = row['ORG_ITEMID']
            if row['ORG_NAME'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['ORG_NAME'] = row['ORG_NAME']
            if row['ISOLATE_NUM'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['ISOLATE_NUM'] = row['ISOLATE_NUM']
            if row['DILUTION_TEXT'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['DILUTION_TEXT'] = row['DILUTION_TEXT']
            if row['DILUTION_COMPARISON'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['DILUTION_COMPARISON'] = row['DILUTION_COMPARISON']
            if row['INTERPRETATION'] != '':
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_MICROBIOLOGY_EVENT'][size_micro]['INTERPRETATION'] = row['INTERPRETATION']

        elif filepath == './database/csv/OUTPUTEVENTS.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_OUTPUT_EVENT']=[]
            seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                               'PATIENT_ICU_OUTPUT_EVENT'])
            if seq_size > 0:
                for m in range(0, seq_size):
                    icustay_id = sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_OUTPUT_EVENT'][m]['ICUSTAY_ID']
                    middle[str(icustay_id)] = m
                if not (row['ICUSTAY_ID'] in middle.keys()):
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_OUTPUT_EVENT'].append({
                        "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_OUTPUT_EVENT": []})
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_OUTPUT_EVENT'][seq_size]['ICU_OUTPUT_EVENT'].append(
                        {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "VALUE": row['VALUE'],"VALUEUOM": row['VALUEUOM']}
                    )
                else:
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_OUTPUTINPUT_EVENT'][
                        middle[row['ICUSTAY_ID']]]['ICU_OUTPUT_EVENT'].append(
                        {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "VALUE": row['VALUE'], "VALUEUOM": row['VALUEUOM']})
            else:
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_OUTPUT_EVENT'].append({
                    "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_OUTPUT_EVENT": []})
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_OUTPUT_EVENT'][
                    seq_size]['ICU_OUTPUT_EVENT'].append(
                    {"ITEMID": row['ITEMID'], "CHARTTIME": row['CHARTTIME'], "STORETIME": row['STORETIME'],
                     "CGID": row['CGID'], "VALUE": row['VALUE'], "VALUEUOM": row['VALUEUOM']})
        #elif filepath == './csv_demo/PRESCRIPTIONS.csv':
         #   sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'] = {"PATIENT_ICU_CALLOUT_EVENT": {}}
          #  sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_CALLOUT_EVENT'] = {}
        elif filepath == './database/csv/PROCEDUREEVENTS_MV.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_PROCEDUREEVENTS_EVENT']=[]
            seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                               'PATIENT_ICU_PROCEDUREEVENTS_EVENT'])
            if seq_size > 0:
                for m in range(0, seq_size):
                    icustay_id = sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_PROCEDUREEVENTS_EVENT'][m]['ICUSTAY_ID']
                    middle[str(icustay_id)] = m
                if not (row['ICUSTAY_ID'] in middle.keys()):
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_PROCEDUREEVENTS_EVENT'].append({
                        "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_PROCEDUREEVENTS_EVENT": []})
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_PROCEDUREEVENTS_EVENT'][seq_size]['ICU_PROCEDUREEVENTS_EVENT'].append(
                        {"ITEMID": row['ITEMID'],"STARTTIME": row['STARTTIME'], "ENDTIME": row['ENDTIME'], "STORETIME": row['STORETIME'],
                         "CGID": row['CGID'], "VALUE": row['VALUE'], "VALUEUOM": row['VALUEUOM'],"ORDERID": row['ORDERID'], "LINKORDERID":
                             row['LINKORDERID'],"ISOPENBAG":row['ISOPENBAG']}
                    )
                else:
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_PROCEDUREEVENTS_EVENT'][
                        middle[row['ICUSTAY_ID']]]['ICU_PROCEDUREEVENTS_EVENT'].append(
                        {"ITEMID": row['ITEMID'], "STARTTIME": row['STARTTIME'], "ENDTIME": row['ENDTIME'],
                         "STORETIME": row['STORETIME'],"CGID": row['CGID'], "VALUE": row['VALUE'], "VALUEUOM": row['VALUEUOM'],
                         "ORDERID": row['ORDERID'], "LINKORDERID":row['LINKORDERID'],"ISOPENBAG": row['ISOPENBAG']})
            else:
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_PROCEDUREEVENTS_EVENT'].append({
                    "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_PROCEDUREEVENTS_EVENT": []})
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_PROCEDUREEVENTS_EVENT'][
                    seq_size]['ICU_PROCEDUREEVENTS_EVENT'].append(
                    {"ITEMID": row['ITEMID'], "STARTTIME": row['STARTTIME'], "ENDTIME": row['ENDTIME'],
                     "STORETIME": row['STORETIME'], "CGID": row['CGID'], "VALUE": row['VALUE'],"VALUEUOM": row['VALUEUOM'],
                     "ORDERID": row['ORDERID'], "LINKORDERID": row['LINKORDERID'],"ISOPENBAG": row['ISOPENBAG']})
        elif filepath == './database/csv/PROCEDURES_ICD.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_PROCEDURES_ICD_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_PROCEDURES_ICD_EVENT'].append({
                "SEQ_NUM": row['SEQ_NUM'], "ICD9_CODE": row['ICD9_CODE']
            })

        elif filepath == './database/csv/SERVICES.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_SERVICES_EVENT']=[]
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                'PATIENT_SERVICES_EVENT'].append({
                "TRANSFERTIME": row['TRANSFERTIME'], "PREV_SERVICE": row['PREV_SERVICE'],"CURR_SERVICE": row['CURR_SERVICE']
            })
        elif filepath == './database/csv/TRANSFERS.csv':
            sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE']['PATIENT_ICU_TRANSFERS_EVENT']=[]
            seq_size = len(sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                               'PATIENT_ICU_TRANSFERS_EVENT'])
            if seq_size > 0:
                for m in range(0, seq_size):
                    icustay_id = sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_TRANSFERS_EVENT'][m]['ICUSTAY_ID']
                    middle[str(icustay_id)] = m
                if not (row['ICUSTAY_ID'] in middle.keys()):
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_TRANSFERS_EVENT'].append({
                        "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_TRANSFERS_EVENT": []})
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_TRANSFERS_EVENT'][seq_size]['ICU_TRANSFERS_EVENT'].append(
                        {"EVENTTYPE":row['EVENTTYPE'],"PREV_CAREUNIT":row['PREV_CAREUNIT'],"CURR_CAREUNIT":row['CURR_CAREUNIT'],
                         "INTIME":row['INTIME'],"OUTTIME":row['OUTTIME'],"LOS":row['LOS']}
                    )
                else:
                    sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                        'PATIENT_ICU_TRANSFERS_EVENT'][
                        middle[row['ICUSTAY_ID']]]['ICU_TRANSFERS_EVENT'].append(
                        {"EVENTTYPE": row['EVENTTYPE'], "PREV_CAREUNIT": row['PREV_CAREUNIT'],
                         "CURR_CAREUNIT": row['CURR_CAREUNIT'],
                         "INTIME": row['INTIME'], "OUTTIME": row['OUTTIME'], "LOS": row['LOS']})
            else:
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_TRANSFERS_EVENT'].append({
                    "ICUSTAY_ID": row['ICUSTAY_ID'], "ICU_TRANSFERS_EVENT": []})
                sequence_data[size_flag_1]['ADMISSIONS_EVENT'][size_flag_2]['SEQUENCE'][
                    'PATIENT_ICU_TRANSFERS_EVENT'][
                    seq_size]['ICU_TRANSFERS_EVENT'].append(
                    {"EVENTTYPE": row['EVENTTYPE'], "PREV_CAREUNIT": row['PREV_CAREUNIT'],
                     "CURR_CAREUNIT": row['CURR_CAREUNIT'],
                     "INTIME": row['INTIME'], "OUTTIME": row['OUTTIME'], "LOS": row['LOS']})

    return sequence_data

def json_write(data,resultpath):
    json.dump(data, open(resultpath, "wb"))


if __name__ == '__main__':
    sequence_data = []
    def_data = []
    file_dir = './database/csv/'
    result_dir = './database/json/'
    print("start")

    sequence_data = patient_file_read(file_dir+'ADMISSIONS.csv',sequence_data)
    print("ADMISSIONS")
    sequence_data = event_file_read(file_dir+'CALLOUT.csv',sequence_data)
    print("CALLOUT")
    #sequence_data = event_file_read(file_dir + 'CHARTEVENTS.csv',sequence_data)
    #print("CHARTEVENTS")
    sequence_data = event_file_read(file_dir + 'CPTEVENTS.csv',sequence_data)
    print("CPTEVENTS")
    sequence_data = event_file_read(file_dir + 'DATETIMEEVENTS.csv',sequence_data)
    print("DATETIMEEVENTS")
    sequence_data = event_file_read(file_dir + 'DIAGNOSES_ICD.csv',sequence_data)
    print("DIAGNOSES_ICD")
    sequence_data = event_file_read(file_dir + 'DRGCODES.csv',sequence_data)
    print("DRGCODES")
    sequence_data = event_file_read(file_dir + 'ICUSTAYS.csv',sequence_data)
    print("ICUSTAYS")
    sequence_data = event_file_read(file_dir + 'INPUTEVENTS_CV.csv',sequence_data)
    print("INPUTEVENTS_CV")
    sequence_data = event_file_read(file_dir + 'INPUTEVENTS_MV.csv',sequence_data)
    print("INPUTEVENTS_MV")
    sequence_data = event_file_read(file_dir + 'LABEVENTS.csv',sequence_data)
    print("LABEVENTS")
    sequence_data = event_file_read(file_dir + 'MICROBIOLOGYEVENTS.csv',sequence_data)
    print("MICROBIOLOGYEVENTS")
    sequence_data = event_file_read(file_dir + 'OUTPUTEVENTS.csv',sequence_data)
    print("OUTPUTEVENTS")
    sequence_data = event_file_read(file_dir + 'PRESCRIPTIONS.csv',sequence_data)
    print("PRESCRIPTIONS")
    sequence_data = event_file_read(file_dir + 'PROCEDUREEVENTS_MV.csv',sequence_data)
    print("PROCEDUREEVENTS_MV")
    sequence_data = event_file_read(file_dir + 'PROCEDURES_ICD.csv',sequence_data)
    print("PROCEDURES_ICD")
    sequence_data = event_file_read(file_dir + 'SERVICES.csv',sequence_data)
    print("SERVICES")
    sequence_data = event_file_read(file_dir + 'TRANSFERS.csv', sequence_data)
    print("TRANSFERS")
    json_write(sequence_data,result_dir+'PATIENT_EVENTS_SUMMARY.json')
    print("json")
'''
    def_data = def_file_read(file_dir + 'D_LABITEMS.csv',[])
    json_write(def_data,result_dir+'D_LABITEMS.json')
    def_data = def_file_read(file_dir + 'D_ITEMS.csv',[])
    json_write(def_data,result_dir+'D_ITEMS.json')
    def_data = def_file_read(file_dir + 'D_ICD_PROCEDURES.csv',[])
    json_write(def_data, result_dir + 'D_ICD_PROCEDURES.json')
    def_data = def_file_read(file_dir + 'D_ICD_DIAGNOSES.csv', [])
    json_write(def_data, result_dir + 'D_ICD_DIAGNOSES.json')
    def_data = def_file_read(file_dir + 'D_CPT.csv', [])
    json_write(def_data, result_dir + 'D_CPT.json')
'''