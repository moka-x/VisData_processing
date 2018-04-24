import json
import psycopg2
pattern_str = "Allergy 2|TRACHE CARE"
pattern_set = pattern_str.split('|')
len_pattern = len(pattern_str)
conn = psycopg2.connect(database="mimic_demo_rest", user="postgres", password="123456", host="127.0.0.1", port="5432")
cur = conn.cursor()
with open('./app/data/item_label.json','r')as f:
        data = json.load(f)

print len(data)
for j in range(len(data)):
    if data[j]['ITEMID']=='20012':
        index = j
        meaning = data[j]['LABEL']
        print index, meaning
        print data[j]

with open('./app/data/other_item.json','r')as fs:
    datas = json.load(fs)
    reset=[]
    with open('./app/data/lab_label.json', 'r')as fm:
        datas2 = json.load(fm)
        reset = []
    print len(datas)
    # for j in range(len(datas)):
    #     reset.append({'ITEMID':datas[j]})
    # json.dump(reset,open('./app/data/other_item_2.json','wb'))
        # if datas[j] == 'Tacrolimus':
        #     index = j
        #     meaning = datas[j]
        #     print index, meaning
        #     print datas[j]
        #for i in range(0, len_pattern):
          #  cur.execute()
