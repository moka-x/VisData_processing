import csv
import json


def file_read(filepath, resultpath):
        csvfile = open(filepath, 'r')
        reader = csv.DictReader(csvfile)
        tripdata = []
        origin = {}

        for row in reader:
            if not(row['bikeid'] in origin.keys()):
                origin[row['bikeid']] = {"sequence": []}
            if len(origin[row['bikeid']]['sequence']) == 0:
                origin[row['bikeid']]['sequence'].append({"Station_ID": row['start station id'],
                                                            "Station_Name": row['start station name']})
            size = len(origin[row['bikeid']]['sequence'])

            if origin[row['bikeid']]['sequence'][size-1]['Station_ID'] != row['start station id']:
                origin[row['bikeid']]['sequence'].append({"Station_ID": row['start station id'],
                                                            "Station_Name": row['start station name']})

            origin[row['bikeid']]['sequence'].append({"Station_ID": row['end station id'],
                                                        "Station_Name": row['end station name']})

        for id in origin:
            tripdata.append({"bike_id": id, "sequence": origin[id]['sequence']})
        #print origin['24608']['sequence']
        #print tripdata
        json.dump(tripdata, open(resultpath + ".json", "wb"))
        return tripdata


def multifiles(rootdir):
    year = [2013, 2014, 2015, 2016, 2017]
    month = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    origin = []
    middledata = {}
    tripdata = []

    for i in year:
        for j in month:
            if j < 10:
                flag = "0"
            else:
                flag = ""

           # if(i == 2016 and j>5):
                timetepms = "201606-201612"
            if(i == 2013 and j > 6) or (i == 2017 and j < 10) or (i == 2014) or (i == 2015) or (i == 2016):
                    filepath = rootdir + "csv/" + str(i) + flag + str(j) +"-citibike-tripdata.csv"
                    resultpath = rootdir + "json/" + str(i) + flag + str(j)
                    print filepath
                   # print i, flag, j
                    #timeflag = i * 100 + j
                    origin = []
                    origin = file_read(filepath, resultpath)
            '''
                    bike_size = len(origin)
                    print bike_size
                    if bike_size > 0:
                        for m in range(0, bike_size):
                            #print m
                            bike_id = origin[m]['bike_id']
                            if not (str(bike_id) in middledata.keys()):
                                middledata[str(bike_id)] = {"sequence": []}
                            middledata[str(bike_id)]['sequence'].extend(origin[m]['sequence'])

    for id in middledata:
        tripdata.append({"bike_id": id, "sequence": middledata[id]['sequence']})
    #print tripdata
    resultpath = rootdir + "json/"+timetemps+"dataset"
    json.dump(tripdata, open(resultpath + ".json", "wb"))
'''
def mergefile(rootdir):
    year = [2013, 2014, 2015, 2016, 2017]
    month = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    middledata = {}
    middle = {}
    tripdata = []
    datastatic  = {}
    resultpath = rootdir + "201307-201312-dataset"
    for i in year:
        for j in month:
            '''
            if(i == 2014 and j<7):
                if j < 10:
                    flag = "0"
                else:
                    flag = ""
                filepath = rootdir + "../csv/" + str(i) + flag + str(j) + "-citibike-tripdata.csv"
                resultpath = rootdir + str(i)+flag+str(j)
                origin = file_read(filepath, resultpath)
                print i,j
'''
            if(i ==2013 and j > 6):
            #if(i == 2017 and j < 10) or (i == 2013 and j > 6) or (i == 2014) or (i == 2015) or (i ==2016):
            #if(i == 2013 and j > 6) or (i == 2014) or (i == 2015 and j<10):
                if j < 10:
                    flag = "0"
                else:
                    flag = ""
                jsonfile = rootdir+str(i)+flag+str(j)+'.json'
                print jsonfile
                with open(jsonfile, 'r') as f:

                    origin = json.load(f)

                    bike_size = len(origin)
                    print bike_size
                    if bike_size > 0:
                        for m in range(0, bike_size):
                            # print m
                            bike_id = origin[m]['bike_id']
                           # if not(str(i)+flag+str(j)in datastatic.keys()):
                            #    datastatic[str(i)+flag+str(j)] = {"time":str(i)+flag+str(j),"bike_num":bike_size,"static":[]}
                            #datastatic[str(i)+flag+str(j)]['static'].append({"bike_id": bike_id, "trace_num": len(origin[m]['sequence'])})
                            if not (str(bike_id) in middledata.keys()):
                                middledata[str(bike_id)] = {"sequence": []}
                            middledata[str(bike_id)]['sequence'].extend(origin[m]['sequence'])
                    origin = []
                    print len(middledata)


                    f.flush()
                    f.close()




    #json.dump(datastatic,open("static.json","wb"))
   # print datastatic
    for id in middledata:
        tripdata.append({"bike_id": id, "sequence": middledata[id]['sequence']})
        # print tripdata
    json.dump(tripdata, open(resultpath + ".json", "wb"))

if __name__ == '__main__':
    #multifiles("./data/")
    mergefile("./data/json/")
   # with open("static.json",'r') as f:
    #origin =


