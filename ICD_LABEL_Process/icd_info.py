import psycopg2
def icd_info(code,version):
    conn = psycopg2.connect(database="icd_database", user="postgres", password="123456", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    info = ""
    if version == 10:
        if info == "":
            cur.execute("select concept_name from concept "
                    "where vocabulary_id ='ICD10' and concept_code = '" + str(code)+"'")
            rows = cur.fetchall()
            if len(rows) == 1:
                info = rows[0][0]
        if info == "":
            cur.execute("select concept_name from concept "
                        "where vocabulary_id ='ICD10CM' and concept_code = '" + str(code)+"'")
            rows = cur.fetchall()
            if len(rows) == 1:
                info = rows[0][0]

    elif version == 9:
            if info == "":
                cur.execute("select concept_name from concept "
                            "where vocabulary_id ='ICD9CM' and concept_code = '" + str(code)+"'")
                rows = cur.fetchall()
                if len(rows) == 1:
                    info = rows[0][0]
            if info == "":
                cur.execute("select concept_name from concept "
                            "where vocabulary_id ='ICD9Proc' and concept_code = '" + str(code)+"'")
                rows = cur.fetchall()
                if len(rows) == 1:
                    info = rows[0][0]
    conn.close()
    return info
if __name__ == "__main__":

    info = icd_info('44.31', 9)
    print(info)