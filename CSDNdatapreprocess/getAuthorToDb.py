import pandas as pd
import pymysql

con = pymysql.connect(host="localhost", user="root", password="im123456", db="article_info", charset="utf8",
                      cursorclass=pymysql.cursors.DictCursor)
cursor = con.cursor()

df = pd.read_csv("D:\pythonCoding\CSDNanalysis\CSDNcrawl\\author_info1.csv", encoding="utf8")

sql = "INSERT INTO author_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

for value in df.values:
    cursor.execute(sql, [value[i] for i in range(12)])

con.commit()
