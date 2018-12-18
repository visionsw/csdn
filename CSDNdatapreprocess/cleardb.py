import pymysql
'''
数据库清理
清理前：1100151
清理后：1036213
'''
con = pymysql.connect(host="localhost", user="root", password="im123456", db="article_info", charset="utf8",
                      cursorclass=pymysql.cursors.DictCursor)

cursor = con.cursor()
# 消除帝都的凛冬
# delete_sql1 = "DELETE FROM article_data WHERE title='帝都的凛冬'"
# num_sql = "SELECT * FROM article_data"
#
# num = cursor.execute(num_sql)
# print(num)
#
# cursor.execute(delete_sql1)
# con.commit()
#
# num = cursor.execute(num_sql)
# print(num)


# 导入1000条测试数据
def test_data():
    sql = "SELECT * FROM sarticle_data limit 0,1000"
    d_sql = "DELETE FROM sarticle_data_copy"
    cursor.execute(d_sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    for i in result:
        sql1 = "INSERT INTO sarticle_data_copy VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql1, list(i.values()))
    con.commit()


# 消除数据项偏移问题
def fix():
    sql = "UPDATE article_data as a set a.brief_des=a.title, a.title=a.type, a.type='null' WHERE LENGTH(a.type)<>3"
    cursor.execute(sql)
    con.commit()

# test_data()
fix()

