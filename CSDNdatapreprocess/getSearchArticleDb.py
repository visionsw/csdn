import pymysql
import re
import tqdm
import threading
'''
建立检索项文章信息数据库
清理前：
清理后：
'''


def insert_sarticle(data):
    data = tuple(data)
    con_temp = pymysql.connect(host="localhost", user="root", password="im123456", db="article_info", charset="utf8",
                          cursorclass=pymysql.cursors.DictCursor)
    sql_temp = "INSERT INTO sarticle_data(link, id, type, title, brief_des, time, read_num, comments_num) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor_temp = con_temp.cursor()
    cursor_temp.execute(sql_temp, data)
    con_temp.commit()
    con_temp.close()


class Mythread(threading.Thread):
    def __init__(self, results, s_article_list):
        threading.Thread.__init__(self)
        self.results = results
        self.s_article_list = s_article_list

    def run(self):
        for i in tqdm.trange(len(self.s_article_list)):
            item = self.s_article_list[i]
            for article in self.results:
                try:
                    if article["link"] == item[0] and article["id"] == item[1]:
                        # print(tuple(article.values()))
                        insert_sarticle(article.values())
                        break
                except Exception as e:
                    # print(item)
                    break


truncated = [(0, 100000), (100000, 200000), (200000, 300000),  (300000, 400000),  (400000, 500000),  (500000, 600000),
             (600000, 700000), (700000, 800000),  (800000, 900000),  (900000, 1000000),  (1000000, 1036213)]

con = pymysql.connect(host="localhost", user="root", password="im123456", db="article_info", charset="utf8",
                      cursorclass=pymysql.cursors.DictCursor)

cursor = con.cursor()
sql = "SELECT * FROM article_data"
cursor.execute(sql)
# 获取所有文章的信息
results1 = cursor.fetchall()

# 从csv里获取检索文章的链接并切分成作者和id
s_article_list1 = []
with open("D:\pythonCoding\CSDNanalysis\data\link_data\\article_link.csv", "rb") as f:
    next(f)
    lines = f.readlines()
    for line in lines:
        link = str(line, encoding='utf8').strip().split(",")[1]
        try:
            r = link.split("article/details/")
            s_article_list1.append(r)
        except Exception as e:
            print(e)
# 构造线程
thread_list = [Mythread(results1[it[0]:it[1]], s_article_list1) for it in truncated]

# 运行
for th in thread_list:
    th.start()
for th in thread_list:
    th.join()


# for i in tqdm.trange(len(s_article_list)):
#     item = s_article_list[i]
#     for article in results:
#         try:
#             if article["link"] == item[0] and article["id"] == item[1]:
#                 # print(tuple(article.values()))
#                 break
#         except Exception as e:
#             # print(item)
#             break


