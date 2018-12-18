import pymysql
import jieba
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import collections
from multiprocessing import Process,Lock
import pandas as pd
import tqdm

con = pymysql.connect(host="localhost", user="root", password="im123456", db="article_info", charset="gbk",
                      cursorclass=pymysql.cursors.DictCursor)
cursor = con.cursor()


# 添加词表
def addWords():
    with open("D:\pythonCoding\CSDNanalysis\data\MLwords.txt", "r", encoding="UTF-16LE") as f:
        reader = [item.strip().strip("\ufeff") for item in f.readlines()]  # 防止读取空格和换行
        for item in reader:
            jieba.add_word(item)


# 从数据库中获取数据
def getData(name, tuple):
    sql = "SELECT * FROM " + name + " limit " + str(tuple[0]) + "," + str(tuple[1])
    cursor.execute(sql)
    results = cursor.fetchall()
    return [(item["title"], item["brief_des"], item["aid"]) for item in results]


# 切词并筛去停用词以及'\r\n'等符号
def cwt(onetext):
    filer = open("D:\pythonCoding\多元数据融合\data\stop_words.txt", "r")
    reader = [item.strip() for item in filer.readlines()]   # 防止读取空格和换行
    # print(reader)
    return [i.strip() for i in jieba.cut(onetext) if i not in reader and i.strip() != ""
            and not i.strip().isdigit() and len(i.strip()) > 1]


# 取某个文档的主题词
def print_top_words(model, feature_names, n_top_words):
    topic_results = []
    for topic_idx, topic in enumerate(model.components_):
        topic_results.append(" ".join([feature_names[i] for i in topic.argsort()[:-n_top_words - 1:-1]]))  # 由小到大排列的索引
    return topic_results


# 对得到的LDA结果进行筛选,返回所有主题联合串以及词频字典
def emerge(results_list):
    # 将每个主题的串连接成一个串再用空格切分
    frequency_dict = collections.Counter(" ".join(results_list).split(" "))
    return " ".join(results_list).split(" "), frequency_dict


class MyProcess(Process):
    def __init__(self, table, tuple, lock):
        Process.__init__(self)
        self.tuple = tuple
        self.table = table
        self.lock = lock

    def run(self):
        get_topic(self.table, self.tuple, self.lock)


def get_topic(table, tuple, lock):
    results_dict = dict()
    lock.acquire()
    # 获取id
    ids = [item[2] for item in getData(table, tuple)]
    results_dict["ids"] = ids

    # 对文章标题进行分词
    titles = [item[0] for item in getData(table, tuple)]
    titles_cut = [cwt(item) for item in titles]
    print(len(titles_cut), titles_cut)

    # 对文章简介进行分词及主体提取
    dess = [item[1] for item in getData(table, tuple)]
    lock.release()
    # 切词,并将每个简介连接成字符串
    dess_cut = [" ".join(cwt(des)) for des in dess]

    # 词向量
    cvectorizer = CountVectorizer()
    tf = cvectorizer.fit_transform(dess_cut)  # 稀疏矩阵


    # lda Model
    lda = LatentDirichletAllocation(learning_method='batch')
    # 对每个简介进行主题抽取
    all_data_keywords = []
    for one in tf:
        lda.fit_transform(one)
        # 取主题词
        results = print_top_words(lda, cvectorizer.get_feature_names(), n_top_words=15)
        # 选择在每个主题都出现的词作为关键词标签
        topic_words = [item[0] for item in emerge(results)[1].items() if item[1] > 9]
        all_data_keywords.append(topic_words)

    if len(titles_cut) == len(all_data_keywords):
        r = [list(set(titles_cut[i] + all_data_keywords[i])) for i in range(len(titles_cut))]
        results_dict["kw"] = r
        df = pd.DataFrame(results_dict)
        df.to_csv("D:\pythonCoding\CSDNanalysis\data\\a_kw\\" + str(table) + "-" + str(tuple) + ".csv")

    else:
        print("length different")


if __name__ == '__main__':
    addWords()
    tlock = Lock()
    for i in tqdm.trange(50, 52):
        index_list = [(i * 20000 + j * 2000, 2000)for j in range(10)]
        for index in range(len(index_list)):
            p = MyProcess("article_data", index_list[index], tlock)
            p.daemon = True
            p.start()
            p.join()