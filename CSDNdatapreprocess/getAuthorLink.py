import pandas
import numpy as np
from pandas import DataFrame

# 打开文件
opener = open('D:\pythonCoding\CSDNanalysis\CSDNcrawl\\article_link.csv', 'r', encoding='utf-8')
# 读取文件生成dataframe
dataframe = pandas.read_csv(opener, names=["sort", "link"])
# 清除空数据
dataframe = dataframe.dropna()

author_dict =dict()

for item in dataframe["link"]:
    author_link = str(item).split("article")[0]
    if author_link in author_dict.keys():
        author_dict[author_link] += 1
    else:
        author_dict[author_link] = 1
# 字典转换为dataframe
data = {"author_link": list(author_dict.keys()), "num": list(author_dict.values())}
result = DataFrame(data)
result.to_csv("author_link.csv")



