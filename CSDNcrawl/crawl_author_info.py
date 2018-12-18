import pandas
import re
import requests
from bs4 import BeautifulSoup
import threading
import tqdm
import time


AUTHOR_LINK_CSV_PATH = r"D:\pythonCoding\CSDNanalysis\data\link_data\author_link.csv"
AUTHOR_LINK_SET = set()
FAIL_LINK_SET = set()
RESULT_DICT = dict(url=[["null"]], description=[["null"]], creation_num=[["null"]], fan_num=[["null"]], like_num=[["null"]], comments_num=[["null"]], click_num=[["null"]], score=[["null"]],
                   rank=[["null"]], archive_dict=[["null"]], category_dict=[["null"]])


def get_page(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
               'cookie': 'TY_SESSION_ID=79e5cb5c-50e6-4199-b554-82a542e489a7; JSESSIONID=D8C20935B65E4A4DCAB4D8A4243F4654; uuid_tt_dd=10_20938293930-1519461289810-838748; kd_user_id=32417aec-28f4-421b-8e3a-69a162c72461; Hm_ct_6bcd52f51e9b3dce32bec4a3997715ac=1788*1*PC_VC; __utma=17226283.741878656.1520520072.1520688148.1520729997.3; UN=visionim; BT=1527004132286; smidV2=2018062409084080e46e79d72de7c50db4c8f8673fc9ac00b98f66bf13601a0; UM_distinctid=165cbf531ddb8-06f355f471f677-9393265-144000-165cbf531de756; ARK_ID=JSac8952b0dfd5afd4be897c2fc046eaf2ac89; dc_session_id=10_1540916288773.349031; _ga=GA1.2.741878656.1520520072; _gid=GA1.2.37367375.1542611876; __yadk_uid=xNB8xkhITGK8qyjrDwG3nXizp6KnGfoo; Hm_lvt_6bcd52f51e9b3dce32bec4a3997715ac=1542515110,1542523914,1542611875,1542614546; Hm_lpvt_6bcd52f51e9b3dce32bec4a3997715ac=1542615360; dc_tos=piflmn',
                'Referer': "https://so.csdn.net/so/search/s.do?p=1&q=%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0&t=blog&domain=&o=&s=&u=&l=&f=&rbg=0",#一定要有
               }
    try:
        r = requests.get(url=url, headers=headers)
        r.encoding = r.apparent_encoding
        return r.content

    except Exception as e:
        print(e, "||", "The link:", url, "can't be requested")
        global FAIL_LINK_SET
        FAIL_LINK_SET.add(url)


def get_data_from_csv():

    try:
        df = pandas.read_csv(AUTHOR_LINK_CSV_PATH)
        return list(df["author_link"])
    except Exception as e:
        print(e)


def get_author_data(page):
    page = str(page, encoding='utf-8')
    try:
        soup = BeautifulSoup(page, 'html.parser')

        # 所有获取项均为列表格式，不然后面会因为数目不同无法写进csv文件
        description = re.findall(r'<p class="description">([\w\W]*?)</p>', page)
        creation_num = re.findall(r'原创</a>[\w\W]*?<span class="count">([\w\W]*?)</span>', page)
        fan_num = re.findall(r'粉丝[\w\W]*?<span class="count" id="fan">([\w\W]*?)</span>', page)
        like_num = re.findall(r'喜欢[\w\W]*?<span class="count">([\w\W]*?)</span>', page)
        comments_num = re.findall(r'评论</dt>[\w\W]*?<span class="count">(\d*?)</span>', page)
        # 去换行，前后空格，下同
        click_num = re.findall(r'访问：</dt>[\w\W]*?>([\w\W]*?)</dd>', page)
        if click_num is not None:
            click_num = [item.replace("\n", "").strip() for item in click_num]

        score = re.findall(r'积分：</dt>[\w\W]*?>([\w\W]*?)</dd>', page)
        if score is not None:
            score = [item.replace("\n", "").strip() for item in score]

        rank = re.findall(r'排名：</dt>[\w\W]*?>([\w\W]*?)</dd>', page)

        archive_list = soup.find("ul", attrs={'class': "archive-list"})
        if archive_list is None:
            archive_dict = ["null"]
        else:
            stripped_list = list(archive_list.stripped_strings)
            archive_tuple = zip(stripped_list[0::2], stripped_list[1::2])
            archive_dict = [{item[0]: item[1] for item in archive_tuple}]

        category_list = soup.find("div", attrs={'id': "asideCategory"})
        if category_list is None:
            category_dict = ["null"]
        else:
            stripped_list = list(category_list.stripped_strings)[1:]
            category_tuple = zip(stripped_list[0::2], stripped_list[1::2])
            category_dict = [{item[0]: item[1] for item in category_tuple}]

        return [description, creation_num, fan_num, like_num, comments_num, click_num, score, rank, archive_dict, category_dict]
    except Exception as e:
        print(e)


class MyThread(threading.Thread):

    def __init__(self, urls):
        threading.Thread.__init__(self)
        self.url_list = urls

    def run(self):
        for it in tqdm.trange(len(self.url_list)):
            time.sleep(1)
            html = get_page(self.url_list[it])
            temp_data = get_author_data(html)
            RESULT_DICT["url"].append(self.url_list[it])

            if temp_data is not None:
                RESULT_DICT["description"].append(temp_data[0])
                RESULT_DICT["creation_num"].append(temp_data[1])
                RESULT_DICT["fan_num"].append(temp_data[2])
                RESULT_DICT["like_num"].append(temp_data[3])
                RESULT_DICT["comments_num"].append(temp_data[4])
                RESULT_DICT["click_num"].append(temp_data[5])
                RESULT_DICT["score"].append(temp_data[6])
                RESULT_DICT["rank"].append(temp_data[7])
                RESULT_DICT["archive_dict"].append(temp_data[8])
                RESULT_DICT["category_dict"].append(temp_data[9])
            else:
                RESULT_DICT["description"].append(["null"])
                RESULT_DICT["creation_num"].append(["null"])
                RESULT_DICT["fan_num"].append(["null"])
                RESULT_DICT["like_num"].append(["null"])
                RESULT_DICT["comments_num"].append(["null"])
                RESULT_DICT["click_num"].append(["null"])
                RESULT_DICT["score"].append(["null"])
                RESULT_DICT["rank"].append(["null"])
                RESULT_DICT["category_dict"].append(['null'])


def write_csv(name):
    df = pandas.DataFrame(RESULT_DICT)
    df.to_csv(name)


if __name__ == '__main__':
    all_author_link = get_data_from_csv()
    # for link in all_author_link:
    #     html = get_page(link)
    #     temp_data = get_author_data(html)
    #     RESULT_DICT["description"].append(temp_data[0])
    #     RESULT_DICT["creation_num"].append(temp_data[1])
    #     RESULT_DICT["fan_num"].append(temp_data[2])
    #     RESULT_DICT["like_num"].append(temp_data[3])
    #     RESULT_DICT["comments_num"].append(temp_data[4])
    #     RESULT_DICT["click_num"].append(temp_data[5])
    #     RESULT_DICT["score"].append(temp_data[6])
    #     RESULT_DICT["rank"].append(temp_data[7])
    #     RESULT_DICT["archive_dict"].append(temp_data[8])
    #     print(RESULT_DICT)
    # url线程切分
    thread_num = 10
    seg = int(len(all_author_link)/thread_num) + 1

    url_list = []
    for i in range(thread_num):
        if i != thread_num-1:
            url_list.append(all_author_link[i*seg:(i+1)*seg])
        else:
            url_list.append(all_author_link[i*seg:])

    print("-------url lists have been created------")
    # 构造线程列表
    thread_list = [MyThread(url_temp) for url_temp in url_list]

    print("------threading list have been created------")

    # 开始线程
    for th in thread_list:
        th.start()
    # 等待所有线程完成
    for th in thread_list:
        th.join()

    print("------end main threading----")

    write_csv("author_info1.csv")


