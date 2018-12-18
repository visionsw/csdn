import pandas
import requests
from bs4 import BeautifulSoup
import threading
import tqdm
import time


AUTHOR_LINK_CSV_PATH = r"D:\pythonCoding\CSDNanalysis\data\link_data\author_link.csv"
AUTHOR_LINK_SET = set()
FAIL_LINK_SET = set()
RESULT_DICT = dict(link=[['null']], id=[['null']], type=[['null']], title=[['null']], brief_des=[['null']], time=[['null']], read_num=[['null']], comments_num=[['null']])


def get_page(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
               'cookie': 'TY_SESSION_ID=79e5cb5c-50e6-4199-b554-82a542e489a7; JSESSIONID=D8C20935B65E4A4DCAB4D8A4243F4654; uuid_tt_dd=10_20938293930-1519461289810-838748; kd_user_id=32417aec-28f4-421b-8e3a-69a162c72461; Hm_ct_6bcd52f51e9b3dce32bec4a3997715ac=1788*1*PC_VC; __utma=17226283.741878656.1520520072.1520688148.1520729997.3; UN=visionim; BT=1527004132286; smidV2=2018062409084080e46e79d72de7c50db4c8f8673fc9ac00b98f66bf13601a0; UM_distinctid=165cbf531ddb8-06f355f471f677-9393265-144000-165cbf531de756; ARK_ID=JSac8952b0dfd5afd4be897c2fc046eaf2ac89; dc_session_id=10_1540916288773.349031; _ga=GA1.2.741878656.1520520072; _gid=GA1.2.37367375.1542611876; __yadk_uid=xNB8xkhITGK8qyjrDwG3nXizp6KnGfoo; Hm_lvt_6bcd52f51e9b3dce32bec4a3997715ac=1542515110,1542523914,1542611875,1542614546; Hm_lpvt_6bcd52f51e9b3dce32bec4a3997715ac=1542615360; dc_tos=piflmn',
                'Referer': "https://so.csdn.net/so/search/s.do?p=1&q=%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0&t=blog&domain=&o=&s=&u=&l=&f=&rbg=0",#一定要有
               }
    try:
        r = requests.get(url=url, headers=headers)
        r.encoding = r.apparent_encoding
        return r.text

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


def get_article_data(page):
    try:
        soup = BeautifulSoup(page, "html.parser")
        main_tag = soup.find("main")
        if "空空如也" in main_tag.stripped_strings:
            return 0
        else:

            return 1
    except Exception as e:
        print(e)


class MyThread(threading.Thread):

    def __init__(self, urls):
        threading.Thread.__init__(self)
        self.url_list = urls

    def run(self):
        for it in tqdm.trange(len(self.url_list)):
            time.sleep(1)
            while True:
                ITER = 1
                html = get_page(self.url_list[it] + "article/list/" + str(ITER))
                temp_data = get_article_data(html)
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
                else:
                    RESULT_DICT["description"].append(["null"])
                    RESULT_DICT["creation_num"].append(["null"])
                    RESULT_DICT["fan_num"].append(["null"])
                    RESULT_DICT["like_num"].append(["null"])
                    RESULT_DICT["comments_num"].append(["null"])
                    RESULT_DICT["click_num"].append(["null"])
                    RESULT_DICT["score"].append(["null"])
                    RESULT_DICT["rank"].append(["null"])
                    RESULT_DICT["archive_dict"].append(["null"])


def write_csv(name):
    df = pandas.DataFrame(RESULT_DICT)
    df.to_csv(name)


if __name__ == '__main__':
    url = "https://blog.csdn.net/kayv/article/list/11"
    html = get_page(url)
    print(get_article_data(html))
    # soup = BeautifulSoup(html, "html.parser")
    # article_list_tag = soup.find_all("div", attrs={'class': 'article-item-box csdn-tracking-statistics'})
    # print(len(article_list_tag))
    # for item in article_list_tag:
    #     id = item['data-articleid']
    #     text_list = list(item.stripped_strings)
    #     # print(str(text_list[2]).replace("\n", ""))
    #
    #     RESULT_DICT["link"].append([url])
    #     RESULT_DICT["id"].append([id])
    #     RESULT_DICT["type"].append([text_list[0]])
    #     RESULT_DICT["title"].append([text_list[1]])
    #     RESULT_DICT["brief_des"].append([text_list[2]])
    #     RESULT_DICT["time"].append([text_list[3]])
    #     RESULT_DICT["read_num"].append([text_list[4]])
    #     RESULT_DICT["comments_num"].append([text_list[5]])
    #
    # print(RESULT_DICT)











