import pandas
import requests
from bs4 import BeautifulSoup
import threading
import tqdm
import time
import csv


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


def get_article_data(page, link):
    try:
        soup = BeautifulSoup(page, "html.parser")
        main_tag = soup.find("main")
        if "空空如也" in main_tag.stripped_strings:
            return -1
        else:
            article_list_tag = soup.find_all("div", attrs={'class': 'article-item-box csdn-tracking-statistics'})
            if article_list_tag is not None:
                for item in article_list_tag:
                    t_ids = item['data-articleid']
                    t_text_list = list(item.stripped_strings)
                    if (t_text_list is not None) and (len(t_text_list) == 6):
                        temp_dict = dict()
                        # RESULT_DICT["link"].append([link])
                        # RESULT_DICT["id"].append([t_ids])
                        # RESULT_DICT["type"].append([t_text_list[0]])
                        # RESULT_DICT["title"].append([t_text_list[1]])
                        # RESULT_DICT["brief_des"].append([t_text_list[2]])
                        # RESULT_DICT["time"].append([t_text_list[3]])
                        # RESULT_DICT["read_num"].append([t_text_list[4]])
                        # RESULT_DICT["comments_num"].append([t_text_list[5]])
                        temp_dict["link"] = link
                        temp_dict["id"] = t_ids
                        temp_dict['type'] = t_text_list[0]
                        temp_dict['title'] = t_text_list[1]
                        temp_dict['brief_des'] = t_text_list[2]
                        temp_dict['time'] = t_text_list[3]
                        temp_dict['read_num'] = t_text_list[4]
                        temp_dict["comments_num"] = t_text_list[5]
                        writer.writerow(temp_dict)
                    # 处理无描述信息
                    elif (t_text_list is not None) and (len(t_text_list) == 5):
                        temp_dict = dict()
                        # RESULT_DICT["link"].append([link])
                        # RESULT_DICT["id"].append([t_ids])
                        # RESULT_DICT["type"].append([t_text_list[0]])
                        # RESULT_DICT["title"].append([t_text_list[1]])
                        # RESULT_DICT["brief_des"].append(['null'])
                        # RESULT_DICT["time"].append([t_text_list[2]])
                        # RESULT_DICT["read_num"].append([t_text_list[3]])
                        # RESULT_DICT["comments_num"].append([t_text_list[4]])
                        temp_dict["link"] = link
                        temp_dict["id"] = t_ids
                        temp_dict['type'] = t_text_list[0]
                        temp_dict['title'] = t_text_list[1]
                        temp_dict['brief_des'] = "null"
                        temp_dict['time'] = t_text_list[2]
                        temp_dict['read_num'] = t_text_list[3]
                        temp_dict["comments_num"] = t_text_list[4]
                        writer.writerow(temp_dict)
                    # 字符串列表为空
                    else:
                        # RESULT_DICT["link"].append(['null'])
                        # RESULT_DICT["id"].append(["null"])
                        # RESULT_DICT["type"].append(["null"])
                        # RESULT_DICT["title"].append(["null"])
                        # RESULT_DICT["brief_des"].append(["null"])
                        # RESULT_DICT["time"].append(["null"])
                        # RESULT_DICT["read_num"].append(["null"])
                        # RESULT_DICT["comments_num"].append(["null"])
                        temp_dict["link"] = link
                        temp_dict["id"] = t_ids
                        temp_dict['type'] = "null"
                        temp_dict['title'] = "null"
                        temp_dict['brief_des'] = "null"
                        temp_dict['time'] = "null"
                        temp_dict['read_num'] = "null"
                        temp_dict["comments_num"] = "null"
                        writer.writerow(temp_dict)
                return 0
            else:
                return -2

    except Exception as e:
        print(e)
        print("The wrong link and id is: ", link, t_text_list)


class MyThread(threading.Thread):

    def __init__(self, urls):
        threading.Thread.__init__(self)
        self.url_list = urls

    def run(self):
        for it in tqdm.trange(len(self.url_list)):
            ITER = 1
            while True:
                time.sleep(1)
                html = get_page(self.url_list[it] + "article/list/" + str(ITER))
                judge_code = get_article_data(html, self.url_list[it])
                if judge_code == -1:
                    break
                ITER += 1


def write_csv(name):
    df = pandas.DataFrame(RESULT_DICT)
    df.to_csv(name)


if __name__ == '__main__':
    header = {"link", "id", "type", "title", "brief_des", "time", "read_num", "comments_num"}
    writer_0 = open("article_info_all.csv", 'w', encoding='utf-8', newline="")
    writer = csv.DictWriter(writer_0, fieldnames=header)

    all_author_link = get_data_from_csv()

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
    writer_0.close()
    # write_csv("article_info.csv")


