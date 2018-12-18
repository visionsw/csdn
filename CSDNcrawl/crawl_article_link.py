import re
import json
import requests
import time
import tqdm
from pandas import DataFrame
import threading

# step1-get_author_url,get_detail_url
# step2-

FAIL_LINK_SET = set()
ARTICLE_LINK_SET = set()


def get_page(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
               'cookie': 'TY_SESSION_ID=79e5cb5c-50e6-4199-b554-82a542e489a7; JSESSIONID=D8C20935B65E4A4DCAB4D8A4243F4654; uuid_tt_dd=10_20938293930-1519461289810-838748; kd_user_id=32417aec-28f4-421b-8e3a-69a162c72461; Hm_ct_6bcd52f51e9b3dce32bec4a3997715ac=1788*1*PC_VC; __utma=17226283.741878656.1520520072.1520688148.1520729997.3; UN=visionim; BT=1527004132286; smidV2=2018062409084080e46e79d72de7c50db4c8f8673fc9ac00b98f66bf13601a0; UM_distinctid=165cbf531ddb8-06f355f471f677-9393265-144000-165cbf531de756; ARK_ID=JSac8952b0dfd5afd4be897c2fc046eaf2ac89; dc_session_id=10_1540916288773.349031; _ga=GA1.2.741878656.1520520072; _gid=GA1.2.37367375.1542611876; __yadk_uid=xNB8xkhITGK8qyjrDwG3nXizp6KnGfoo; Hm_lvt_6bcd52f51e9b3dce32bec4a3997715ac=1542515110,1542523914,1542611875,1542614546; Hm_lpvt_6bcd52f51e9b3dce32bec4a3997715ac=1542615360; dc_tos=piflmn',
                'Referer': "https://so.csdn.net/so/search/s.do?p=1&q=%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0&t=blog&domain=&o=&s=&u=&l=&f=&rbg=0",#一定要有
               }
    try:
        r = requests.get(url=url, headers=headers)
        r.encoding = r.apparent_encoding
        return r.text
    except:
        print("The link:", url, "can't be requested")
        FAIL_LINK_SET.add(url)


def get_data_list_from_page2(page):
    index1 = page.find("{")
    json_data = json.loads(page[index1:len(page) - 1])
    datalist = json_data['blockData']

    return datalist


def get_url_from_page1(page):
    result = re.findall(r'(https://blog\.csdn\.net/.*/article/details/\d+)', page)
    result_set = set(result)

    return result_set


def get_url_from_page2(datalist):
    temp_url_set = set()
    for dataitem in datalist:
        temp_url_set.add(dataitem["linkurl"])

    return temp_url_set


def get_result_num(url):
    page = get_page(url)
    num = eval(re.findall(r'共(\d+)条结果', page)[0])
    return num


def get_all_links(url1, url2):

    page1 = get_page(url1)
    set1 = get_url_from_page1(page1)

    page2 = get_page(url2)
    dataList = get_data_list_from_page2(page2)
    set2 = get_url_from_page2(dataList)

    global ARTICLE_LINK_SET

    ARTICLE_LINK_SET = ARTICLE_LINK_SET | set1
    ARTICLE_LINK_SET = ARTICLE_LINK_SET | set2


class MyThread(threading.Thread):

    def __init__(self, url1_list, url2_list):
        threading.Thread.__init__(self)
        self.url1_list = url1_list
        self.url2_list = url2_list

    def run(self):
        for it in tqdm.trange(len(self.url1_list)):
            time.sleep(1)
            get_all_links(self.url1_list[it], self.url2_list[it])


if __name__ == '__main__':
    print("------start main threading-----")
    s = time.time()
    search_url1 = "https://so.csdn.net/so/search/s.do?q=%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0&t=blog&o=&s=&l="
    search_url2 = "https://gsp0.baidu.com/yrwHcjSl0MgCo2Kml5_Y_D3/api/customsearch/apisearch?s=10742016945123576423&q=%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0&nojc=1&ct=2&cc=blog.csdn.net&p=0&tk=5ff687f1302e609d08e92386af91fb15&v=2.0&callback=flyjsonp_C70840A3275A4A8C8EA126DFEF643A3C"

    result_num = get_result_num(search_url1)
    print("result number is:", result_num)
    page_num = int(result_num/20) + 1
    page_num = 1000

    print("------the page number is: ", page_num, "--------")
    # 构造url列表
    search_url1_list = []
    search_url2_list = []

    for i in range(page_num):
        search_url1_list.append("https://so.csdn.net/so/search/s.do?p=" + str(i+1) + "&q=%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0&t=blog&o=&s=&l=")
        search_url2_list.append("https://gsp0.baidu.com/yrwHcjSl0MgCo2Kml5_Y_D3/api/customsearch/apisearch?s=10742016945123576423&q=%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0&nojc=1&ct=2&cc=blog.csdn.net&p=" + str(i) + "&tk=5ff687f1302e609d08e92386af91fb15&v=2.0&callback=flyjsonp_C70840A3275A4A8C8EA126DFEF643A3C")

    thread_num = 10
    seg = int(len(search_url1_list)/thread_num)
    link_seg_list1 = []
    link_seg_list2 = []
    for i in range(thread_num):
        if i != thread_num - 1:
            link_seg_list1.append(search_url1_list[i * seg:(i + 1) * seg])
            link_seg_list2.append(search_url2_list[i * seg:(i + 1) * seg])
        else:
            link_seg_list1.append(search_url1_list[i * seg:])
            link_seg_list2.append(search_url2_list[i * seg:])

    print("-------url lists have been created------")

    # 构造线程列表
    thread_list = [MyThread(u_list[0], u_list[1]) for u_list in zip(link_seg_list1, link_seg_list2)]

    print("------threading list have been created------")

    # 开始线程
    for th in thread_list:
        th.start()
    # 等待所有线程完成
    for th in thread_list:
        th.join()

    print("------end main threading----")

    print(len(ARTICLE_LINK_SET))
    DataFrame(list(ARTICLE_LINK_SET)).to_csv("temp_resutl1000.csv")

    e = time.time()
    print("The time cost is ", e-s)