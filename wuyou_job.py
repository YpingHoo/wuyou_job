#_*_ coding:utf-8 _*_

"""
 通过requests等模块，通过城市和关键词搜索获取前程无忧网站相关工作信息，
    并将工作信息保存至mysql数据库。
"""

import area_code
import requests
from bs4 import BeautifulSoup
import re
import pymysql.cursors

#对城市码键值互换,以方便提取城市所对应代码
new_area={city:code for code,city in area_code.area.items()}

#设置mysql数据表名称
global table_name
table_name = input("请输入数据表名称[如:tianjin_python]: ")

#工作查询起始页链接
def make_first_url():
    flag = True
    while flag:
        city = input("请输入工作城市名称: ")
        if city not in new_area.keys():
            print("城市输入有误!! 请重新输入!!")
            flag = True
        else:
            flag = False
    #对关键词进行处理,中文情况下转换为相应形式
    keyword = str(input("请输入工作关键词: ").encode('utf-8')).replace(r'\x', '%').split('\'')[1]

    url = 'http://search.51job.com/jobsearch/search_result.php?fromJs=1&jobarea=%s&keyword=%s' \
          '&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9' %(new_area[city],keyword)
    return url

#获取网页页面
def get_content(url):

    header = {
        'Origin' : 'http://search.51job.com',
        'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
    }


    html = requests.get(url,header).content
    soup = BeautifulSoup(html,'html.parser')
    return soup

#获取工作信息
def get_job_information_save_mysql(soup):
    listjob_div = soup.find('div',class_='dw_table')
    list_job = listjob_div.findAll('div',class_=re.compile('el$'))
    #列表中第一项数据不合格,删除掉
    del list_job[0]
    #对获取到的工作进行计数
    count = 0
    for job in list_job:
        position = job.a.get_text().strip()                                     #职位信息
        company = job.find('span', class_='t2').a.get_text().strip()            #公司名称
        address = job.find('span', class_='t3').get_text().strip()              #公司地址
        salary = job.find('span', class_='t4').get_text().strip()               #薪资情况
        release_time = job.find('span',class_='t5').get_text().strip()          #工作发布时间
        #保存至数据库
        try:
            save_to_mysql(table_name,position,company,address,salary,release_time)
        except:
            print("请检查数据库服务！！！")


        print('*' * 40)
        print("职位:     " + position)
        print("公司名称: " + company)
        print("公司地址: " + address)
        print("薪资:     " + salary)
        print("发布时间: " + release_time)
        print('*' * 40)
        print('\n')

        #对工作计数
        count += 1

    return count


#获取下一页链接
def get_next_href(soup):
    try:
        next_page = soup.findAll('li',class_='bk')[1]
        next_href = next_page.a.get('href')
        return next_href
    except:
        print('获取完毕!!')

def save_to_mysql(table_name,position,company,address,salary,release_time):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='password',
        db='qcwy_job',
        charset='utf8'
    )
    try:
        with connection.cursor() as cursor:
            #新建表
            create_sql = "create table IF NOT EXISTS `%s` (`id` int(11) PRIMARY KEY AUTO_INCREMENT NOT NULL ,`position` VARCHAR(500) NOT NULL," \
                         "`company` VARCHAR(500) NOT NULL ,`address` VARCHAR(500) NOT NULL ,`salary` VARCHAR(255) NOT NULL ," \
                         "`release_time` VARCHAR(255) NOT NULL )" %table_name
            #设置表存储编码,以存储中文
            set_charset = "alter table %s CONVERT TO CHARACTER SET utf8" %table_name
            #设置编码,以查询
            set_name_charset = "set names utf8"
            cursor.execute(create_sql)
            cursor.execute(set_charset)
            cursor.execute(set_name_charset)
            #插入数据
            insert_sql = "insert into `%s` (`position`,`company`,`address`,`salary`,`release_time`)" %table_name +"VALUES(%s,%s,%s,%s,%s)"
            cursor.execute(insert_sql, (position, company, address, salary, release_time))

            connection.commit()
    finally:
        connection.close()

def run_app():
    url = make_first_url()
    #工作计数总数
    count_total = 0
    #在url不为空的情况下,循环获取,直至下一页[href]为空(最后一页)
    while url:
        soup = get_content(url)
        count = get_job_information_save_mysql(soup)
        count_total = count_total + count
        url = get_next_href(soup)
    print("查询到有%s个相关职位信息" %count_total)


#程序运行
run_app()


