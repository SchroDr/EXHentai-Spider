import requests
from bs4 import BeautifulSoup
import re
import pymysql
import threading
import time
import random
import json

class Spider():
    def __init__(self):

        self.session = requests.Session()

        self.jar = requests.cookies.RequestsCookieJar()

        self.session.keep_alive = False

        self.root_url = 'https://exhentai.org/'

        self.head = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': '__cfduid=d27d3659b246fb2d6c775a475b696aa1e1531979918; ipb_member_id=4343255; ipb_pass_hash=e7e8fd94a626a458654ca9626fa1f669; s=1a8748db0; sk=lxxns586tbfbrgj7lt6w82d8d4kv; igneous=189ed0627; lv=1537884033-1538967966',
            'Host': 'exhentai.org',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36'
        }
        self.raw_cookies = '__cfduid=d27d3659b246fb2d6c775a475b696aa1e1531979918; ipb_member_id=4343255; ipb_pass_hash=e7e8fd94a626a458654ca9626fa1f669; s=1a8748db0; sk=lxxns586tbfbrgj7lt6w82d8d4kv; igneous=189ed0627; lv=1537884033-1538965327'

        for cookie in self.raw_cookies.split(';'):
            key, value = cookie.split('=', 1)
            self.jar.set(key, value)

        self.session.cookies.update(self.jar)

        self.db = pymysql.connect('localhost', 'root', '', 'exhentai', charset='utf8')

        self.cursor = self.db.cursor()

        self.proxy_ip_pool = []

        self.page_pool = []

        self.info_pool = []

        self.page_pool_status = True

        self.info_pool_status = 10

        self.harvest = 0

        #self.appKey = 'eUNiWlJ1WVllc2Q3TkF0UTpVQ2REWm1Vbk44bFhaQURm'

        #self.ip_port = 'transfer.mogumiao.com:9001'

        #self.head['Proxy-Authorization'] = 'Basic ' + self.appKey

        #self.proxy = {'http': 'http://' + self.ip_port, 'https://': self.ip_port}

    def getHtml(self, url, proxy_ip = None, count = 0):
        if(proxy_ip == None):
            proxy_ip = self.randomIp()
        if(count >= 5):
            if((proxy_ip in self.proxy_ip_pool)):
                self.proxy_ip_pool.remove(proxy_ip)
                print('----------------------')
                print('!!!!!!!!!!!!!!!!!!!!!!')
                print('Remove one IP!!!!!!')
                print('----------------------')
                print('!!!!!!!!!!!!!!!!!!!!!!')
            return self.getHtml(url)
        proxies = {
            'http': 'http://%s' % proxy_ip,
            'https': 'http://%s' % proxy_ip
        }
        try:
            #html = self.session.get(url = url, headers = self.head, proxies = self.proxy, verify=False,allow_redirects=False)
            html = self.session.get(url = url, headers = self.head, proxies = proxies, timeout = 5)
            return html
        #except requests.exceptions.ProxyError:
        #    if proxy_ip in self.proxy_ip_pool:
        #        self.proxy_ip_pool.remove(proxy_ip) 
        #    return self.getHtml(url)
        except requests.exceptions.ConnectionError:
            count += 1
            print('ConnectionError')
            time.sleep(2)
            return self.getHtml(url, proxy_ip, count)
        except requests.exceptions.SSLError:
            count += 1
            print('SSLError')
            time.sleep(2)
            return self.getHtml(url, proxy_ip, count)
        except requests.exceptions.ReadTimeout:
            count += 1
            print('ReadTimeout')
            time.sleep(2)
            return self.getHtml(url, proxy_ip, count)

    def getProxyIp(self, count):
        url = 'http://piping.mogumiao.com/proxy/api/get_ip_al?appKey=04282f5d7e974715bfe5f39808f28207&count={count}&expiryDate=0&format=1&newLine=2'.format(count = str(count))
        html = requests.get(url = url)
        raw_ips = json.loads(html.text)['msg']
        for raw_ip in raw_ips:
            ip = raw_ip['ip'] + ':' + raw_ip['port']
            self.proxy_ip_pool.append(ip)
            print(ip)


    def checkProxyIp(self, ip):
        if ip in self.proxy_ip_pool:
            print('Check: ' + ip)
            self.proxy_ip_pool.remove(ip)
            proxies = {
                'http': 'http://%s' % ip,
                'https': 'http://%s' % ip
            }
            time.sleep(3)
            status = False
            for i in range(3):
                try:
                    html = self.session.get(url = self.root_url, headers = self.head, proxies = proxies, timeout = 5)
                    status = html.status_code
                except requests.exceptions.ReadTimeout:
                    status = False
                finally:
                    if(status == 200):
                        self.proxy_ip_pool.append(ip)
                        print('IP:' + ip + ' is OK')
                        break
                time.sleep(3)

    def randomIp(self):
        print(len(self.proxy_ip_pool))
        if(len(self.proxy_ip_pool) <= 10):
            self.getProxyIp(5)
        return random.choice(self.proxy_ip_pool)
        
    def getPages(self, begin_page = 0, end_page = 0):
        
        print("Begin getPages()")

        number_pool = list(range(begin_page, end_page + 1))

        for number in number_pool:
            time.sleep(2)
            url = self.root_url + '?page=' + str(number)
            html = self.getHtml(url)
            soup = BeautifulSoup(html.text, 'html5lib')
            sources = soup.find_all(class_ = re.compile('gtr'))

            for source in sources:
                #kind = source.select('.itdc a img')[0]['alt']
                #time = source.find_all(name = 'td', class_ = 'itd')[0].text
                #name = source.select('.it5 a')[0].text
                href = source.select('.it5 a')[0]['href']

                self.page_pool.append(href)



        self.page_pool_status = False
        print('++++++++++++++++++++++++++++++++++++++++++++++++')
        print('All pages have been found!!!!!!!!!!!!!!!!!!!!!!!')
        print('++++++++++++++++++++++++++++++++++++++++++++++++')

    def getInfo(self, href):

        html = self.getHtml(href)
        soup = BeautifulSoup(html.text, 'html5lib')

        manga_id = re.findall(r"\d+/\w+", href)[0]
        manga_pure_id = re.findall(r"(\d+)/\w+", manga_id)[0]
        head = soup.find_all(name = 'h1', id = 'gn')[0].text
        subhead = soup.find_all(name = 'h1', id = 'gj')[0].text
        kind = soup.select('#gdc a img')[0]['alt']
        uploader = soup.select('#gdn a')[0].text
        time = soup.find_all(name = 'td', text = re.compile('Posted'))[0].next_sibling.text
        parent = soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.text
        parent_href = re.findall(
                        r"\d+/\w+", soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.find('a')['href']
                        )[0] if parent != 'None' else 'None'
        visible = soup.find_all(name = 'td', text = re.compile('Visible'))[0].next_sibling.text
        language = soup.find_all(name = 'td', text = re.compile('Language'))[0].next_sibling.text
        file_size = re.findall(r"\d+\.?\d*",
                                    soup.find_all(name = 'td', text = re.compile('File Size'))[0].next_sibling.text)[0]
        length = re.findall(r"\d+",
                                    soup.find_all(name = 'td', text = re.compile('Length'))[0].next_sibling.text)[0]
        raw_favorited = re.findall(r"\d+",
                                soup.find_all(name = 'td', text = re.compile('Favorited'))[0].next_sibling.text)
        favorited = raw_favorited[0] if raw_favorited else '0'
        rating_count = soup.find_all(name = 'span', id = 'rating_count')[0].text
        avrage_rating = re.findall(r"\d+\.?\d*", 
                                    soup.find_all(name = 'td', id = 'rating_label')[0].text)[0]

        artist_feature = []
        group_feature = []
        female_feature = []
        male_feature = []
        language_feature = []
        character_feature = []
        misc_feature = []
        parody_feature = []

        for feature in soup.find_all(name = 'a', id = re.compile('ta_')):
            if(re.search('artist', feature['id'])):
                artist_feature.append(feature.text)
            elif(re.search('group', feature['id'])):
                group_feature.append(feature.text)
            elif(re.search('female', feature['id'])):
                female_feature.append(feature.text)
            elif(re.search('male', feature['id'])):
                male_feature.append(feature.text)
            elif(re.search('language', feature['id'])):
                language_feature.append(feature.text)
            elif(re.search('character', feature['id'])):
                character_feature.append(feature.text)
            elif(re.search('misc', feature['id'])):
                misc_feature.append(feature.text)
            elif(re.search('parody', feature['id'])):
                parody_feature.append(feature.text)
    
        print('Manga_id: ' + manga_id)

        self.info_pool.append({
            'manga_pure_id': manga_pure_id,
            'manga_id': manga_id,
            'head': head.replace('\'', '\\\'').replace('\"', '\\\"'),
            'subhead': subhead.replace('\'', '\\\'').replace('\"', '\\\"'),
            'kind': kind,
            'uploader': uploader,
            'time': time,
            'parent': parent,
            'parent_href': parent_href,
            'visible': visible,
            'language': language,
            'file_size': float(file_size),
            'length': int(length),
            'favorited': int(favorited),
            'rating_count': int(rating_count),
            'average_rating': float(avrage_rating),
            'artist_feature': ';'.join(artist_feature),
            'group_feature': ';'.join(group_feature),
            'female_feature': ';'.join(female_feature),
            'male_feature': ';'.join(male_feature),
            'language_feature': ';'.join(language_feature),
            'character_feature': ';'.join(character_feature),
            'misc_feature': ';'.join(misc_feature),
            'parody_feature': ';'.join(parody_feature)
        })

    def saveInfo(self, info):
        #print(info)
        keys = ', '.join(info.keys())
        values = ', '.join(('"'+str(value)+'"') for value in info.values())
        save_sql = """INSERT INTO exhentai_info({keys})
                VALUES ({values}) ON DUPLICATE KEY UPDATE""".format(keys = keys, values = values)
        update = ','.join([" {key} = %s".format(key = key) for key in info])
        save_sql += update
        #print(save_sql)
        try:
            self.cursor.execute(save_sql, tuple(info.values()))
            self.db.commit()
            self.harvest += 1
            print('Present Harvest: ' + str(self.harvest))

        except pymysql.err.IntegrityError:
            print('----------------------')
            print('!!!!!!!!!!!!!!!!!!!!!!')
            print('Already in MySQL!!!!!!')
            print('----------------------')
            print('!!!!!!!!!!!!!!!!!!!!!!')
            self.db.rollback()
        except pymysql.err.DataError:
            print('----------------------')
            print('!!!!!!!!!!!!!!!!!!!!!!')
            print('Too Long!!!!!!!!!!!!!!')
            print('----------------------')
            print('!!!!!!!!!!!!!!!!!!!!!!')
            self.db.rollback()

    def getInfoFromPool(self):
        print('Begin getInfoFromPool()')
        while True:
            print('Page Pool: ' + str(len(self.page_pool)))
            print('Info Pool: ' + str(len(self.info_pool)))
            try:
                time.sleep(2)
                self.getInfo(self.page_pool.pop(0))
            except IndexError:
                if (self.page_pool_status == False):
                    self.info_pool_status -= 1
                    print("End!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    break

    def saveInfoFromPool(self):
        print('Begin saveInfoFromPool()')
        while True:
            try:
                self.saveInfo(self.info_pool.pop(0))
            except IndexError:
                if (self.info_pool_status == 0):
                    print("End!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    break

    def begin(self):
        self.getPages()
        self.getInfo(self.page_pool.pop(0))
        self.saveInfo(self.info_pool.pop(0))

        print("Close Database")
        self.db.close()

    def multiBegin(self):
        get_page_thread = threading.Thread(target = self.getPages, args = (100, 200))
        get_info_threads = [threading.Thread(target = self.getInfoFromPool) for i in range(10)]
        save_info_thread = threading.Thread(target = self.saveInfoFromPool)

        get_page_thread.start()
        for thread in get_info_threads:
            thread.start()
        save_info_thread.start()

        get_page_thread.join()
        for thread in get_info_threads:
            thread.join()
        save_info_thread.join()

        print("Close Database")
        self.db.close()

    def getTags(self):
        sql = 'SELECT id FROM exhentai_manga_info'

        self.cursor.execute(sql)

        id_list = []

        tag_list = []

        count = 0

        for i in range(500):
            id_list.append(self.cursor.fetchone()[0])

        for manga_id in id_list:
            url = 'https://exhentai.org/g/' + manga_id
            html = self.getHtml(url)
            soup = BeautifulSoup(html.text, 'html5lib')
            parent = soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.text
            parent_href = re.findall(
                        r"\d+/\w+", soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.find('a')['href']
                        )[0] if parent != 'None' else 'None'
            
            print(parent_href)


EXSpider = Spider()
#EXSpider.begin()
#EXSpider.getProxyIp(20)
EXSpider.multiBegin()
#EXSpider.getTags()

