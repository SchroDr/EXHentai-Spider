import requests
from bs4 import BeautifulSoup
import re
import pymysql
import threading
import time
import random

class Spider():
    def __init__(self):

        self.session = requests.Session()

        self.jar = requests.cookies.RequestsCookieJar()

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

        self.info_pool_status = 4

        self.harvest = 0

        self.appKey = 'eUNiWlJ1WVllc2Q3TkF0UTpVQ2REWm1Vbk44bFhaQURm'

        self.ip_port = 'transfer.mogumiao.com:9001'

        self.head['Proxy-Authorization'] = 'Basic ' + self.appKey

        self.proxy = {'http': 'http://' + self.ip_port, 'https://': self.ip_port}

    def getProxyIp(self):
        url = 'https://www.kuaidaili.com/free/inha/1/'
        html = requests.get(url = url, headers = self.head, proxies = self.proxy, verify=False,allow_redirects=False)
        soup = BeautifulSoup(html.text, 'html5lib')
        raw_ips = soup.find_all(name = 'table', class_ = 'table table-bordered table-striped')[0].find_all(name = 'tbody')[0].find_all(name = 'tr')

        for raw_ip in raw_ips:
            raw = raw_ip.find_all(name = 'td')
            ip = raw[0].text + ':' + raw[1].text
            self.proxy_ip_pool.append(ip)

        for ip in self.proxy_ip_pool:
            proxies = {
                'http': 'https://27.159.137.35:48103',
                'https': 'http://27.159.137.35:48103'
            }
            print(proxies)
            try:
                self.session.get(url = self.root_url, headers = self.head, proxies = self.proxy, verify=False,allow_redirects=False)
            except:
                print('Failed')
            else:
                print('Successful')

    def randomIp(self):
        return {
            'http': 'http://%s' % random.choice(self.proxy_ip_pool),
            'https': 'http://%s' % random.choice(self.proxy_ip_pool)
        }
        
    def getPages(self, begin_page = 0, end_page = 0):
        
        print("Begin getPages()")

        for number in range(begin_page, end_page + 1):

            url = self.root_url + '?page=' + str(number)
            html = self.session.get(url = url, headers = self.head, proxies = self.proxy, verify=False,allow_redirects=False)
            soup = BeautifulSoup(html.text, 'html5lib')
            sources = soup.find_all(class_ = re.compile('gtr'))

            for source in sources:

                #kind = source.select('.itdc a img')[0]['alt']
                #time = source.find_all(name = 'td', class_ = 'itd')[0].text
                #name = source.select('.it5 a')[0].text
                href = source.select('.it5 a')[0]['href']

                #print('-------------------------------------')
                #print('Href: ' + href)
                #print('-------------------------------------')

                self.page_pool.append(href)

        self.page_pool_status = False
        print('++++++++++++++++++++++++++++++++++++++++++++++++')
        print('All pages have been found!!!!!!!!!!!!!!!!!!!!!!!')
        print('++++++++++++++++++++++++++++++++++++++++++++++++')

    def getInfo(self, href):

        html = self.session.get(url = href, headers = self.head, proxies = self.proxy, verify=False,allow_redirects=False)
        soup = BeautifulSoup(html.text, 'html5lib')

        manga_id = re.findall(r"\d+/\w+", href)[0]
        head = soup.find_all(name = 'h1', id = 'gn')[0].text
        subhead = soup.find_all(name = 'h1', id = 'gj')[0].text
        kind = soup.select('#gdc a img')[0]['alt']
        uploader = soup.select('#gdn a')[0].text
        time = soup.find_all(name = 'td', text = re.compile('Posted'))[0].next_sibling.text
        #parent = re.findall(r"\d+/\w+", 
        #                            soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling[href])[0] if soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.text != 'None' else 'None'
        parent = soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.text
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
        features = ""
        for feature in soup.find_all(name = 'a', id = re.compile('ta_')):
            features += (feature.text + ';')

        #group = soup.find_all(name = 'a', id = re.compile('ta_group'))[0].text if len(soup.find_all(name = 'a', id = re.compile('ta_group'))) != 0 else 'Null'
        #artist = soup.find_all(name = 'a', id = re.compile('ta_artist'))[0].text if len(soup.find_all(name = 'a', id = re.compile('ta_group'))) != 0 else 'Null'
        #male_features = ''
        #if len(soup.find_all(name = 'a', id = re.compile('ta_male'))) != 0:
        #    for feature in soup.find_all(name = 'a', id = re.compile('ta_male')):
        #        male_features += (';' + feature.text)
        #else:
        #    male_features = 'Null'
        #female_features = ''
        #if len(soup.find_all(name = 'a', id = re.compile('ta_female'))) != 0:
        #    for feature in soup.find_all(name = 'a', id = re.compile('ta_female')):
        #        female_features += (';' + feature.text)
        #else:
        #    female_features = 'Null'
        
        print('Manga_id: ' + manga_id)
        #print('Head: ' + head)
        #print('Subhead: ' + subhead)
        #print('Kind: ' + kind)
        #print('Uploader: ' + uploader)
        #print('Time: ' + time)
        #print('Parent: ' + parent)
        #print('Visible: ' + visible)
        #print('Language: ' + language)
        #print('File_size: ' + file_size)
        #print('Length: ' + length)
        #print('Favorited: ' + favorited)
        #print('Rating_count: ' + rating_count)
        #print('Average_rating: ' + avrage_rating)
        #print('Features: ' + features)
        #print('----------------------------------------------')

        self.info_pool.append({
            'manga_id': manga_id,
            'head': head.replace('\'', '\\\'').replace('\"', '\\\"'),
            'subhead': subhead.replace('\'', '\\\'').replace('\"', '\\\"'),
            'kind': kind,
            'uploader': uploader,
            'time': time,
            'parent': parent,
            'visible': visible,
            'language': language,
            'file_size': float(file_size),
            'length': int(length),
            'favorited': int(favorited),
            'rating_count': int(rating_count),
            'average_rating': float(avrage_rating),
            'features': features
        })

    def saveInfo(self, info):
        save_sql = """INSERT INTO exhentai_manga_info(
                id, head, subhead, kind, uploader, time, parent, visible, 
                language, file_size, length, favorited, rating_count, average_rating, features)
                VALUES ("%s", "%s", "%s", "%s", 
                        "%s", "%s", "%s", "%s", 
                        "%s", "%f", "%d", "%d", 
                        "%d", "%f", "%s")""" % \
                (info['manga_id'], info['head'], info['subhead'], info['kind'], 
                info['uploader'], info['time'], info['parent'], info['visible'], 
                info['language'], info['file_size'], info['length'], info['favorited'], 
                info['rating_count'], info['average_rating'], info['features'])

        try:
            self.cursor.execute(save_sql)
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

    def getInfoFromPool(self):
        print('Begin getInfoFromPool()')
        while True:
            print('Left to get: ' + str(len(self.page_pool)))
            try:
                time.sleep(0.5)
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
                #self.saveInfo(self.getInfo(self.page_pool.pop(0)))
                self.saveInfo(self.info_pool.pop(0))
            except:
                if (self.info_pool_status == 0):
                    print("End!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    break

    def begin(self):
        get_page_threading = threading.Thread(target = self.getPages, args = (1, 10000))
        save_info_threading = threading.Thread(target = self.saveInfoFromPool)
        #save_info_threads = [threading.Thread(target = self.saveInfoFromPool) for i in range(8)]
        get_page_threading.start()
        save_info_threading.start()
        #for thread in save_info_threads:
        #    thread.start()

        get_page_threading.join()
        save_info_threading.join()
        #for thread in save_info_threads:
        #    thread.join()
        print("Close Database")
        self.db.close()

    def multiBegin(self):
        get_page_thread = threading.Thread(target = self.getPages, args = (75, 100))
        get_info_threads = [threading.Thread(target = self.getInfoFromPool) for i in range(4)]
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

EXSpider = Spider()
#EXSpider.getProxyIp()
EXSpider.multiBegin()
