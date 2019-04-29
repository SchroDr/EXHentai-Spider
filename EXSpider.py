# -*- coding: utf-8 -*

import requests
import re
import pymysql
import random
import json
import threading
from bs4 import BeautifulSoup


class Spider():
    def __init__(self):
        self.session = requests.Session()
        self.jar = requests.cookies.RequestsCookieJar()
        self.session.keep_alive = False
        self.root_url = 'https://exhentai.org'
        self.head = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'close',
            'Cookie': '__cfduid=d27d3659b246fb2d6c775a475b696aa1e1531979918; ipb_member_id=4343255; ipb_pass_hash=e7e8fd94a626a458654ca9626fa1f669; s=1a8748db0; sk=lxxns586tbfbrgj7lt6w82d8d4kv; igneous=189ed0627; lv=1537884033-1538967966',
            'Host': 'exhentai.org',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36'
        }
        for cookie in self.head['Cookie'].split(';'):
            key, value = cookie.split('=', 1)
            self.jar.set(key, value)

        self.session.cookies.update(self.jar)

        self.db_pool = DbPool('localhost', 'root', 'mysql', 'exhentai')

        self.db_lock = threading.Lock()
        self.pioneer_lock = threading.Lock()
        self.ids_pool_lock = threading.Lock()

        self.db = self.db_pool.get_instance()
        self.cursor = self.db.cursor()

        self.pioneer_threads = 16
        self.zerg_threads = 64

        self.highpoints = self.subEmojiPattern()


    def getProxy(self):
        r = requests.get('http://127.0.0.1:8991/')
        ip_ports = json.loads(r.text)
        num = random.randint(0, len(ip_ports)-1)
        ip = ip_ports[num][0]
        port = ip_ports[num][1]
        proxies={
            'http':'http://%s:%s'%(ip,port),
            'https':'http://%s:%s'%(ip,port)
        }
        return proxies

    def deleteProxy(self, proxies):
        ip = re.match(r'http://(\S+):\d+', proxies['http']).group(1)
        requests.get('http://127.0.0.1:8991/delete?ip=%s' % ip)
        print(threading.current_thread().name + ': ' + 'Successfully delete %s' % ip)

    def getHtml(self, url):
        while True:
            try:
                proxies = self.getProxy()
                html = self.session.get(url = url, headers = self.head, proxies = proxies, timeout = 10)

                if BeautifulSoup(html.text, 'html5lib').find('title') == None:
                    self.deleteProxy(proxies)
                else:
                    return html
            except:
                #print("Failed with get %s" % url)
                pass

    #def getHtml(self, url):
    #    html = self.session.get(url = url, headers = self.head)
    #    return html

    def getLastPage(self, html):
        soup = BeautifulSoup(html.text, 'html5lib')
        page_num_tbl = soup.find('table', class_ = 'ptb')
        last_page = page_num_tbl.find_all('td')[-2].text
        return last_page

    def subEmojiPattern(self):
        try:  
            #python UCS-4 build的处理方式
            return re.compile(u'[\U00010000-\U0010ffff]')  
        except re.error:  
            #python UCS-2 build的处理方式 
            return re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')


    def dataFormatting(self, item):
        for key in item.keys():
            #if not isinstance(item[key], str):
            #    print(item[key])
            #    item[key] = str(item[key])
            #item[key] = "".join(item[key].split())
            item[key] = self.highpoints.sub(u' ', item[key])  
            item[key] = pymysql.escape_string(item[key])
            #item[key] = item[key].encode('utf-8').decode('utf-8')
        return item

    def insertAndUpdate(self, item):
        item = self.dataFormatting(item)
        #print(item['manga_pure_id'])
        keys = ', '.join(item.keys())
        #values = ', '.join(('"'+str(value)+'"') for value in item.values())
        values = ', '.join((value.join(('"', '"'))) for value in item.values())
        insert_sql = u"""INSERT INTO exhentai_info({keys})
        VALUES ({values}) ON DUPLICATE KEY UPDATE""".format(keys = keys, values = values)
        update = ','.join([u' {key} = "{value}"'.format(key = key, value = item[key]) for key in item.keys()])
        insert_sql += update
        with self.db_lock:
            try:
                self.cursor.execute(insert_sql)
                self.db.commit()
                return None
            except:
                self.db.rollback()
                print("Insert Error")
                print("The failled sql is %s" % insert_sql)
                return 1

    def updateLatestPage(self, latest_page):
        update_record_sql = 'UPDATE record SET page = %d WHERE id = 0' % latest_page
        with self.db_lock:
            try:
                self.cursor.execute(update_record_sql)
                self.db.commit()
            except:
                self.db.rollback()
                print("Failed to update page %d" % latest_page)

    def pioneerSelector(self, html):
        soup = BeautifulSoup(html.text, 'html5lib')
        table = soup.find('table', class_ = 'itg gltc')
        sources = table.find_all('tr')[1:]
        for source in sources:
            href = source.find('td', class_ = 'gl3c glname').find('a')['href']
            head = source.find('div', class_ = 'glink').text
            item = dict()
            item['manga_id'] = re.findall(r"\d+/\w+", href)[0]
            item['manga_pure_id'] = re.findall(r"(\d+)/\w+", item['manga_id'])[0]
            item['head'] = head
            yield item    

    def pioneer(self):
        print(threading.current_thread().name + ': Pioneer Begin')
        while True:
            with self.pioneer_lock:
                page = self.page
                self.page += 1

            url = 'https://exhentai.org/?page=%d' % page
            html = self.getHtml(url)
            for item in self.pioneerSelector(html):
                self.insertAndUpdate(item)
            if page > int(self.getLastPage(html)):
                print(threading.current_thread().name + ' has finished!')
                break
            self.updateLatestPage(page)

    def fightingPioneers(self):
        latest_page_sql = 'SELECT page FROM record WHERE id = 0;'
        self.cursor.execute(latest_page_sql)
        self.page = self.cursor.fetchone()[0] - self.pioneer_threads
        self.page = self.page if self.page >= 0 else 0

        threads = list()
        for i in range(self.pioneer_threads):
            t = threading.Thread(target = self.pioneer)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    def zergSelector(self, html, manga_id):
        soup = BeautifulSoup(html.text, 'html5lib')
        item = dict()
        try:
            if re.search('Gallery Not Available', soup.find('title').text) != None:
                item['manga_id'] = manga_id
                item['manga_pure_id'] = re.findall(r"(\d+)/\w+", manga_id)[0]
                item['removed'] = '1'
                item['crawled'] = '1'
                return item
            else:
                item['manga_id'] = manga_id
                item['manga_pure_id'] = re.findall(r"(\d+)/\w+", manga_id)[0]
                item['head'] = soup.find_all(name = 'h1', id = 'gn')[0].text
                item['subhead'] = soup.find_all(name = 'h1', id = 'gj')[0].text
                item['kind'] = soup.select('#gdc div')[0].text
                item['uploader'] = soup.select('#gdn a')[0].text
                item['time'] = soup.find_all(name = 'td', text = re.compile('Posted'))[0].next_sibling.text
                item['parent'] = soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.text
                item['parent_href'] = re.findall(
                                r"\d+/\w+", soup.find_all(name = 'td', text = re.compile('Parent'))[0].next_sibling.find('a')['href']
                                )[0] if item['parent'] != 'None' else 'None'
                item['visible'] = soup.find_all(name = 'td', text = re.compile('Visible'))[0].next_sibling.text
                item['language'] = soup.find_all(name = 'td', text = re.compile('Language'))[0].next_sibling.text
                item['language'] = item['language'].replace('TR', '')
                item['language'] = "".join(item['language'].split())
                item['file_size'] = (re.findall(r"\d+\.?\d*",
                                            soup.find_all(name = 'td', text = re.compile('File Size'))[0].next_sibling.text)[0])
                item['length'] = (re.findall(r"\d+",
                                            soup.find_all(name = 'td', text = re.compile('Length'))[0].next_sibling.text)[0])
                raw_favorited = re.findall(r"\d+",
                                        soup.find_all(name = 'td', text = re.compile('Favorited'))[0].next_sibling.text)
                item['favorited'] = (raw_favorited[0] if raw_favorited else '0')
                item['rating_count'] = (soup.find_all(name = 'span', id = 'rating_count')[0].text)
                item['average_rating'] = (re.findall(r"\d+\.?\d*", 
                                            soup.find_all(name = 'td', id = 'rating_label')[0].text)[0])

                artist_feature = list()
                group_feature = list()
                female_feature = list()
                male_feature = list()
                language_feature = list()
                character_feature = list()
                misc_feature = list()
                parody_feature = list()

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

                item['artist_feature'] = ';'.join(artist_feature)
                item['group_feature'] = ';'.join(group_feature)
                item['female_feature'] = ';'.join(female_feature)
                item['male_feature'] = ';'.join(male_feature)
                item['language_feature'] = ';'.join(language_feature)
                item['character_feature'] = ';'.join(character_feature)
                item['misc_feature'] = ';'.join(misc_feature)
                item['parody_feature'] = ';'.join(parody_feature)
                item['crawled'] = '1'
                return item
        except:
            item['manga_id'] = manga_id
            item['manga_pure_id'] = re.findall(r"(\d+)/\w+", manga_id)[0]
            item['failed'] = '1'
            return item


    def zerg(self):
        print(threading.current_thread().name + ': Zerg Begin')
        while True:
            with self.ids_pool_lock:
                id = self.manga_ids.pop()[0]
            url = 'https://exhentai.org/g/%s' % id
            html = self.getHtml(url)
            item = self.zergSelector(html, id)

            if self.insertAndUpdate(item) == 1:
                item = dict()
                item['manga_id'] = id
                item['manga_pure_id'] = re.findall(r"(\d+)/\w+", item['manga_id'])[0]
                item['failed'] = '1'
                self.insertAndUpdate(item)

    def zergNest(self):
        manga_ids_sql = 'SELECT manga_id FROM exhentai_info WHERE crawled = 0'
        self.cursor.execute(manga_ids_sql)
        self.manga_ids = list(self.cursor.fetchall())

        threads = list()
        for i in range(self.zerg_threads):
            t = threading.Thread(target = self.zerg)
            t.start()
            threads.append(t)
        for t in threads:
            t.join() 


class DbPool():
    def __init__(self, host, user, pwd, dbname, charset='utf8'):
        self.pool = {}
        self.host = host
        self.user = user
        self.pwd = pwd
        self.dbname = dbname
        
    def get_instance(self):
        name = threading.current_thread().name
        if name not in self.pool:
            connect = pymysql.connect(self.host, self.user, self.pwd, self.dbname)
            self.pool[name] = connect
        return self.pool[name]

spider = Spider()
#spider.fightingPioneers()
spider.zergNest()

