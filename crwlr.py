# Python Crawler using threads
# Mevin Babu
# mevinbabuc@gmail.com

import threading
from queue import Queue
from urllib.parse import urljoin
from urllib.request import urlretrieve
from collections import defaultdict

import requests
from bs4 import BeautifulSoup


# Initial seed URL list
urlList = ['http://ipm.ap.nic.in/water1/hyd.html']

# Queue to store links to be crawled
urlq = Queue()

# Dictionary to store unique URL values
UniqueURLs = defaultdict(int)

# LIMIT=5 Crawls only 5 links in a Queue.put LIMIT=-1 to crawl untill the end is reached
LIMIT = -1

lock = threading.RLock()                                                  

# Put the initial links into the Queue to get started
for link in urlList:
    urlq.put(link)

# Crawler
class LinkCrawler(threading.Thread):

    def __init__(self, urlq, UniqueURLs):
        threading.Thread.__init__(self)
        self.urlq = urlq
        self.UniqueURLs = UniqueURLs
        self.CRAWL = True

    def run(self):

        while self.CRAWL:
            print("Waiting for new URL")
            link = self.urlq.get()

            try:
                print("Waiting to fetch data")
                data = requests.get(link).content
            except:
                print("Check Your Internet Connection it has either disconnected or is slow")
                self.urlq.task_done()
                continue

            try:
                soup = BeautifulSoup(data, features="html.parser")
                for atag in soup.find_all('a'):
                    lnk = atag.get('href')

                    if lnk is None :
                        continue
                    # else:
                        # lnk = lnk.encode('utf-8')

                    if lnk in UniqueURLs:
                        continue

                    if lnk.startswith('http:') or lnk.startswith('https:'):

                        if lnk.lower().endswith('.pdf'):
                            # Lets download the resource
                            filename = lnk.split('/')[-1]
                            urlretrieve(lnk, f"data/{filename}")
                        
                        print(f"Normal links {lnk}")
                        self.urlq.put(lnk)

                    elif lnk.startswith('javascript:'):
                        print(f"Javascript call {lnk}")
                        continue

                    elif lnk.startswith('#'):
                        print("#tags")
                        continue

                    else :
                        # convert relative link into absolute link and add to Queue
                        self.urlq.put(urljoin(link, lnk))

            except Exception as e:
                print(f"Exception has occured with link - {lnk} :: Error: {str(e)}")

            with lock:
                # Put the visited link into a dictionary so that already crawled links can be avoided
                self.UniqueURLs[link] = 1
                print(link)

                if (len(self.UniqueURLs) > LIMIT) and LIMIT != -1:
                    # Set CRAWL to false to stop further crawling
                    self.CRAWL = False
                    print("LIMIT has reached The rest of the unique URLs will be added to the dictionary and saved!")
                   
                   # Put the rest of the unique link into the dictionary
                    while self.urlq.qsize():
                        lnk = self.urlq.get()

                        if lnk in self.UniqueURLs:
                            self.urlq.task_done()
                            continue

                        self.UniqueURLs[lnk] = 1
                        self.urlq.task_done()

            self.urlq.task_done()

        print("Threads Exiting")

# Thread Generation !
for i in range(5):
    lc = LinkCrawler(urlq, UniqueURLs)
    lc.setDaemon(True)
    lc.start()

# Waiting for the Threads till they finish!
urlq.join()

# Write the Unique links into a file or do other processing work
f = open("CrawlData.txt", 'wb')
for link in UniqueURLs:
    f.write(f"{link}\r\n".encode('utf-8'))

f.close()
