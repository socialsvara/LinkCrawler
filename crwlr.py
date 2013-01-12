# Python Crawler using threads
# Mevin Babu
# mevinbabuc@gmail.com


from Queue import Queue
import threading,urllib2,urlparse
from bs4 import BeautifulSoup
from collections import defaultdict


urlList=['http://python.org']                                           #Initial seed URL list
urlq=Queue()                                                            #Queue to store links to be crawled
UniqueURLs=defaultdict(int)                                             #Dictionary to store unique URL values

LIMIT=5                                                                 #LIMIT=5 Crawls only 5 links in a Queue.put LIMIT=-1 to crawl untill the end is reached

lock=threading.RLock()                                                  

for link in urlList:                                                    #Put the initial links into the Queue to get started
    urlq.put(link)

#Crawler
class LinkCrawler(threading.Thread):

    def __init__(self,urlq,UniqueURLs):
        threading.Thread.__init__(self)
        self.urlq=urlq
        self.UniqueURLs=UniqueURLs
        self.CRAWL=True

    def run(self):

        while self.CRAWL :
            #print "Waiting for new URL"
            link=self.urlq.get()                                        #get the Original Link

            try:
                #print "Waiting to fetch data "
                data=urllib2.urlopen(link,None,15).read()               #Fetch data form the Original Link with 15seconds as timeout
            except :
                print "Check Your Internet Connection it has either disconnected or is slow"                  
                self.urlq.task_done()
                continue
            try:
                soup=BeautifulSoup(data)
                for atag in soup.find_all('a'):                         #Get the list of all links within Original Link
                    lnk=atag.get('href')

                    if lnk is None :
                        continue
                    else:
                        lnk=lnk.encode('utf-8')                         #Convert the links to utf encoding

                    if lnk in UniqueURLs:                               #Avoid crawling visited links so that it doesnt go into a loop!
                        continue

                    if lnk.startswith('http:') or lnk.startswith('https:'):
                        #print "Normal links "+lnk
                        self.urlq.put(lnk)

                    elif lnk.startswith('javascript:'):                 #Continue loop if the link is a javascript call
                        #print "Javascript call "+lnk
                        continue

                    elif lnk.startswith('#'):                           #Continue loop if the link is a #tag/fragments
                        #print "#tags"
                        continue

                    else :
                        #print "Relative links "+urlparse.urljoin(link,lnk)
                        self.urlq.put(urlparse.urljoin(link,lnk)+"")    #convert relative link into absolute link and add to Queue

            except Exception,e:
                print "Exception has occured with this link",e,lnk


            with lock:
                self.UniqueURLs[link]=1                                 #Put the visited link into a dictionary so that already crawled links can be avoided
                print link
                if len(self.UniqueURLs)>LIMIT:
                    self.CRAWL=False                                    #Set CRAWL to false to stop further crawling
                    print "LIMIT has reached The rest of the unique URLs will be added to the dictionary and saved!"
                   
                    while self.urlq.qsize():                            #Put the rest of the unique link into the dictionary 
                        lnk=self.urlq.get()
                        if lnk in self.UniqueURLs:
                            self.urlq.task_done()
                            continue
                        self.UniqueURLs[lnk]=1
                        self.urlq.task_done()
            self.urlq.task_done()

        print "Threads Exiting"



#Thread Generation !

for i in range(5):
    lc=LinkCrawler(urlq,UniqueURLs)
    lc.setDaemon(True)
    lc.start()


#Waiting for the Threads till they finish!
urlq.join()

#Write the Unique links into a file or do other processing work
f=open("CrawlData.txt",'wb')
for link in UniqueURLs:
    f.write(link+"\r\n")

f.close()