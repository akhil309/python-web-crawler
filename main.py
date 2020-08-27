import pymongo
import requests
import time
import datetime
from bson import ObjectId
from bs4 import BeautifulSoup
from cfg import config
from pymongo import MongoClient
client = MongoClient()

db=client['python-web-crawler']
collections = db.Links

def get_file_name(name, content_type):
    if 'text/html' in content_type :
        return name+"."+'html'
    elif 'audio/aac' in content_type :
        return name+"."+'aac'
    elif 'video/x-msvideo' in content_type :
        return name+"."+'avi'
    elif 'application/vnd.amazon.ebook' in content_type :
        return name+"."+'azw'
    elif 'application/octet-stream' in content_type :
        return name+"."+'bin'
    elif 'image/bmp' in content_type :
        return name+"."+'bmp'
    elif 'application/x-bzip' in content_type :
        return name+"."+'bz'
    elif 'text/css' in content_type :
        return name+"."+'css'
    elif 'text/csv' in content_type :
        return name+"."+'csv'
    elif 'application/msword' in content_type :
        return name+"."+'doc'
    elif 'application/gzip' in content_type :
        return name+"."+'gz'
    elif 'image/gif' in content_type :
        return name+"."+'gif'
    elif 'text/calendar' in content_type :
        return name+"."+'ics'
    elif 'application/java-archive' in content_type :
        return name+"."+'jar'
    elif 'image/jpeg' in content_type :
        return name+"."+'jpeg'
    elif 'text/javascript' in content_type:
        return name+"."+'js'
    elif 'application/json' in content_type:
        return name+"."+'json'
    elif 'audio/mpeg' in content_type:
        return name+"."+'mp3'
    elif 'video/mpeg' in content_type:
        return name+"."+'mp4'
    elif 'image/png' in content_type:
        return name+"."+'png'
    elif 'application/pdf' in content_type:
        return name+"."+'pdf'
    elif 'application/rtf' in content_type:
        return name+"."+'rtf'
    elif 'text/plain' in content_type:
        return name+"."+'txt'
    elif 'application/xml' in content_type:
        return name+"."+'xml'
    elif 'application/zip' in content_type:
        return name+"."+'zip'
    else:
        return name

def crawler(url, file_path):
    base_link = url
    soup = BeautifulSoup(open(file_path),'html.parser')
    links = soup.find_all('a')

    for link in links:
        
        link_href = link.get('href')
        if link_href is not None:
            if 'http' in link_href and collections.count({'Link':link_href})==0:
                mydoc = {
                	"Link" : link_href,
                    "Source_link" : url,
                    "Is_crawled" : False,
                    "Last_crawl_dt" : None,
                    "Response_status" : 0,
                    "Content_type" : "",
                    "Content_length" : 0,
                    "File_path" : "",
                    "Created_at" : datetime.datetime.utcnow()
                }   
                collections.insert(mydoc)
            elif link_href[0]=='/' and collections.count({'Link':url+link_href})==0:
                
                mydoc = {
                	"Link" : url+link_href,
                    "Source_link" : url,
                    "Is_crawled" : False,
                    "Last_crawl_dt" : None,
                    "Response_status" : 0,
                    "Content_type" : "",
                    "Content_length" : 0,
                    "File_path" : "",
                    "Created_at" : datetime.datetime.utcnow()
                } 
                collections.insert(mydoc)  


while True:
    for document in collections.find():
        if collections.count()>=config['max_documents']:
            print('Maximum limit reached')
            break
        if document['Last_crawl_dt'] is not None and document['Last_crawl_dt'] <= time.time()-24*60*60*1000:
            continue
        
    
        url=document['Link']
        try:
            r = requests.get(url)

            if r.status_code != 200:
                continue
        
            content_type  = r.headers['content-type']

            file_name = get_file_name(str(document['_id']), content_type)
            file_path = './request_files/'+ str(file_name)

            new_document = {
                "Is_crawled" : True,
                "Last_crawl_dt" : time.time(),
                "Response_status" : r.status_code,
                "Content_type" : content_type,
                "Content_length" : len(r.content),
                "File_path" : file_path,
            }
            collections.update({"_id":ObjectId(document['_id'])}, {"$set":new_document})

            if 'text/html' not in content_type:
                file=open(file_path, "w")
                file.write(r.text)
                file.close()  
                continue
        
            file=open(file_path, "w")
            file.write(r.text)
            file.close()
            print('carwling... ',url)
            crawler(url, file_path)
        except:
            continue
        

    print('hello')  
    time.sleep(config['sleep_time'])