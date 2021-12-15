from TikTokApi import TikTokApi
from pika import connection
import requests
from pathlib import Path
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
import logging
import logging.handlers as handlers
import mysql.connector
import config
import pika
import ftplib
import schedule
import time
from datetime import datetime
import os
import json

print("start config tiktok!!!!!")
api = TikTokApi.get_instance() 
print("end config tiktok!!!!!")

logger = logging.getLogger('my tool')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler('logs/time_app.log' , when='M' , interval=1 , backupCount=2)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)

errorlogHandler = handlers.RotatingFileHandler('logs/error.log' , maxBytes=5000 , backupCount=0)
errorlogHandler.setLevel(logging.ERROR)
errorlogHandler.setFormatter(formatter)

logger.addHandler(logHandler)
logger.addHandler(errorlogHandler)

results = 5

class Video:
    def __init__(self, id , id_tiktok , link , folder):
        self.id = id 
        self.id_tiktok = id_tiktok
        self.link = link
        self.folder = folder

def downloadVideoByUrl(tiktok) :
    try :
        print("start !!!!!")
        video_url = 'https://www.tiktok.com/@%s/video/%s' % (tiktok['author']['uniqueId'], tiktok['id'])
        print("start down urrl {}".format(video_url))
        logger.info("start download video url = {}".format(video_url))
        data = api.get_video_by_url(video_url, return_bytes = 1 , language='en' , proxy = None ,  custom_verifyFp="")

        today = datetime.now().strftime('%Y%m%d')
        
        with open('videos/%s/%s.mp4' % (today,tiktok['id']) , 'wb') as output:
            output.write(data)
            logger.info("sucess save file "+tiktok['id'])


        return True
    
    except Exception as exc:
        logger.error("Error download video tikton detail {}".format(exc))
        return False


def downloadVideoByUrlFinal(id , id_tiktok , embed_link , created_at) :
    try :
        logger.info("start process videoId={} , id_tiktok = {} , link = {}".format(id , id_tiktok , embed_link))
        logger.info("update status in process videoid ={}".format(id))
        updatedaMysql(id,-1)
        logger.info("start download video url = {}".format(embed_link))
        data = api.get_video_by_url(embed_link, return_bytes = 1 , language='en' , proxy = None ,  custom_verifyFp="")

        today = datetime.now().strftime('%Y%m%d')
        creatFolerToday(today)
        
        with open('videos/%s/%s.mp4' % (today,id_tiktok) , 'wb') as output:
            output.write(data)
            logger.info("sucess save file {}.mp4".format(id_tiktok))
            logger.info("update status success id {}".format(id))
            updatedaMysql(id,10)
            logger.info("start push queue id {}".format(id))
            pushMessageTorabbit("hello")


        return True
    
    except Exception as exc:
        logger.error("Error download video tikton detail {}".format(exc))
        updatedaMysql(id,-1)
        return False

def getDataMySql():
    try :
        logger.info("start get database form mysql")
        mydb = mysql.connector.connect(
            host=config.mysqlHost,
            user=config.mysqlUser,
            password=config.mysqlPass,
            database=config.mysqlDatabase
        )

        mycusor = mydb.cursor()

        sql = "SELECT id , id_tiktok , embed_link , created_at FROM video_items_tiktok WHERE status = {} LIMIT = 10".format(1)
        mycusor.execute(sql)

        myresult = mycusor.fetchall()

        for result in myresult:
            print("id = {} , id_tiktok = {} , embed_link = {}".format(result[0],result[1],result[2]))
            downloadVideoByUrlFinal(result[0],result[1],result[2],result[3])
        
        # with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        #         feature_result = {executor.submit(downloadVideoByUrlFinal,result[0],result[1],result[2]) : result for result in myresult}

        #         for data in concurrent.futures.as_completed(feature_result) :
        #             try:
        #                 kq2 = data.result()
        #                 logging.info(kq2)
        #             except Exception as exc :
        #                 logger.error("Error download")

        
        return myresult
    except Exception as exc:
        logger.error("Error get data from mysql {}".format(exc))


def updatedaMysql(id , status):
    try :
        logger.info("start update id = {} status = {}".format(id,status))
        mydb = mysql.connector.connect(
            host=config.mysqlHost,
            user=config.mysqlUser,
            password=config.mysqlPass,
            database=config.mysqlDatabase
        )

        mycusor = mydb.cursor()

        sql = "UPDATE customs SET status = {}".format(status)

        mycusor.execute(sql)

        mydb.commit()

        logger.info(mycusor.rowcount )

        logger.error("Success update id = {} status = {}".format(id, status))
    except Exception as exc:
        logger.error("Error update id = {} status = {} detail {}".format(id, status,exec))


def pushMessageTorabbit(message):
    try :
        credentials = pika.PlainCredentials(config.rabbitUser,config.rabbitPass)

        parameters = pika.ConnectionParameters(config.rabbitHost,
                                                        5672,
                                                        '/',
                                                        credentials)

        connection = pika.BlockingConnection(parameters)

        channel =  connection.channel()

        # push message
        channel.basic_publish(exchange='' , routing_key=config.rabbitQueue , body=message)

        logging.info("send message to rabiit success !!!!")
    except Exception as exc:
        logging.error("Error push message to rabbit !!! {}".format(exc))


def sendfileToFpt(fileName):
    try :

        logging.info("start upload file {}".format(fileName))

        ftp = ftplib.FTP(config.fptHost,config.fptUser , config.fptPass)

        ftp.encoding = "utf-8"

        with open(fileName , "rb") as file:
            ftp.storbinary(f"STOR {fileName}" , file)

        ftp.quit()

        logging.info("upload file {} success".format(fileName))
    except Exception as exc:
        logging.error("Error send file {} to fpt {}".format(fileName,exec)) 


def creatFolerToday(today):
    try:
        logger.info("start created folder today {}".format(today))

        # creat folder today 
        check = os.path.isdir('videos/{}'.format(today))
        if check == False:
            os.mkdir('videos/{}'.format(today))
    

    except Exception as exc:
        logger.error("Error created folder {}".format(exc))  


def downloadvideo():
    try:
        logger.info("start download data trend titok !!!!")
        trending = api.by_trending(count = results , custom_verifyFp="")

        logger.info("start process concurent download video tiktok !!!!!")

        # creat folder today 
        today = datetime.now().strftime('%Y%m%d')
        check = os.path.isdir('videos/{}'.format(today))
        if check == False:
            os.mkdir('videos/{}'.format(today))
        

        # with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        #     feature_result = {executor.submit(downloadVideoByUrl,titok) : titok for titok in trending}

        #     for data in concurrent.futures.as_completed(feature_result) :
        #         try:
        #             kq2 = data.result()
        #             logging.info(kq2)
        #         except Exception as exc :
        #             logger.error("Error download")


        for tiktok in trending :
            downloadVideoByUrl(tiktok)

    except Exception as exc:
        logger.error("Error start download video tiktok {}".format(exc))

# def tetsobject():
#     video = Video(1,2,"link","folder")
#     print(video.link)

#     json1 = json.dumps(video.__dict__)
#     print(json1)

# tetsobject()
# schedule.every(1).minutes.do(tetsobject)
# while True:   
#     schedule.run_pending()
#     time.sleep(1)




if __name__ == "__main__":
    # tetsobject()
    downloadvideo()

