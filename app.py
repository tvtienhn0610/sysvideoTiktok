from TikTokApi import TikTokApi
from pika import connection
api = TikTokApi.get_instance()
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
import schedules

logger = logging.getLogger('my tool')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler('time_app.log' , when='M' , interval=1 , backupCount=2)
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)

errorlogHandler = handlers.RotatingFileHandler('error.log' , maxBytes=5000 , backupCount=0)
errorlogHandler.setLevel(logging.ERROR)
errorlogHandler.setFormatter(formatter)

logger.addHandler(logHandler)
logger.addHandler(errorlogHandler)

results = 5

def downloadVideoByUrl(tiktok) :
    try :
        # print(tiktok)
        video_url = 'https://www.tiktok.com/@%s/video/%s' % (tiktok['author']['uniqueId'], tiktok['id'])
        logger.info("start download video url = {}".format(video_url))
        data = api.get_video_by_url(video_url, return_bytes = 1 , language='en' , proxy = None ,  custom_verifyFp="")


        with open('videos/%s.mp4' % tiktok['id'] , 'wb') as output:
            output.write(data)
            logger.info("sucess save file "+tiktok['id'])

        return True
    
    except :
        logger.error("Error download video tikton url =")
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

        sql = "SELECT * FROM custorms WHERE id = {} LIMIT = 10".format(5)
        mycusor.execute(sql)

        myresult = mycusor.fetchall()

        for x in myresult:
            print(x)
        
        return myresult
    except :
        logger.error("Error get data from mysql")


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
    except :
        logger.error("Error update id = {} status = {}".format(id, status))


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
    except :
        logging.error("Error push message to rabbit !!!")


def sendfileToFpt(fileName):
    try :

        logging.info("start upload file {}".format(fileName))

        ftp = ftplib.FTP(config.fptHost,config.fptUser , config.fptPass)

        ftp.encoding = "utf-8"

        with open(fileName , "rb") as file:
            ftp.storbinary(f"STOR {fileName}" , file)

        ftp.quit()

        logging.info("upload file {} success".format(fileName))
    except :
        logging.error("Error send file {} to fpt".format(fileName)) 
        

def downloadvideo():
    try:
        trending = api.by_trending(count = results , custom_verifyFp="")

        logger.info("start down load !!!!!")

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            feature_result = {executor.submit(downloadVideoByUrl,titok) : titok for titok in trending}

            for data in concurrent.futures.as_completed(feature_result) :
                kq = feature_result[data]
                try:
                    kq2 = data.result()
                    logging.info(kq2)
                except Exception as exc :
                    logger.error("Error download")
    except  Exception as exc:
        logger.error("Error start download video tiktok !!!!!")


if __name__ == "__main__":

    downloadvideo()

