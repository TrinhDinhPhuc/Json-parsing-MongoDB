import tarfile
import time as time_import,datetime
import os,glob,gzip,json
from pymongo import MongoClient
import pprint
class knorex:
    def __init__(self):
        self.client = MongoClient('localhost',27017)
        self.db = self.client['analyticsInterview']
        self.collection = self.db['individialWins']
        self.collection_geo = self.db['geoAggregation']
    def mongodb(self,mylist):
        return self.collection.insert_many(mylist)
    def query(self):
        pipeline = [
            {
                u"$group": {
                    u"_id": {
                        u"campaignId": u"$campaignId",
                        u"creativeId": u"$creativeId",
                        u"adgroupId": u"$adgroupId",
                        u"geo": u"$geo",
                        u"time": u"$time"
                    },
                    u"SUM(price)": {
                        u"$sum": u"$price"
                    },
                    u"MIN(price)": {
                        u"$min": u"$price"
                    },
                    u"MAX(price)": {
                        u"$max": u"$price"
                    }
                }
            },
            {
                u"$project": {
                    u"_id": 0,
                    u"campaignId": u"$_id.campaignId",
                    u"creativeId": u"$_id.creativeId",
                    u"adgroupId": u"$_id.adgroupId",
                    u"geo": u"$_id.geo",
                    u"time": u"$_id.time",
                    u"SUM(price)": u"$SUM(price)",
                    u"MIN(price)": u"$MIN(price)",
                    u"MAX(price)": u"$MAX(price)"
                }
            }
        ]

        cursor = self.collection.aggregate(
            pipeline,
            allowDiskUse=True
        )
        try:
            for doc in cursor:
                self.db['geoAggregation'].insert(doc)
        finally:
            self.client.close()
            print("Done!")
    def extract_tar(self,path):
        for file in glob.glob(path):
            if (tarfile.is_tarfile(file) == True):
                t = tarfile.open(file, 'r')
                t.extractall()
                t.close()
    def file_content(self,file):
        rst = []
        data = gzip.open(file, 'rt', encoding='utf8').readlines()
        for ele in data:
            rst.append(json.loads(ele))
        for i in range(0, len(rst), 1):
            filtered_data = json.dumps(rst[i], sort_keys=True, ensure_ascii=False, indent=4)
            datastore = json.loads(filtered_data)
            try:
                auctionId = datastore['auctionId']
            except KeyError:
                auctionId = "NA"
            try:
                campaignId = datastore['biddingMainAccount']
            except KeyError:
                campaignId = "NA"
            try:
                creativeId = datastore['bidResponseCreativeName']
            except KeyError:
                creativeId = "NA"
            try:
                adgroupId = datastore['biddingSubAccount']
            except KeyError:
                adgroupId = "NA"
            try:
                userAgent = json.loads(datastore['bidRequestString'])['userAgent']
            except KeyError:
                userAgent = "Others"
            try:
                site = json.loads(datastore['bidRequestString'])['url']
            except KeyError:
                site = "Others"
            try:
                geo = json.loads(datastore['bidRequestString'])['device']['geo']['country']
            except KeyError:
                geo = "Others"
            try:
                exchange = json.loads(datastore['bidRequestString'])['exchange']
            except KeyError:
                exchange = "Others"
            try:
                price = datastore['winPrice']
            except KeyError:
                price = 0
            try:
                year = int(str(json.loads(datastore['bidRequestString'])['timestamp']).split('-')[0])
                month = int(str(json.loads(datastore['bidRequestString'])['timestamp']).split('-')[1])
                day = int(
                    (str(json.loads(datastore['bidRequestString'])['timestamp']).split('-')[2]).split(':')[0].split(
                        'T')[0])
                hour = int(
                    (str(json.loads(datastore['bidRequestString'])['timestamp']).split('-')[2]).split(':')[0].split(
                        'T')[1])
                time = time_import.mktime(datetime.datetime(year, month, day, hour).timetuple())
            except KeyError:
                time = 0
            finally:
                print("Done!")
            insert_Many = []
            insert_Many.append({"auctionId":auctionId,"campaignId": campaignId,"creativeId":creativeId, "adgroupId":adgroupId,"userAgent": userAgent,"site":site, "geo":geo, "exchange":exchange, "price":price,"time": time})
            return self.mongodb(insert_Many)
    def get_file(self):
        for fol in glob.glob("C:\\Users\\PhucCoi\\Documents\\knorex\\ty-master\\raw-bid-win\\2017\\01\\11\\00\\*"):
            for file in glob.glob(os.path.join(fol+"\\", "*.gz")):
                self.file_content(file)
    def dump_out_json(self):
        cursor = self.collection_geo.find()
        for _document in cursor:
            open("out.json","w+").write(str(_document))
        return "Wrote!"
if __name__ == '__main__':
    knorex = knorex()
    #EXTRACT .tar FILE
    # path = 'C:\\Users\\PhucCoi\\Documents\\knorex\\ty-master\\*'
    # knorex.extract_tar(path)
    #GET CONTENT FILE
    #knorex.get_file()
    #INIT CONNECTION
    #knorex.mongodb()
    #knorex.query()
    knorex.dump_out_json()