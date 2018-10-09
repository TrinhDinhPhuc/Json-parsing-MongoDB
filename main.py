import tarfile
import time as time_import,datetime
import os,glob,gzip,json
class knorex:
    def extract_tar(self,path):
        for file in glob.glob(path):
            if (tarfile.is_tarfile(file) == True):
                t = tarfile.open(file, 'r')
                t.extractall()
                t.close()
    def file_content(self,file):
        data = gzip.open(file, 'rt', encoding='utf8').readlines()
        rst = []
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
            print(auctionId, campaignId, creativeId, adgroupId, userAgent, site, geo, exchange, price, time)
    def get_file(self):
        for fol in glob.glob("D:\\knorex\\raw-bid-win\\2017\\01\\11\\00\\*"):
            for file in glob.glob(os.path.join(fol+"\\", "*.gz")):
                self.file_content(file)
if __name__ == '__main__':
    path = 'D:\\knorex\\*'
    #extract_tar(path)
    knorex = knorex()
    knorex.get_file()