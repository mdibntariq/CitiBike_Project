
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(appName="CitiBike_download")

print("Intilizaing ---------------------------------------------------------------------")
# In[2]:


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[3]:


import urllib
import json
import requests
fileNameList = ['station_information','station_status','system_alerts','system_regions']  
cleanedData = []
strData = ''


# function to clean the Json
def jSonCleaner(strData):
    strData = r.text[r.text.find("["):-2]
    return strData

# print(r.text)
for nfile in fileNameList:
    #removing unwanted string from the starting of the json file and from starting    
    #reading the json file using requests
    r = requests.get("https://gbfs.citibikenyc.com/gbfs/en/" +nfile + ".json")
    # correct json after cleaning
    cleanedData.append(jSonCleaner(r.text)) 

#print (requests.get(cleanedData[1]).text)
#stn_info = sqlContext.read.text(cleanedData[0])


# ##  Getting JSON  List from formatted string

# In[4]:

station_information = json.loads(cleanedData[0])
station_status = json.loads(cleanedData[1])
system_alerts = json.loads(cleanedData[2])
system_regions = json.loads(cleanedData[3])


# In[5]:

station_information[0].keys()


# In[6]:


station_status[0].keys()


# In[7]:


system_alerts[0].keys()


# In[8]:


system_regions[0].keys()


# ### Dictonary to Pandas Dataframe

# In[9]:


import pandas as pd

pdf_station_info= pd.DataFrame(station_information)
pdf_station_status= pd.DataFrame(station_status)
pdf_system_alerts= pd.DataFrame(system_alerts)
pdf_system_regions= pd.DataFrame(system_regions)

# In[10]:

sqldf_station_info=sqlContext.createDataFrame(pdf_station_info)   
sqldf_station_status=sqlContext.createDataFrame(pdf_station_status)
sqldf_system_alerts=sqlContext.createDataFrame(pdf_system_alerts)   
sqldf_system_regions=sqlContext.createDataFrame(pdf_system_regions)


# In[16]:


from time import localtime, strftime

joined_info_status = sqldf_station_info.join(sqldf_station_status,
                    ['station_id'],'outer')
joined_info_status.createOrReplaceTempView('aggregation')
result = sqlContext.sql('select * from aggregation')


pd_df=joined_info_status.toPandas()
pd_df['Date']=strftime("%Y/%m/%d %H:%M:%S", localtime())

# In[17]:


from time import gmtime, strftime
timestr = strftime("%Y%m%d_%H%M%S_", localtime())
pd_df.to_csv('/citibike/output/'+timestr+'text.csv')

sc.stop()

