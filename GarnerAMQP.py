# Databricks notebook source
FilePath = getArgument('GarnerFilePath')
TriggerInfo = getArgument('TriggerInfo')
KeyVaultName = getArgument('KeyVaultName')
TableName = getArgument('TableName')
QueryString = getArgument('QueryString')
BusinessUnit = getArgument('BusinessUnit')
Environment = getArgument('Environment')
FullLoad = getArgument('FullLoad')
AccessToken = getArgument('AccessToken')
PaginationLimit = int(getArgument('PaginationLimit'))
FilterListLimit = int(getArgument('FilterListLimit'))

# COMMAND ----------

#FilePath = 'Raw/18438_READ/Garner'
#TriggerInfo = 'test'
#KeyVaultName = 'key-ussc-pscm'
#TableName = 'Material'
#QueryString = '{"query":"query MaterialDetails ($pagination: Pagination!,$ids: [UUID!])  {    materials (pagination: $pagination ,ids: $ids)  {  list {    ...materialDetails      ...materialAttachables      __typename  }  }  }  fragment materialDetails on Material {    id    materialId    description    quantity    suppliedBy {      ...partyDetails      __typename    }    managedBy {      ...partyDetails      __typename    }    isAt {      ...facilityDetails      __typename    }    destinedFor {      ...facilityDetails      __typename    }    __typename  }  fragment partyDetails on Party {    id    name    __typename  }  fragment facilityDetails on Facility {    id    name    __typename  }  fragment materialAttachables on Material {    attained {      ...statusDetails      __typename    }    forecasted {      ...statusDetails      __typename    }    referencedAs {      ...referenceDetails      __typename    }    transfers {      ...transferDetails      __typename    }    comments {      ...commentDetails      __typename    }    documents {      ...documentDetails      __typename    }    measuredAs {      ...measureDetails      __typename    }    __typename  }  fragment referenceDetails on Reference {    id    code    value    __typename  }  fragment statusDetails on Status {    id    occursOn    createdOn    code    __typename  }  fragment transferDetails on Transfer {    id    number    from {      ...facilityDetails      __typename    }    to {      ...facilityDetails      __typename    }    carriedBy {      ...partyDetails      __typename    }    attained {      ...statusDetails      __typename    }    forecasted {      ...statusDetails      __typename    }    __typename  }  fragment commentDetails on Comment {    text    commentedOn    createdBy {      name      party {        ...partyDetails        __typename      }      __typename    }    __typename  }  fragment documentDetails on Document {    id    name    description    location    submittedOn    __typename  }  fragment measureDetails on Measure {    id    code    values {      key      value      __typename    }    occursOn    createdOn    __typename  }","variables": {"ids": ###FilterClause###,"pagination":  {    "limit": ###PaginationLimit###,    "offset": ###PaginationOffset###}}}'
#BusinessUnit = 'test'
#FullLoad = 'N'
#AccessToken = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6IlJUUXhPVU5ETjBJMFJVVTRNemd4TWpNM1FUSkNOemxDT0RFeU5USTFRVFpCTURoQk1EWXlPUSJ9.eyJpc3MiOiJodHRwczovL2NoZXZyb24uYXV0aDAuY29tLyIsInN1YiI6InI1azI4dTdwOTBENTlUS3haY0JoS1RJU1RjZktuTDlpQGNsaWVudHMiLCJhdWQiOiJiYWNrZW5kIiwiaWF0IjoxNTY3MDY2NDYzLCJleHAiOjE1NjcxNTI4NjMsImF6cCI6InI1azI4dTdwOTBENTlUS3haY0JoS1RJU1RjZktuTDlpIiwiZ3R5IjoiY2xpZW50LWNyZWRlbnRpYWxzIn0.IsJx6Br5HPSScvH64BMIm6neGbVQsip0J7wO-_u-gVJwtvaM_0t7gWZcWtmez1uDJjfx7-NXu0bwI_HRXznYPd8KBwnf_KIyfGkpi_1ibgIY_et-3JvzYPeo1jVnRdlp5_-lP_2wzhpjzNkLJ-zToOyjyv0UMN8F-MrOBRCJsSrwy2cnmaIJMVdyRmxyx-kjNIh-2WURgLLYPawLI9ZgPG9A5DVunOlKQBk3Rys6hR_fmtXze6fZeZZk-nooKU33TM0Yd0Szaf9DjOr-qxt9mYqdhD_SofBMOJZ086LI0i0c5vFwGXfhaY5d3S4Sl3NISpJ61HeGaBFuj3uS8wAm9A'
#PaginationLimit = 1000


# COMMAND ----------

#FilterClause = '["00000000-0000-0000-0000-000000000000"]' #replaced 20200406
from pyspark.sql.types import *

FilterClauseList = ['["00000000-0000-0000-0000-000000000000"]']
MountDir = '/mnt/Raw/Garner'

# COMMAND ----------

# MAGIC %run /Common/DBlogging

# COMMAND ----------

#######################        GET IDs FROM QUEUE FOR INCREMENTAL LOAD              #############################
Step = 'cmd5 RabbitMQ queue to get changed ids'
try:
  StartTime = datetime.datetime.now()
  
  import json

  #If Incremental mode then use RabbitMQ queue to get changed ids
  if FullLoad == 'N':
    from kombu import Connection, Exchange, Queue, Consumer
    from kombu.mixins import ConsumerMixin
    from pyspark.sql.functions import *

    QueueIdsList = []

    #Getting data from Queue and loading it into list
    class AmqpConsumer(ConsumerMixin):
      ListLen = -1;

      def __init__(self, connection, queues):
          self.connection = connection
          self.queues = queues


      def get_consumers(self, Consumer, channel):
          return [
              Consumer(queues, callbacks=[self.on_message], accept=['json']),
          ]

      def on_message(self, body, message):
          QueueIdsList.append(body)
          print('RECEIVED MESSAGE: {0!r}'.format(body))
          #self.should_stop=True
          #message.ack()

      def on_iteration(self):
        if self.ListLen == len(QueueIdsList):
          print('Messages received: {}'.format(str(self.ListLen)))
          self.should_stop=True

        self.ListLen = len(QueueIdsList)

    ConString = dbutils.secrets.get(scope = KeyVaultName, key = "ADF-Garner-Queue-CS")        
    queue = dbutils.secrets.get(scope = KeyVaultName, key = "ADF-Garner-Queue-Name").format(Environment)

    queues = Queue(name=queue, no_declare=True)

    conn = Connection(ConString)

    AmqpConsumer(conn, queues).run()

    if len(QueueIdsList) > 0:
      QueueIdsList = [json.loads(x) for x in QueueIdsList]
      #Creating dataframe from ids and loading it to datalake
      df = spark.createDataFrame(QueueIdsList)
      df.write.mode("overwrite").format("orc").save('dbfs:{}/Queue/{}/{}/RawMessages/Archive/{}'.format(MountDir, TableName, BusinessUnit, TriggerInfo))

      ExtractedDf = spark.read.format('orc').options(header='true', inferSchema='true').load('dbfs:{}/Queue/{}/{}/RawMessages/Archive/{}'.format(MountDir, TableName, BusinessUnit, TriggerInfo))
      IdsList = ExtractedDf.collect()

      IdsListTransformed = [{x['entity'] : x['id']} for x in IdsList]

      #Removing ids from queue

      class AmqpRemove(ConsumerMixin):
        ListLen = 0;
        ListLenPrev = -1;

        def __init__(self, connection, queues):
            self.connection = connection
            self.queues = queues

        def get_consumers(self, Consumer, channel):
            return [
                Consumer(queues, callbacks=[self.on_message], accept=['json']),
            ]

        def on_message(self, body, message):
          #print("The incoming message is {}".format(body))
          CurrentMessage = json.loads(body)
          self.ListLen += 1
          if CurrentMessage['entity'] == TableName and {CurrentMessage['entity']:CurrentMessage['id']} in IdsListTransformed:
            print(CurrentMessage)
            message.ack()
            #self.should_stop=True

        def on_iteration(self):

          if self.ListLenPrev == self.ListLen:
             self.should_stop=True
          else:
            self.ListLenPrev = self.ListLen


      print('Removing from Queue')

      #conn = Connection(ConString)

      AmqpRemove(conn, queues).run()


      #Transforming ids into convenient form
      ExtractedDf.createOrReplaceTempView("Garner")

      JsonString = spark.sql("""WITH CTE AS (
                                SELECT DISTINCT 
                                  entity, 
                                  CONCAT('"', id, '"') AS id
                                FROM Garner
                                WHERE entity = '"""+ TableName +"""'),
                                CTE2 AS (
                                SELECT 
                                  CONCAT('"', entity, '"', ' : ', '[', array_join(collect_list(id), ','),']' ) AS JsonLine
                                FROM CTE 
                                GROUP BY entity)
                                SELECT
                                  CONCAT('{', array_join(collect_list(JsonLine), ','), '}') AS JsonObject
                                FROm CTE2""").first()["JsonObject"]

      CurrentFileExists = False

      #Check if file with ids exists in datalake
      #If exists then combine it with current ids
      try:
        for Item in dbutils.fs.ls('dbfs:{}/Queue/{}/{}/ProcessedMessages/Current/'.format(MountDir, TableName, BusinessUnit)):
          if Item.name == 'IncMessages.json':
            CurrentFileExists = True
            break
      except:
        print('Hello Exception!')


      if CurrentFileExists:
          LastMessage = spark.read.format('json').options(header='true', inferSchema='true').load('dbfs:{}/Queue/{}/{}/ProcessedMessages/Current/IncMessages.json'.format(MountDir, TableName, BusinessUnit))
          JsonObg = json.loads(JsonString)

          if (TableName in LastMessage.columns) and (TableName in JsonObg):

            LastMessage = LastMessage.select(concat(concat_ws(',',col(TableName)), lit(','), lit(','.join(list(JsonObg[TableName])))).alias(TableName))
            LastMessage.createOrReplaceTempView("GarnerCombined")      
            JsonString = spark.sql("""WITH UnitedData AS (
                                      SELECT '"""+ TableName +"""' AS entity, explode(split("""+ TableName +""", ',')) AS Id FROM GarnerCombined),
                                      CTE AS (
                                      SELECT DISTINCT 
                                        entity, 
                                        CONCAT('"', id, '"') AS id
                                      FROM UnitedData),
                                      CTE2 AS (
                                      SELECT 
                                        CONCAT('"', entity, '"', ' : ', '[', array_join(collect_list(id), ','),']' ) AS JsonLine
                                      FROM CTE 
                                      GROUP BY entity)
                                      SELECT
                                        CONCAT('{', array_join(collect_list(JsonLine), ','), '}') AS JsonObject
                                      FROm CTE2""").first()["JsonObject"]

      if (TableName in json.loads(JsonString)):  
        dbutils.fs.put('dbfs:{}/Queue/{}/{}/ProcessedMessages/Archive/{}/IncMessages.json'.format(MountDir, TableName, BusinessUnit, TriggerInfo), JsonString, True)

        dbutils.fs.put('dbfs:{}/Queue/{}/{}/ProcessedMessages/Current/IncMessages.json'.format(MountDir, TableName, BusinessUnit), JsonString, True)

      try: 
        LastMessage = spark.read.format('json').options(header='true', inferSchema='true').load('dbfs:{}/Queue/{}/{}/ProcessedMessages/Current/IncMessages.json'.format(MountDir, TableName, BusinessUnit))
        # FilterClause = str(LastMessage.first()[TableName]).replace("'", '"') -replaced 20200406
        FilterList = LastMessage.first()[TableName]
        FilterClauseList = [str(FilterList[i:i + FilterListLimit]).replace("'", '"') for i in range(0, len(FilterList), FilterListLimit)]
      except:
        # FilterClause = '["00000000-0000-0000-0000-000000000000"]'  -replaced 20200406
        FilterClauseList = ['["00000000-0000-0000-0000-000000000000"]']
        
  EndTime = datetime.datetime.now()
  DBlog(DatabricksName,'Success',Step,StartTime,EndTime,'Success')
except Exception as Description:
  Description = str(Description)
  EndTime = datetime.datetime.now()
  DBlog(DatabricksName,Description,Step,StartTime,EndTime,'Failed')
  raise      

# COMMAND ----------

print(FilterClauseList)

# COMMAND ----------

#######################        Executing GraphQL queries              #############################
Step = 'cmd7 Executing GraphQL queries'
try:
  StartTime = datetime.datetime.now()

  import requests

  HostName = dbutils.secrets.get(scope = KeyVaultName, key = 'ADF-Garner-HostName').format(Environment)
  #HostName = 'https://api.{}.chevron.garnercorp.com/api/graph-ql'.format(BusinessUnit)
  FilterListOffset = 0

  for i, FilterClause in enumerate(FilterClauseList):
    PaginationOffset = 0
    RowCount = 1
    while RowCount > 0:
      response = requests.post(
        HostName,
        headers={'Content-Type':'application/json',
               'Authorization': "Bearer {} ".format(AccessToken)},
        data=QueryString.replace('###FilterClause###', FilterClause).replace('###PaginationLimit###', str(PaginationLimit)).replace('###PaginationOffset###', str(PaginationOffset))
      )
      if response.status_code == 200:
        JsonObj = json.loads(response.text)
        RowCount = len(JsonObj['data'][TableName.lower() +'s']['list'])
        if RowCount > 0:
          dbutils.fs.put('dbfs:{}/{}/{}/Archive/{}/Json/{}.json'.format(MountDir, TableName, BusinessUnit, TriggerInfo, TableName + '_' + str(FilterListOffset)+ '_' + str(PaginationOffset) ), response.text, True)
          PaginationOffset += PaginationLimit
      else:
        raise Exception('Exception has been raised. Status Code: {}. Error: {}'.format(response.status_code,response.text.encode('utf8')))
    FilterListOffset += FilterListLimit
    
  EndTime = datetime.datetime.now()
  DBlog(DatabricksName,'Success',Step,StartTime,EndTime,'Success')
except Exception as Description:
  Description = str(Description)
  EndTime = datetime.datetime.now()
  DBlog(DatabricksName,Description,Step,StartTime,EndTime,'Failed')
  raise          

# COMMAND ----------

#import http.client
#import requests

#ClientID = dbutils.secrets.get(scope = KeyVaultName, key = 'ADF-Garner-Client-ID')
#ClientSecret = dbutils.secrets.get(scope = KeyVaultName, key = 'ADF-Garner-Client')
#HostName = dbutils.secrets.get(scope = KeyVaultName, key = 'ADF-Garner-HostName')

#conn = http.client.HTTPSConnection("chevron.auth0.com")

#payload = '{"client_id":"'+ClientID+'","client_secret":"'+ClientSecret+'","audience":"backend","grant_type":"client_credentials"}'

#headers = { 'content-type': "application/json" }
 
#conn.request("POST", "/oauth/token", payload, headers)
 
#res = conn.getresponse()
#data = res.read()


#access_token = json.loads(data.decode("utf-8"))['access_token']

