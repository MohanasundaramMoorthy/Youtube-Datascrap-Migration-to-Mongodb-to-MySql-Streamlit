import googleapiclient.discovery
from pprint import pprint
import streamlit as st
import pymongo
# from airflow import DAG
# from airflow.decorators import task
# from airflow.providers.mongo.hooks.mongo import MongoHook
# from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
from pymongo import MongoClient
import psycopg2
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import mysql.connector
import pymysql
from sqlalchemy import text
import sqlalchemy as sa


api_key = "AIzaSyCBioWRinqfMv4Go0A3EUOAnUafwfb_p1o"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["demo"]
mycol = mydb["names"]

cnx = create_engine('mysql+pymysql://root:!ONEone1@localhost/guviproject_1',future=True)    
conn = mysql.connector.connect(host="localhost",port="3306",user="root",password="!ONEone1",database="guviproject_1")

# connection_url = sa.engine.URL.create('mysql+pyodbc://root:!ONEone1@localhost/guviproject_1',future=True)
# engine = sa.create_engine(connection_url)
# table_abc = sa.Table("Channel", sa.MetaData(), autoload_with=engine)

# engine = create_engine("guvi_mysql_project1://root@localhost:3306") 
# conn = engine.connect(host="localhost",
# database="youtube_data_harvesting_postgre",
# user="root",
# password="!ONEone1")

cur = conn.cursor()
                      

# # Connect to PostgreSQL
# conn = psycopg2.connect(
# host="localhost",
# database="youtube_data_harvesting_postgre",
# user="postgres",
# password="!ONEone1"
# )

# cur = conn.cursor()



def channel_details(channel_ids):
    global video_id = []
    global video_url_=[]
    video_title_=[], video_description_=[]
        
        
        
        
    # fetching the details of  channel description, channel_published date, channel_thumbnail, channel_Subscriberscount, channel_videocount, channel_viewcount using channel id
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_ids
    )
    response = request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    videos = []
    next_page_token = None
    while True:
        playlist_items_response=youtube.playlistItems().list(
                #part='contentDetails',
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
    ).execute()
        if not next_page_token: break

        videos += playlist_items_response['items']

        next_page_token = playlist_items_response.get('nextPageToken')

        
        for video in videos:
    #video_id = video['contentDetails']['videoId']
            video_id = video['snippet']['resourceId']['videoId']
            video_id_.append(video_id)
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            video_url_.append(video_url)
            video_title=video['snippet']['title']
            video_description = video['snippet']['description']
            video_description_.append(video_description)
            video_title_.append(video_title)
            video_url_.append(video_url)
    
    channel_details_var = dict(title=response['items'][0]['snippet']['title'],
                               channelid=response['items'][0]['id'],
                               channel_description=response['items'][0]['snippet']['description'],
                               channel_published=response['items'][0]['snippet']['publishedAt'],
                               channsel_thumbnail=response['items'][0]['snippet']['thumbnails']['medium'],
                               channel_subscribercount=response['items'][0]['statistics']['subscriberCount'],
                               channel_videocount=response['items'][0]['statistics']['videoCount'],
                               channel_viewcount=response['items'][0]['statistics']['viewCount'],
                               channel_video_id = video_id_,
                               channel_video_url= video_url_,
                               channel_video_Description = video_description_,
                               channel_video_title = video_title_,
                               channel_video_URL = video_url_
                            
                               )
    return channel_details_var




channel_ids = st.text_input('Please Enter your channel id here')
if st.button('Scrape') and channel_ids:
    details = channel_details(channel_ids)
    st.write(details)
    x = mycol.insert_many([details])
    # st.write(details)


# fetching data from mongo
messages_to_migrate = list(mycol.find())

        # print(messages_to_migrate, "message to migrate")
option = st.selectbox('which channel you would like to scrap, please select it',
messages_to_migrate,
index=None,
placeholder="Select channel name...",)
print("option",option)
print("this is options type!!!!!!!!!!!!!!!!!!!!!",type(option))


# def migration_mongo_to_mysql():

#     if st.button('migrate') and option:
#         print("yessssssssssss!!!!!!!!!!!!!!!!!!!!!")
#         messages_to_migrate_list = web.get_(mycol.find(option))
#         st.write("THis is Mycol !!!!!!!!!!!!!!",  mycol)
#         st.write(messages_to_migrate_list)
#         messages_to_migrate_list.to_sql('Channel', cnx, if_exists='append', index = False)
#     # insert data
#         cols = "`,`".join([str(i) for i in messages_to_migrate_list.columns.tolist()])
#     # insert dict records .
#         for i,row in messages_to_migrate_list.iterrows():
#             sql = "INSERT INTO `Channel` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
#             cur.execute(sql, tuple(row))
#             conn.commit()
#     messages_to_migrate_sql = list(mycol.find(option))
#     print(messages_to_migrate_sql)
if st.button('migrate') and option:
    messages_to_migrate_list = list(mycol.find(option))
    # print("This is the length!!!!!!!!!!",len(messages_to_migrate_list))
    st.write("THis is Mycol !!!!!!!!!!!!!!",  mycol)
    st.write(messages_to_migrate_list)
    print(type(messages_to_migrate_list))

    d1={}
    for i in messages_to_migrate_list:
        d1.update(i)
    print(d1)
    print("this d1",type(d1))
    print("LEngth od d1", len(d1))

    st.write(d1)
    keys = d1.keys()
    st.write("mohan keys : ", str(keys))
    values = d1.values()
    st.write("mohan values : ", str(values))

    insert_query = "INSERT INTO Channel (_id,title, channelid, channel_description,channel_published,channel_subscribercount,channel_videocount,channel_viewcount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    data_user = (str(d1.get('_id')), d1.get('title'), d1.get('channelid'), d1.get('channel_description'), d1.get('channel_published'),d1.get('channel_subscribercount'),d1.get('channel_videocount'),d1.get('channel_viewcount'))

    cur.execute(insert_query, data_user)

    conn.commit()
   
    # for message in d1:
    #     id = d1.get('_id')
    #     print(id)

    # with engine.begin() as conn:
    #     conn.execute(table_abc.insert(), my_dict)

    # fields = (str(list(d1.keys()))[1:-1])
    # values = (str(list(d1.values()))[1:-1])

    # columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in d1.keys())
    # values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in d1.values())
    # sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('Channel', columns, values)

#     with engine.connect() as con:
#     rs = con.execute(sql)
    
    
        
        # cur.execute(insert_query, ('34','fsfd'))

        # qmarks = ', '.join('?' * len(d1))
        # qry = "Insert Into Channel (channel_id,title) Values (%s)" % (qmarks)
        # cur.execute(qry, list({"channel_id": x, "title": y}))
        # cur.execute(
        # text("INSERT INTO Channel(channel_id,channel_description ) VALUES (x, y)")  )
        # cur.commit()
        # cur.execute(insert_query,message.title)
        # cur.commit()
        
    # d1=dict(enumerate(messages_to_migrate_list))

    # st.write(d1)
    # print(type(d1))
    # print(len(d1))

    # keys = d1.keys()
    # values = d1.values()
    # print("keys : ", str(keys))
    # print("values : ", str(values))
    # next(iter(dict))

    # with cnx.connect() as conn:
    #  for value in d1:
    #     conn.execute(
    #             text("INSERT INTO Channel(title, channel_id,channel_description, channel_published, channsel_thumbnail, channel_subscribercount, channel_videocount, channel_viewcount ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #                 ")
    #  conn.commit()
   
    
    # def convert(messages_to_migrate_list):
    #     res_dict = {}
    #     for i in range(0, len(messages_to_migrate_list), 2):
    #         res_dict[messages_to_migrate_list[i]] = messages_to_migrate_list[i + 1]
    #     return res_dict
 
    
    # messages_to_migrate_Dict = convert(messages_to_migrate_list)
    # print(type(messages_to_migrate_Dict))



# # Insert migrated messages
#     for message in messages_to_migrate_list:
#         st.write("hello")
#         st.write(message["title"])
#         insert_query = "INSERT INTO Channel ( title, channelid, channel_description,channel_published,channsel_thumbnail,channel_subscribercount,channel_videocount,channel_viewcount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
#         # cur.execute(insert_query,list((message['title'], message['channelid'],message["channel_description"],message["channel_published"],message["channsel_thumbnail"],message["channel_subscribercount"],message["channel_videocount"],message["channel_viewcount"]  )))
#         cur.execute(insert_query,message.title)
#         cur.commit()


# 
# @task
# def transfer_data():
#     mongo_conn = MongoHook(conn_id='mongo_default')
#     mysql_conn = MySqlHook(mysql_conn_id='mysql_default')

#     # fetch data from MongoDB
#     collection = mongo_conn.get_collection('names','demo')
#     data = list(collection.find())

#     # insert data into Mysql
#     for document in data:
#         query = "INSERT INTO Channel(channel_id, channel_name, channel_type, channel_views, channel_description, channel_status) VALUES (%s, %s, %s, %s, %s, %s)"
#         mysql_conn.run(query,parameters=(document['channelid'],document['title'],document['channsel_thumbnail'],document['channel_viewcount'],document['channel_description'],document['channel_published']))
    
# with DAG('mongot_to_mysql',
#          start_Date = datetime(2024,3,1),
#          schedule_interval = '@hourly') as dag:
    
#     transfer_Task = transfer_data()
