import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import pymongo
from pymongo import MongoClient
from googleapiclient.discovery import build
import pymysql
import numpy as np
from datetime import datetime
import pandas as pd

def remove_t_and_z(datetime_str):
    # Replace 'T' and 'Z' with a space
    cleaned_str = datetime_str.replace('T', ' ').replace('Z', '')
    return cleaned_str

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb+srv://saranyasaranya2014:8220163867@cluster0.pmvx63b.mongodb.net/")
db = client.youtube1
col1 = db.ytube


# CONNECTING WITH MYSQL DATABASE
mydb = pymysql.connect(
     host="localhost",
     user="root",
     password="12345",
     autocommit=True
     )
print(mydb)
cursor=mydb.cursor()

# BUILDING CONNECTION WITH YOUTUBE API
api_key = 'AIzaSyC_pEsC9E7Q9o0G_ESpw40V8qoAZeevBXU'
youtube = build("youtube","v3",developerKey=api_key)
# FUNCTION TO GET CHANNEL DETAILS
def channel_details(channel_id):
    channel_details=[]

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for i in range(len(response['items'])):
            data = dict(
                    channel_id=response['items'][i]['id'],
                    channel_name=response['items'][i]['snippet']['title'],
                    subscriber_count=int(response['items'][i]['statistics']['subscriberCount']),
                    videoCount=int(response['items'][i]['statistics']['videoCount']),
                    viewsCount=int(response['items'][i]['statistics']['viewCount']),
                    playlistId=response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
                    )
            channel_details.append(data) 

    return channel_details

#FUNCTION TO DET PLAYLIST DETAILS
def get_playlist_details(channel_id):
        a=[]
        next_page = None
        while True:

            request = youtube.playlists().list(
               part="snippet,contentDetails",
               channelId = channel_id,
               maxResults=50,
               pageToken=next_page

            )
            response = request.execute()
            for i in response['items']:
                dic={'paylist_id':i['id'],
                     'channel_id':i['snippet']['channelId'],
                     'playlist_name':i['snippet']['title'],
                     'publishedAt':(i['snippet']['publishedAt'].replace('z','')),
                     'playlist_video_count':int(i['contentDetails']['itemCount'])}
                a.append(dic)
            next_page=response.get('nextpageToken')
            if next_page is None:
                break
        return a

 # FUNCTION TO GET VIDEO DETAILS
def get_video_ids(video_id):
  b=[]
  response = youtube.channels().list(id=channel_id,part='contentDetails').execute()
  playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  token = None
  while True:
    request = youtube.playlistItems().list(
       part="snippet,contentDetails",
       playlistId = playlist_id ,
       maxResults=100,
       pageToken=token
    )

    response = request.execute()
    for item in response['items']:
      b.append(item['contentDetails']['videoId'])
    token=response.get("nextpageToken")
    if token is None:
      break
  return b

def get_video_details(youtube,video_ids):
  all_video=[]
  for i in range(0,len(video_ids),50):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_ids[i:i+50]
    )
    response = request.execute()
    for video in response['items']:
        video = {'video_id':video['id'],
                 'channel_name':video['snippet']['channelTitle'],
                 'Title':video['snippet']['title'],
                  'like_Count':video['statistics'].get('videoCount',0),
                  'views_Count':video['statistics'].get('viewCount',0),
                  'comment_count':video['statistics'].get('commentCount',0),
                  'published_date':video['snippet']['publishedAt'],
                  'duration':video['contentDetails']['duration']
                }
  all_video.append(video)
  return (all_video)

 # FUNCTION TO GET COMMEND DETAILS
def get_comments_details(video_ids):
  all_comment = []
  for i in video_ids:
    try:
      request = youtube.commentThreads().list(
          part="snippet",
          videoId=i,
          maxResults=50
        )
      response = request.execute()
      for item in response['items']:
        data = {'comment_id':item['id'],
                  'published_date':item['snippet']['topLevelComment']['snippet']['publishedAt'],
                  'videos_id':item['snippet']['videoId'],
                  'comment_text':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                  'comment_author':item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                  }
        all_comment.append(data)
    except:
      pass
  return all_comment
#main function details
def main(channel_id):
    c = channel_details(channel_id)
    a = get_playlist_details(channel_id)
    video_ids = get_video_ids(channel_id)
    v = get_video_details(youtube,video_ids)
    cm = get_comments_details(video_ids)
    data = {'channel_details':c,
            'playlist_id':a,
            'video_id':v,
            'comment_id':cm
            }
    return data
# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_name():   
    ch_name = []
    for i in db.ytube.find():
     ch_name.append(i['channel_details'][0]['channel_name'])
    return ch_name
#create a new database and use 
cursor.execute("create database if not exists youtube_db1")
cursor.execute("use youtube_db1")
#create a new table
cursor.execute("create table if not exists channel_details(channel_id VARCHAR(50) ,channel_name VARCHAR(100),subscriber_count int,videoCount int,viewsCount int,playlist_id VARCHAR(250));")
cursor.execute("create table if not exists playlist(playlist_id VARCHAR(250),channel_id VARCHAR(50),playlist_name VARCHAR(100),publishedAt VARCHAR(100),playlist_video_count int);")
cursor.execute("create table if not exists video(video_id VARCHAR(250),channel_name VARCHAR(150),Title VARCHAR(200),like_Count int, views_Count int,comment_count int,published_date VARCHAR(250),duration VARCHAR(250));")
cursor.execute("create table if not exists comment(comment_id VARCHAR(250),published_date VARCHAR(200),video_id VARCHAR(250),comment_text TEXT,comment_author VARCHAR(250));")

#function insert values to sql
def print_data_tuples(details):
    sql1="""INSERT INTO channel_details(channel_id,channel_name,subscriber_count,videoCount,viewsCount,playlist_id) values(%s,%s,%s,%s,%s,%s);"""
    val1= tuple (details['channel_details'][0].values())
    cursor.execute(sql1, val1)
    
    sql2="""INSERT INTO playlist(playlist_id,channel_id,playlist_name,publishedAt,playlist_video_count)values(%s,%s,%s,%s,%s);"""
    for i in details['playlist_id']:
        val=(tuple(i.values()))
        cursor.execute(sql2,val)
    
    sql3="""INSERT INTO video(video_id,channel_name,Title,like_Count,views_Count,comment_count,published_date,duration)values(%s,%s,%s,%s,%s,%s,%s,%s);"""
    for j in details['video_id']:
        val3= tuple (j.values())
        cursor.execute(sql3, val3)
    
    sql4="""INSERT INTO comment(comment_id ,published_date ,video_id,comment_text ,comment_author )values(%s,%s,%s,%s,%s);"""
    for k in details['comment_id']:
        val4= tuple (k.values())
        cursor.execute(sql4,val4)
        
mydb.commit()
#creating option menu
with st.sidebar:
    selected = option_menu(None, ["DATA COLLECTION","SELECT AND STORE","DATA ANALYSIS"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical")
st.title(':violet[Youtube Data Harvesting]') 
#VIEW DATA COLLECTION PAGE
if selected == "DATA COLLECTION":
    def collection_data():
        return pd.DataFrame({"Channel Name": ['Namma Paiyan','LEARN WITH DR.AARTHI RAVI',
                                'Feel good & E Smart by Rekkha Ravikumar',
                                'Jaivins Academy (Official)',
                                'Rowdy Baby','Best Prime Stories Tamil','Arunkumar Bairavan', 
                                'Market Tamizha','Deep Matrix','Error Makes Clever Academy'],

                            "Channel Id": ['UCMahdXKrfxOjEfh7t9iaNcQ',
                            'UCKJ38e2QJX4z4AA4TWQoOKg',
                            'UC77fzn4tscE_NwrKJX3vZKg',
                            'UCONuGIXAY4jf1L-pm68pEpA',
                            'UCz7vNR5jqM_fbIy6ZP9bxzQ','UCWvrhwQpW4MNCWKzTz_Qh3g','UCXdZNHV74WIzwR07T3fy8qg',
                            'UCOe2svFdokRH2l0lvQURbqg','UCnVpEcfut-Bu1IFmQr7vRuw','UCwr-evhuzGZgDFrq_1pLt_A']
            }
        )
    df = collection_data()       
    st.dataframe(df) 
 # EXTRACT and TRANSFORM PAGE
elif selected == "SELECT AND STORE":
    tab1,tab2 = st.tabs(["huge EXTRACT ", "huge TRANSFORM "])
      # EXTRACT TAB
    with tab1:
        channel_id = st.text_input("**Enter channel_id**")
        if channel_id and st.button("Extract Data"):
            ch = channel_details(channel_id)
            st.write(ch)
            data = main(channel_id)
            col1.insert_one(data)
            st.success("upload to mongodb successful!!" )

# TRANSFORM TAB
    with tab2: 
        st.header(":green[Migration of Data]")    
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        ch_name = channel_name()
        user_input = st.selectbox("Select channel",options= ch_name)

        if st.button("Submit"):
            data = col1.find_one({'channel_details.channel_name':user_input})
            print_data_tuples(data) 
            st.success("Transformation to MySQL Successful!!!")
    
# DATA ANALYSIS PAGE
if selected == "DATA ANALYSIS":
    cursor.execute("USE youtube_db1")
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT Title,channel_name FROM video channel_name""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT channel_name, videoCount FROM channel_details
                            ORDER BY videoCount DESC""")
        df=pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT channel_name, Title, views_Count FROM video ORDER BY views_Count DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT a.video_id, a.Title, b.comment_count
                            FROM video AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS comment_count
                            FROM comment GROUP BY video_id) AS b
                            ON a.video_id = b.video_id ORDER BY b.comment_count DESC""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
                    
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name, Title, like_count FROM video
                            ORDER BY like_Count DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT Title,like_count FROM video ORDER BY like_count DESC""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name, viewsCount FROM channel_details ORDER BY viewsCount DESC""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT channel_name,published_date FROM video""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name,AVG(duration)FROM video GROUP BY 1""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name,video_id comment_count FROM video ORDER BY comment_count DESC LIMIT 10;""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
      

       


   



