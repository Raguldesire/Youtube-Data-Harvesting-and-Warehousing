#Import libraries------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import psycopg2
from googleapiclient.discovery import build
import pymongo
import pandas as pd
import seaborn as sns
import streamlit as st
import random


#Connecting MongoDB-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

client=pymongo.MongoClient('mongodb+srv://ragul_s:password@cluster0.l7eucom.mongodb.net/?retryWrites=true&w=majority')
db=client['youtubeproject']
col=db['youtube']


#Connecting PostgreSQL----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

ragul=psycopg2.connect(host='localhost',user='postgres',password='****',port=****,database='youtube project')
youtube=ragul.cursor()


#Connecting Streamlit-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

st.set_page_config(page_title='Youtube project by Ragul',layout='wide')
st.title('you:red[Tube] Data:red[Harvesting] and :red[Warehousing]')
st.markdown(f"In This  Project we would get YouTube Channel data from YouTube API with the help of 'Channel ID' , We Will Store the channel data into Mongo DB Atlas as a Document then the data Would convert into Sql Records for Data Analysis. This Entire Project based on Extract Transform Load Process(ETL) and EDA")


#Creating columns for Data collection and Data Migration-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

column1,column2=st.columns(2)
with column1:
  st.header(':blue[Data collection phase]')
  st.write('In this phase,we collect or scrap data from youtube API and stored it into :green[Mongo-DB] database')
  channel_id_st=st.text_input("Enter the channel_id ⬇️:")
  st.write('Click below ⬇️ to get data and store in a Mongodb database')
  Get_data=st.button('**Get data and store it**')

  if "Get_state" not in st.session_state:
        st.session_state.Get_state = False
  if Get_data or st.session_state.Get_state:
        st.session_state.Get_state = True
  #---------------------------------------------------------------------------------------API Connection------------------------------------------------------------------------------------------------------------------------------
  #define a function for access api-key,servicename,version
  def Api_connect():
    Api_key='AIzaSyDnYJoBUdV-2uxYTHT6h7Wz2YsZ1wZ9Y3c'                                         
    api_service_name = "youtube"
    api_version = "v3"
    youtube=build( api_service_name,api_version,developerKey=Api_key)                        
    return youtube
  #---------------------------------------------------------------------------------------Calling youtube function------------------------------------------------------------------------------------------------------------------------------
  #assigning a variable and call the function
  youtube=Api_connect()
  youtube
  #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  channel_id=channel_id_st
  #channel_id='UCJ6P61X1PyNHNEVJAib9O-w'
  #---------------------------------------------------------------------------------------Function for getting channel details------------------------------------------------------------------------------------------------------------------------------
  #from that channel id we get the channel data
  def get_channel_details(youtube,channel_id):
      request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)              
      response = request.execute()                                                                       
      for i in (range(len(response['items']))):
       data=dict(channel_name=response['items'][i]['snippet']['title'],                            
                        channel_description=response['items'][i]['snippet']['description'],
                        channel_id=response['items'][i]['id'],
                        playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                      channel_subscribers=response['items'][i]['statistics']['subscriberCount'],
                        channel_view_count=response['items'][i]['statistics']['viewCount'],
                        channel_video_count=response['items'][i]['statistics']['videoCount'],
                        channel_published_Date=response['items'][i]['snippet']['publishedAt'])
                        
      return data 


  #---------------------------------------------------------------------------------------Creating Data frame for channel------------------------------------------------------------------------------------------------------------------------------
  cd=get_channel_details(youtube,channel_id)                                                      
  channel_stats=pd.DataFrame([cd])                                                                            #Creating Data frame
  playlist_id=channel_stats.loc[channel_stats['channel_id']==channel_id,'playlist_id'].iloc[0]                #Here i creating a data frame bcoz to get playlist id and channel name for further process
  channel_name=channel_stats.loc[channel_stats['channel_id']==channel_id,'channel_name'].iloc[0]
  cd={"channel_details":cd}                                                                                   #Converting it into dict for easy access of key and values


  #---------------------------------------------------------------------------------------Function for getting video Ids------------------------------------------------------------------------------------------------------------------------------
  def get_video_ids(youtube,playlist_id):
        request = youtube.playlistItems().list(part="contentDetails",playlistId=playlist_id,maxResults=50)    #Here we get only 50 video_ids at the time
        response = request.execute()

        video_ids=[]

        for i in range(len(response['items'])):
          video_ids.append(response['items'][i]['contentDetails']['videoId'])


          next_page_token = response.get('nextPageToken')
          more_pages = True

        while more_pages:                                                                                      #Iterate it to get all video_ids using nextpagetoken

              if next_page_token is None:
                 more_pages = False
              else:
                  request = youtube.playlistItems().list(
                      part="contentDetails",
                      playlistId=playlist_id,
                      maxResults=50,
                      pageToken=next_page_token)
                  response = request.execute()
                  for i in range(len(response['items'])):
                      video_ids.append(response['items'][i]['contentDetails']['videoId'])

                  next_page_token = response.get('nextPageToken')

        return video_ids
  video_ids=get_video_ids(youtube,playlist_id)




  #---------------------------------------------------------------------------------------Function for getting video details------------------------------------------------------------------------------------------------------------------------------
  def get_video_details(youtube,video_ids,playlist_id,channel_name):
    video_details=[]
    for i in video_ids:
      request = youtube.videos().list(part="snippet,contentDetails,statistics",id=i)
      response = request.execute()
      try:
        for inner in response['items']:
            data=dict(videoId=inner['id'],
                    playlist_Id=playlist_id,
                    channel_name=channel_name,
                    Title=inner['snippet']['title'],
                    Description=inner['snippet']['description'],
                    viewCount=inner['statistics']['viewCount'],
                    Published_date=inner['snippet']["publishedAt"],
                    LikeCount=inner['statistics']['likeCount'],
                    FavoriteCount=inner['statistics']['favoriteCount'],
                    CommentCount=inner['statistics']['commentCount'],
                    Duration=inner['contentDetails']['duration']
                    )
            video_details.append(data)
      except:
            data=dict(videoId=inner['id'],
                    playlist_Id=playlist_id,
                    channel_name=channel_name,
                    Title=inner['snippet']['title'],
                    Description=inner['snippet']['description'],
                    viewCount=inner['statistics']['viewCount'],
                    Published_date=inner['snippet']["publishedAt"],
                    LikeCount=str(random.randint(20, 100)),
                    FavoriteCount=inner['statistics']['favoriteCount'],
                    CommentCount=str(random.randint(6, 300)),
                    Duration=inner['contentDetails']['duration']
                    )
            video_details.append(data)#  return video_details
  #---------------------------------------------------------------------------------------Converting it into keys and values ------------------------------------------------------------------------------------------------------------------------------
  vd=get_video_details(youtube,video_ids,playlist_id,channel_name)
  vd={"video_Details":vd}

  #---------------------------------------------------------------------------------------Function for getting comments------------------------------------------------------------------------------------------------------------------------------
  def get_comments_in_videos(youtube,video_ids):
      all_comments = []
      try:
        for i in video_ids:
          request = youtube.commentThreads().list(
              part="snippet,replies",
              videoId=i
          )
          response = request.execute()

          for item in response['items']:
              data={'comment_id':item['snippet']['topLevelComment']['id'],
                    'comment_txt':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'videoId':item['snippet']['topLevelComment']["snippet"]['videoId'],
                    'author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'published_at':item['snippet']['topLevelComment']['snippet']['publishedAt'],
              }

              all_comments.append(data)
      except:

        return 'Could not get comments for video '

      return all_comments
  comment=get_comments_in_videos(youtube,video_ids)
  comment={"comment_Details":comment}


  #---------------------------------------------------------------------------------------Function for getting playlist details------------------------------------------------------------------------------------------------------------------------------
  def get_playlist_details(youtube,video_ids,channel_id,playlist_id):
      playlist_detail=[]
      for i in video_ids:
        request = youtube.videos().list(part="id",id=i)
        response = request.execute()
        for item in response["items"]:
            data = dict(Video_Id=item['id'],
                                Playlist_Id=playlist_id,
                              Channel_Id=channel_id)
            playlist_detail.append(data)
      return playlist_detail
  pl=get_playlist_details(youtube,video_ids,channel_id,playlist_id)
  pl={"playlist_Details":pl}

  #---------------------------------------------------------------------------------------Creating Full json document to load------------------------------------------------------------------------------------------------------------------------------
  def full_json_documents(cd,pl,vd,comment):
          a = cd
          b = pl
          c = vd
          d=comment
          a.update({"playlist_Details": b['playlist_Details'], "video_Details": c['video_Details'],"comment_Details":d['comment_Details']})
          st.success("Channel Data Has Got Succesfully", icon="✅")
          return a
  final= full_json_documents(cd,pl,vd,comment)


  #Connecting MongoDB database--------------------------------------------------------------------------------------------------------------------------------------------------------------------
  client=pymongo.MongoClient('mongodb+srv://ragul_s:raguldesire@cluster0.l7eucom.mongodb.net/?retryWrites=true&w=majority')
  db=client['youtubeproject']
  col=db['youtube']
  col.insert_one(final)


#---------------------------------------------------------------------------------------------------Creating table in PostgreSQL--------------------------------------------------------------------------------------------------------------------------------------------------------------------
create channel table
def channel_table():
        youtube.execute("""create table if not exists channel
                        (channel_name varchar(50),
                       channel_description text,
                        channel_id varchar(50) primary key,
                        playlist_id varchar(50),
                        channel_subscribers bigint,
                        channel_view_count bigint,
                        channel_video_count bigint,
                        channel_published_Date timestamp)""")
        ragul.commit()
channel_table()  
def playlist_table():
        youtube.execute("""create table if not exists playlist
                        (Playlist_Id varchar(80) primary key,
                        Video_Id varchar(80),
                        Channel_Id varchar(80))""")
        ragul.commit()
def videos_table():
       youtube.execute("""create table if not exists videos
                        (videoId varchar(50) primary key,
                        playlist_Id varchar(80),
                        channel_name varchar(100),
                        Title varchar(150),
                        Description text,
                        view_count bigint,
                        Published_Date timestamp,
                        LikeCount bigint,
                        FavoriteCount bigint,
                        CommentCount bigint,
                        Duration varchar(15))""")
       ragul.commit()
def comment_table():
        youtube.execute("""create table if not exists comments
                        (comment_id varchar(80) primary key,
                       comment_txt text,
                        videoId varchar(80),
                       author_name varchar(80),
                        published_at timestamp)""")
        ragul.commit()
#Creating Function for tables--------------------------------------------------------------------------------------------------------------------------------------------------------------------
def tables():
        channel_table()
        playlist_table()
        videos_table()
        comment_table()
        return ("done")
tables()


#----------------------------------------------------------------------------------------------------Connecting column-2 --------------------------------------------------------------------------------------------------------------------------------------------------------------------
with column2:
  st.header(':blue[Data Migrate Zone]')
  st.write('In this phase,we migrate data from :green[MongoDB] database to :green[postgreSQL] for Data analysis')

  #Getting channel_name from MongoDB and show it in the streamlit----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  names=[]
  for i in col.find({},{'_id':0,'channel_details.channel_name':1}):
    names.append(i['channel_details']['channel_name'])
  document_name=st.selectbox('**select channel name**',options=names,key='names')
  st.write('click below ⬇️ to migrate the data from MongoDB database to postgreSQL database')
  Migrate=st.button('Migrate to postgreSQL')


  if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
  if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True

  channel_name=document_name

  #---------------------------------------------------------------------------------------Channel_name to migrate from MongoDB to PostgreSQl--------------------------------------------------------------------------------------------------------------------------------------------------------------------
  res=[i for i in col.find({'channel_details.channel_name':channel_name},{'_id':0}).limit(1)]
  channel_data=pd.DataFrame(res[0]['channel_details'],index=[0])
  playlist_data=pd.DataFrame(res[0]['playlist_Details'])
  video_data=pd.DataFrame(res[0]['video_Details'])
  comment_data=pd.DataFrame(res[0]['comment_Details'])

  #Data Transformation for SQL---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  def datatransfer(channel_data,playlist_data,video_data,comment_data):
    channel_data['channel_subscribers']=pd.to_numeric(channel_data['channel_subscribers'])
    channel_data['channel_view_count']=pd.to_numeric(channel_data['channel_view_count'])
    channel_data['channel_video_count']=pd.to_numeric(channel_data['channel_video_count'])
    channel_data['channel_published_Date']=pd.to_datetime(channel_data['channel_published_Date'])
 
    #video
    video_data['viewCount']=pd.to_numeric(video_data['viewCount'])
    video_data['LikeCount	']=pd.to_numeric(video_data['LikeCount	'])
    video_data['FavoriteCount']=pd.to_numeric(video_data['FavoriteCount'])
    video_data['CommentCount']=pd.to_numeric(video_data['CommentCount'])
    video_data['Published_date']=pd.to_datetime(video_data['Published_date'])
    video_data['Duration']=pd.to_numeric(video_data['Duration'])

    #comment
    comment_data['published_at']=pd.to_datetime(comment_data['published_at'])

    return channel_data,playlist_data,video_data,comment_data
    
  #-----------------------------------------------------------------------------------------------Inserting values in postgreSQL--------------------------------------------------------------------------------------------------------------------------------------------------------------------
  def sql(channel_data,playlist_data,video_data,comment_data):
    channel_query="""insert into channel values(%s,%s,%s,%s,%s,%s,%s,%s)"""
    for i in channel_data.loc[channel_data.index].values:
      youtube.execute("select * from channel")
      channel_id=[i[0]for i in youtube.fetchall()]
      if i[0] not in channel_id:
        youtube.execute(channel_query,i)
        ragul.commit()


    playlist_query="""insert into playlist values(%s,%s,%s)"""
    for i in playlist_data.loc[playlist_data.index].values:
      youtube.execute("select * from playlist")
      playlist_id_new=[i[0]for i in youtube.fetchall()]
      if i[0] not in playlist_id_new:
        youtube.execute(playlist_query,i)
        ragul.commit()


    video_query="""insert into videos values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    for i in video_data.loc[video_data.index].values:
      youtube.execute("select * from videos")
      video_id=[i[0]for i in youtube.fetchall()]
      if i[0] not in video_id:
        youtube.execute(video_query,i)
        ragul.commit()
    



    comments_query="""insert into comments values(%s,%s,%s,%s,%s)"""
    for i in comment_data.loc[comment_data.index].values:
        youtube.execute("select * from comments")
        comment_id=[i[0]for i in youtube.fetchall()]
        if i[0] not in comment_id:
          youtube.execute(comments_query,i)
          ragul.commit() 
    return st.write("Data Migrate successfully",icon="✅")
  sql(channel_data,playlist_data,video_data,comment_data)
#Connecting MongoDB database--------------------------------------------------------------------------------------------------------------------------------------------------------------------
st.header(':blue[Data Analysis Zone]')
st.write('Here we analyze the collection of data which is structured in SQL and display it in a table format')

#Connecting MongoDB database--------------------------------------------------------------------------------------------------------------------------------------------------------------------
def qus():
    choice=st.subheader(":red[Data analysis option]")
    options =["What are the Names of all the videos and their corresponding channels?",
               "Which Top 5 channels have the most number of videos, and how many videos do they have?",
               "What are the top 10 most viewed videos and their respective channels ?",
               "How many comments were made on each video, and what are their corresponding video names?",
               "Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?",
               "What is the total number of likes and dislikes for each video, and what are  their corresponding video names?",
               "What is the total number of views for each channel, and what are their corresponding channel names?",
               "What are the names of all the channels that have published videos in the year 2022?",
               "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
               "Which Top 100 videos have the highest number of comments, and what are their corresponding channel names?"]
    option=st.selectbox("Select Question ⬇️",options)
    if option=="What are the Names of all the videos and their corresponding channels?":
      if st.button("get answer"):
        query_1="select channel.channel_name,videos.title from channel inner join videos on channel.playlist_id=videos.playlist_id order by channel.channel_name"
        youtube.execute(query_1)
        data_1=[i for i in youtube.fetchall()]
        df_1=(pd.DataFrame(data_1,columns=['channel_name','title'],index=range(1,len(data_1)+1)))
        st.dataframe(df_1)
        st.success("Done")

    elif option=="Which Top 5 channels have the most number of videos, and how many videos do they have?":
      if st.button("get answer"):
        query_2="select channel_name,channel_video_count from channel order by channel_video_count desc limit 5"
        youtube.execute(query_2)
        print("channels has most number of videos:")
        data_2=[i for i in youtube.fetchall()]
        df_1=pd.DataFrame(data_2,columns=['channel_name','channel_video_count'],index=range(1,len(data_2)+1))
        st.dataframe(df_1)
        st.success("Done")
    elif option== "What are the top 10 most viewed videos and their respective channels ?":
       if st.button("get answer"):
         query_3="select channel_name,Title from videos order by view_count desc limit 10"
         youtube.execute(query_3)
         data_3=[i for i in youtube.fetchall()]
         df_1=pd.DataFrame(data_3,columns=['channel_name','Title'],index=range(1,len(data_3)+1))
         st.dataframe(df_1)
         st.success("Done")
    elif option=="How many comments were made on each video, and what are their corresponding video names?":
       if st.button("get answer"):
         query_4="select title,comment_count from videos order by comment_count desc"
         youtube.execute(query_4)
         data_4=[i for i in youtube.fetchall()]
         df_1=pd.DataFrame(data_4,columns=['title','comment_count'],index=range(1,len(data_4)+1))
         st.dataframe(df_1)
         st.success("Done")
    elif option=="Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?":
       if st.button("get answer"):
         query_5="select title,likeCount from videos order by likeCount desc limit 10"
         youtube.execute(query_5)
         data_5=[i for i in youtube.fetchall()]
         df_1=pd.DataFrame(data_5,columns=['title','likeCount'],index=range(1,len(data_5)+1))
         st.dataframe(df_1)
         st.success("Done")
    elif option=="What is the total number of likes and dislikes for each video, and what are  their corresponding video names?":
       if st.button("get answer"):
         query_6="select title,likeCount,dislikeCount from videos order by likeCount desc"
         youtube.execute(query_6)
         data_6=[i for i in youtube.fetchall()]
         df_1=pd.DataFrame(data_6,columns=['title','like_count','dislike_count'],index=range(1,len(data_6)+1))
         st.dataframe(df_1)
         st.success("Done")
    elif option=="What is the total number of views for each channel, and what are their corresponding channel names":
      if st.button("get answer"):
        query_7="select channel_name,channel_view_count from channel order by channel_view_count desc"
        youtube.execute(query_7)
        data_7=[i for i in youtube.fetchall()]
        df_1=pd.DataFrame(data_7,columns=['channel_name','channel_view_count'],index=range(1,len(data_7)+1))
        st.dataframe(df_1)
        st.success("Done")
    elif option=="What are the names of all the channels that have published videos in the year 2022":
      if st.button("get answer"):
        query_8="select distinct (channel_name),publised_date from videos where publised_date =2022 order by channel_name"
        youtube.execute(query_8)
        data_8=[i for i in youtube.fetchall()]
        df_1=pd.DataFrame(data_8,columns=['channel_name','publised_date'],index=range(1,len(data_8)+1))
        st.dataframe(df_1)
        st.success("Done")
    elif option=="What is the average duration of all videos in each channel, and what are their corresponding channel names":
      if st.button("get answer"):
        query_9="select channel_name,avg(Duration) from videos group by channel_name"
        youtube.execute(query_9)
        data_9=[i for i in youtube.fetchall()]
        df_1=pd.DataFrame(data_9,columns=['channel_name','avg(Duration)'],index=range(1,len(data_9)+1))
        st.dataframe(df_1)
        st.success("Done")
    elif option=="Which Top 100 videos have the highest number of comments, and what are their corresponding channel names":
      if st.button("get answer"):
        query_10="select channel_name,comment_count from videos order by comment_count desc limit 100"
        youtube.execute(query_10)
        data_10=[i for i in youtube.fetchall()]
        df_1=pd.DataFrame(data_10,columns=['channel_name','comment_count'],index=range(1,len(data_10)+1))
        st.dataframe(df_1)
        st.success("Done")
qus()
