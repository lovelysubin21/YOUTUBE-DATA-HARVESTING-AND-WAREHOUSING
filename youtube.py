from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection

def Api_connect():
    Api_ID="AIzaSyC8wkM3R6m_MUUmUlAdKZObkZQoO9ogtqk"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_ID)

    return youtube

youtube=Api_connect() 

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response["items"]:
        data=dict(channel_Name=i["snippet"]["title"],
                channel_Id=i["id"],
                subscribers=i['statistics']['subscriberCount'],
                views=i["statistics"]["viewCount"],
                total_videos=i["statistics"]["videoCount"],
                channel_description=i["snippet"]["description"],
                playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    respones=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    playlist_Id=respones['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#get video information
def get_videos_info(video_Ids):
    video_data=[]
    for video_id in video_Ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    video_id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Like=item['statistics'].get('likeCount'),
                    comments=item['statistics'].get('commentCount'),
                    Favorite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    caption_status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data

#get comment information
def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                        video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])


                comment_data.append(data)
    except:
        pass
    return comment_data

#get playlist_details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(playlist_id=item['id'],
                                Title=item['snippet']['title'],
                                channel_id=item['snippet']['channelId'],
                                channel_Name=item['snippet']['channelTitle'],
                                publishedAt=item['snippet']['publishedAt'],
                                video_count=item['contentDetails']['itemCount'])
                        All_data.append(data)
                
                next_page_token=response.get('next_page_Token')
                if next_page_token is None:
                        break
        return All_data
        

# upload to mongoDB
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_videos_info(vi_ids)
    com_deatils=get_comment_info(vi_ids)


    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_deatils})

    return"upload completed successfully"


#Table creation for channels,playlists,videos,comments
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="lovelysubin",
                    database="youtube_data",
                    port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channel'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channel(channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            subscribers bigint,
                                                            views bigint,
                                                            total_Videos int,
                                                            channel_description text,
                                                            playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channel table already created")


    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channel(channel_Name,
                                            channel_Id,
                                            subscribers,
                                            views,
                                            total_videos,
                                            channel_description,
                                            playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['channel_Id'],
                row['subscribers'],
                row['views'],
                row['total_videos'],
                row['channel_description'],
                row['playlist_Id'])
        
        cursor.execute(insert_query,values)
        mydb.commit()



#creating the playlist table

def playlist_table():
    mydb= psycopg2.connect(host="localhost",
                        user="postgres",
                        password="lovelysubin",
                        database="youtube_data",
                        port = "5432")
    cursor = mydb.cursor()

    drop_query = "drop table if exists playlist"
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlist(playlist_id varchar(100) primary key,
                                                        Title varchar(100),
                                                        channelId varchar(100),
                                                        channelName varchar(100),
                                                        publishedAt timestamp,
                                                        video_count int
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)


    for index,row in df1.iterrows():
        insert_query='''insert into playlist(playlist_id,
                                            Title,
                                            channelid,
                                            channelName,
                                            publishedAt,
                                            video_count
                                            )
                                            
                                                    
                                            values(%s,%s,%s,%s,%s,%s)'''
        
        values=(row['playlist_id'],
                row['Title'],
                row['channel_id'],
                row['channel_Name'],
                row['publishedAt'],          
                row['video_count']
                )
        
        cursor.execute(insert_query,values)
        mydb.commit()



# create table for video
def videos_table():
        mydb= psycopg2.connect(host="localhost",
                        user="postgres",
                        password="lovelysubin",
                        database="youtube_data",
                        port = "5432")
        cursor = mydb.cursor()

        drop_query = "drop table if exists videos"
        cursor.execute(drop_query)
        mydb.commit()


        create_query='''create table if not exists videos(channel_Name varchar(100),
                                                        channel_id varchar(100),
                                                        video_id varchar(100) primary key,
                                                        Title varchar(200),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        comments int,
                                                        Favorite_count int,
                                                        Definition varchar(100),
                                                        caption_status varchar(100)
                                                        )'''
        cursor.execute(create_query)
        mydb.commit()

        vi_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
        df2=pd.DataFrame(vi_list)


        for index,row in df2.iterrows():
                insert_query='''insert into videos(channel_Name,
                                                channel_id,
                                                video_id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                comments,
                                                Favorite_Count,
                                                Definition,
                                                caption_status
                                                )
                                                
                                                        
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
                values=(row['channel_Name'],
                        row['channel_id'],
                        row['video_id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published'],
                        row['Duration'],
                        row['Views'],
                        row['Like'],                      
                        row['comments'],
                        row['Favorite_count'],
                        row['Definition'],
                        row['caption_status']
                        )
                
                cursor.execute(insert_query,values)
                mydb.commit()


# create table for comments
def commments_table():
    mydb= psycopg2.connect(host="localhost",
                        user="postgres",
                        password="lovelysubin",
                        database="youtube_data",
                        port = "5432")
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists comments(comment_Id varchar(100)primary key,
                                                            video_id varchar(100),
                                                            comment_text text,
                                                            comment_Author varchar(200),
                                                            comment_Published timestamp
                                                            )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
         st.write('comment table already created ')


    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query='''insert into comments(comment_Id,
                                                    video_id,
                                                    comment_text,
                                                    comment_Author,
                                                    comment_Published
                                                )
                                                
                                                        
                                                values(%s,%s,%s,%s,%s)'''
            
            values=(row['comment_Id'],
                    row['video_id'],
                    row['comment_text'],
                    row['comment_Author'],
                    row['comment_Published']       
                    )
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                 st.write("Commnets values already uploaded")


def tables():
    channels_table()
    playlist_table()
    videos_table()
    commments_table()

    return "table created successfully"


# channels
def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


# playlists
def show_paylists_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1


# videos
def show_videos_table():
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                    vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2



# comments
def show_comments_table():
    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3


# streamlit part

with st.sidebar:
    st.title(":orange[YOUTUBE DATA HAVERDTING AND WAREHOUSING]")
    st.header("skill Take Away")
    st.caption("python scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

Channel_Id=st.text_input("Enter the Channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_Id"])

    if Channel_Id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(Channel_Id)
        st.success(insert)

if st.button("Migrate to sql"):
    Tables=tables()
    st.success(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_paylists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()


#SQL Connection
  
mydb= psycopg2.connect(host="localhost",
                    user="postgres",
                    password="lovelysubin",
                    database="youtube_data",
                    port = "5432")
cursor = mydb.cursor()

question=st.selectbox("Select your question",("1. All the video and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

if question=="1. All the video and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

    
elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as No_videos from channel
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["chammel name","No of videos"])
    st.write(df2)


elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotile"])
    st.write(df3)
    
elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)


elif question=="5. videos with higest likes":                                                #error like table not create
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)


elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname,views as totalviews from channel'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel_name","tottalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,Published_Date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022 '''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averegeduration from videos group by channel_name '''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)

