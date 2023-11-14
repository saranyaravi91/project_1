# project_1
     youtube data harvesting and warehousing
This streamlit app will get the youtube channel ID from the user and get channel details, playlist details, video details and comment details from Youtube API V3 and then transform the data into a dictionary and load into MongoDB for further steps.
We can select which channel details we need to migrate from MongoDB to MYSQL in a Drop down with dynamic values.
Once migrated we can do some Queries from MySQL DB

Youtube API to MongoDB
1.Enter API Key which is created from Google developer console to connect with the Youtube API V3
2.Enter Youtube Channel ID to get the details
3.Once Search button is clicked, the script will fetch the details from Youtube API and store the details in a dictionary.
4.The data will be displayed in json format in Streamlit App
5.We can load the data into MongoDB as a single document in a collection which was already created in MongoDB
6.Once loaded, a success message will be displayed

MongoDB to MYSQL DB
1.In the second tab, a drop down will be displayed with the channel names whatever available in the MongoDB
2.We can select a channel and migrate it to MySQL
3.Details fetched from MongoDB will be kept in separate dataframes like channel_df, playlists_df,videos_df and comments_df.
4.From dataframe we will be inserting the data into MySQL. Please note there should be schemas created already in MySQL to append the data
5.Once data is migrated, a success message will be displayed.

SQL Query
1.In the third tab, a drop down will be displayed with set of questions related to the data in the MySQL tables(channel_details,playlist_details,video_details,comment_details).
2.If any one of the question is selected and "Go" button is clicked, result will be displayed in table format in streamlit App.
