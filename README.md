# Youtube-Datascrap-Migration-to-Mongodb-to-MySql-Streamlit
In this project i am going to use, Python as my primary language to code, where i am trying to give a connection to both Mongodb and MySql through streamlit. This repository contains a code for youtube data scraping of channel description, channel_published date, channel_thumbnail, channel_Subscriberscount, channel_videocount, channel_viewcount.
and also i am trying to scrap the details of playlist available in the particular channel, and using the playlist ids, i am going to generate the videos available in the whole channel not only that, but also i am going to scrap the details of the video like video name, video description, video published date, the like count, view count,  duration, favourite count, thumbnail, and top 5 comments from all the video in view of comment id, comment publisher, comment date, comment description on basis of Video id through API

After scraping i am going to save all those API referenced JSON code into Mongo DB as it will handle both structured and Unstructered data well. Once it has stored the value, i am going to ask the user whether to scrap a particular youtube channel's content and post it in MYSql on user basis.

Once the user selects particular youtube channel to scrap those details from the MongoDB to MySql. All those particular youtube channel details will be scrapped and will be posted inside the MySql databse.


