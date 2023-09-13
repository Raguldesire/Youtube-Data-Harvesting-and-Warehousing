### Youtube-Data-Harvesting-and-Warehousing
_____________________________________________________________________________________________________________________________________________
### Introduction
This is my first Python Project to scrap data from YouTube using YouTube Data API and store the data in a MongoDB database as a data lake,
After that the data is migrated from the data lake to a SQL database as tables and are displayed in the streamlit application.
_____________________________________________________________________________________________________________________________________________
### skills covered:
* Data Collection
* Data Storage
* Data Warehousing
* Data Analysis
* Data Management using MongoDB and PostgreSQL
_____________________________________________________________________________________________________________________________________________
### Tools used :
* Virtual studio
* python 3.11 or higher
* postgreSQL
* MongoDB
* Youtube API key
_____________________________________________________________________________________________________________________________________________
### Libraries used:
* File handling libraries(json)
* pymongo
* pandas and numpy
* Dashboard libraries(streamlit)
 _____________________________________________________________________________________________________________________________________________
### ETL process:
* Extract data:
	Extract the data from any particular youtube channel by using channel_id and with the help of youtube API developer console
* Tranform the data:
	Once extraction done,we need to convert the extraction data into JSON format.
* Load data:
	Once we convert into JSON format,data is to be stored in MongoDB database.
_____________________________________________________________________________________________________________________________________________
### EDA process:
* Access postgreSQL DB:
	Connect the PostgreSQL server and access table
* Filter the data:
	Process the collected data and filter it based on the requirements
* Visualization:
	We have to create a dashboard using streamlit and visualize the user need
