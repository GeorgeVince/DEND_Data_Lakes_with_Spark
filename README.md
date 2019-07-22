# Project 4 - Data Lakes

## Project Summary:
To build a data pipeline using Spark to process data from JSON files in an S3 bucket into a star schema, stored in parquet files an S3 bucket.

The final star schema should be designed to efficiently produce results to analytical queries, this will involve partitioning the data set.

The source files reside in a fictional company's (Sparkify) data lake, these need to be parsed into a format suitable for analytical queries.

## Data Lake
In a data lake the data is stored in both structured and unstructured format.  The structure of the data is not defined when the data is captured, it is then transformed and loaded into a suitable format for analytical queries at a later stage.

This brings an advantage because companies can store all of their data - without the need to know the design of schemas, and questions that need to be answered. 

This ultimately leads to giving the business the ability to harness more data, from different sources in much less time than typical data warehouse architecture.  

## Data Sources
##### Song data: `s3://udacity-dend/song_data`

A number of JSON files. partitioned by the first three letters of each song's track ID.  Used to produce the `artist`, `songs` and `song_plays` tables.

##### Log data: `s3://udacity-dend/log_data`

JSON log files, partitioned by year and month.  Used to produce the `users`, `time` and `song_plays` tables.


## Project Files
- `etl.py` -  contains the data pipeline methods to process the log and songs data.
- `dl.cfg` - AWS secret ID and secret key credentials. 

## Usage 
- Ensure a valid AWS key and secret and placed in `dl.cfg`
- Run `etl.py`

## Output
A star schema with the `song_plays` as the fact table, and dimension tables of `users`, `artists`, `time` and `songs`.