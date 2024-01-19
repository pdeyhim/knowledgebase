# Databricks notebook source
# DBTITLE 1,Install required external libraries 
# MAGIC %pip install  --upgrade langchain langchain_community tiktoken
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from langchain_community.document_loaders import WebBaseLoader    #Importing required library
import requests   

loader = WebBaseLoader("https://www.morganstanley.com/ideas/global-macro-economy-outlook-2024")   #Defining a web loader object to load content from a url
docs = loader.load()    #Storing the loaded content in the 'docs' variable


# COMMAND ----------

# Importing the RecursiveCharacterTextSplitter class from langchain.text_splitter
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Creating an instance of RecursiveCharacterTextSplitter with a chunk size of 500 and a chunk overlap of 0
splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(chunk_size=500, chunk_overlap=0)

# Splitting the loaded documents 'docs' as per the chunk specifications above and storing them in 'splits' variable
splits = splitter.split_documents(docs);


# COMMAND ----------

from pyspark.sql.types import StringType

content_df = spark.createDataFrame(list(map(lambda x: x.page_content, splits)), StringType()).toDF("content")
content_df.show()
