# Databricks notebook source
# DBTITLE 1,Install required external libraries 
# MAGIC %pip install mlflow langchain databricks-vectorsearch==0.22 langchain_community tiktoken langchain_openai
# MAGIC

# COMMAND ----------

# MAGIC %pip install --upgrade langchain

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

 
from pyspark.sql.functions import pandas_udf
import pandas as pd
import pyspark.sql.functions as F
import requests
from pyspark.sql.types import StringType

# COMMAND ----------

#from langchain.chains.summarize import load_summarize_chain
from langchain_community.document_loaders import WebBaseLoader
import requests

loader = WebBaseLoader("https://www.morganstanley.com/ideas/global-macro-economy-outlook-2024")
docs = loader.load()


# COMMAND ----------

from langchain.text_splitter import RecursiveCharacterTextSplitter


splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    chunk_size=500, chunk_overlap=0
)

splits = splitter.split_documents(docs);

# COMMAND ----------

content_df = spark.createDataFrame(list(map(lambda x: x.page_content, splits)), StringType()).toDF("content")
content_df.show()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd 
import mlflow.deployments
deploy_client = mlflow.deployments.get_deploy_client("databricks")

@pandas_udf("array<float>")
def get_embeddings(content: pd.Series) -> pd.Series: 
  response = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": content.tolist()})
  embeddings = [e['embedding'] for e in response.data]
  return pd.Series(embeddings)

# COMMAND ----------

content_w_embedding = content_df.withColumn("embeddings", get_embeddings("content"))
content_w_embedding.show()

# COMMAND ----------

# MAGIC %sql create catalog parviz

# COMMAND ----------

# MAGIC %sql create schema parviz.rag

# COMMAND ----------

# MAGIC %sql drop table parviz.rag.parviz_source_rag_table

# COMMAND ----------

content_w_embedding \
# How do I add identity columns to this table?
from pyspark.sql.functions import monotonically_increasing_id 

content_w_embedding \
  .withColumn("id", monotonically_increasing_id()) \
  .write.mode("overwrite").option("delta.enableChangeDataFeed", True) \
  .saveAsTable("parviz.rag.parviz_source_rag_table")

# COMMAND ----------

VECTOR_SEARCH_ENDPOINT_NAME= 'parviz_vs_endpoint_v1'

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if VECTOR_SEARCH_ENDPOINT_NAME not in [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]:
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")


# COMMAND ----------

# vsc.create_delta_sync_index(
#   endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
#   index_name="parviz.rag.parviz_source_rag_index_table",
#   source_table_name="parviz.rag.parviz_source_rag_table",
#   pipeline_type="TRIGGERED",
#   primary_key="id",
#   embedding_dimension=1024, #Match your model embedding size (bge)
#   embedding_vector_column="embeddings"
# )
# else:
# #Trigger a sync to update our vs content with the new data saved in the table
vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, "parviz.rag.parviz_source_rag_index_table").sync()

# COMMAND ----------

import mlflow.deployments
deploy_client = mlflow.deployments.get_deploy_client("databricks")

question = "what are trends in growth?"
response = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": [question]})
embeddings = [e['embedding'] for e in response.data]

results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, "parviz.rag.parviz_source_rag_index_table").similarity_search(
  query_vector=embeddings[0],
  columns=["content"],
  num_results=3)
docs = results.get('result', {}).get('data_array', [])
docs

# COMMAND ----------

embeddings

# COMMAND ----------


