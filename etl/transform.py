
# https://www.escoe.ac.uk/the-skills-extractor-library/

#  https://github.com/kingabzpro/jobzilla_ai/blob/main/jz_skill_patterns.jsonl

# # python -m spacy download en_core_web_lg 
# installed java 

import requests
import spacy
from spacy.pipeline import EntityRuler
from datetime import datetime
import pandas as pd
from google.cloud import storage
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as F


# Load the spaCy model
nlp = spacy.load('en_core_web_lg')

# Add the EntityRuler to the pipeline and load the patterns from the JSONL file
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.from_disk("../assets/skills_no_en.jsonl")

today = datetime.today().strftime("%d%m%Y")
bucket_name = "oslo-linkedin-dataengineer-jobs" 
source_file_name = f"extracted/jobs_{today}.csv"
destination_file_name = f"transformed/jobs_{today}.parquet"
source_gcs_uri =      f"gs://{bucket_name}/{source_file_name}" 
destination_gcs_uri = f"gs://{bucket_name}/{destination_file_name}" 

# Function to extract skills from text
def get_skills(text):
    doc = nlp(text)
    list_skills = []
    for ent in doc.ents:
        if ent.label_ == "SKILL":
            list_skills.append(ent.text.lower())
    return list(set(list_skills))


# Define a Pandas UDF for the get_skills function
@F.pandas_udf(ArrayType(StringType()))
def get_skills_udf(text_series):
    return text_series.apply(get_skills)

# Initialize the Spark session
spark = SparkSession.builder \
    .appName('pyspark-run-with-gcp-bucket') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .config("spark.sql.execution.arrow.pyspark.enabled", True) \
    .getOrCreate()

# Set GCS credentials 
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/Users/elisealstad/code/service-account-details.json")

# Read CSV file from GCS into DataFrame
df = spark.read.csv(source_gcs_uri, header=True, inferSchema=True, sep = ';')

df = df.withColumn("skills", get_skills_udf(F.col("description")))

df = df.dropna()
df = df.drop("title").drop("description")

df.write.parquet(destination_gcs_uri)

spark.stop()