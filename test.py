from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from dotenv import load_dotenv
import os
import time
import getpass
import pandas as pd
from bs4 import BeautifulSoup
import requests
import math, re, sys


list_url = "https://www.linkedin.com/jobs/search/?keywords=Data%20Engineer%20Oslo%20&location=Oslo%2C%20Norway&geoId=105719246&f_TPR=r86400&position=1&pageNum=0"
response = requests.get(list_url)
# print(response.text)

list_data = response.text
soup = BeautifulSoup(list_data, 'html.parser')
job_card = soup.find('div', class_='job-search-card')
# print(job_card)

# Extract the jobid from the data-entity-urn attribute
data_entity_urn = job_card.get('data-entity-urn').split(':')[3]
# print(data_entity_urn)
### WORKS ABOVE###

soup = BeautifulSoup(list_data, 'html.parser')

# # Find all job cards
job_cards = soup.find_all('div', class_='job-search-card')

# Loop through each job card and extract the jobid
job_ids = []
jobs_data = []
for job_card in job_cards:
    data_entity_urn = job_card.get('data-entity-urn')
    if data_entity_urn:
        jobid = data_entity_urn.split(':')[-1]
        job_ids.append(jobid)
        job_title_tag = job_card.find('h3', class_='base-search-card__title')
        job_title = job_title_tag.get_text(strip=True) if job_title_tag else None
        print(job_title)
        jobs_data.append({
            'jobid': jobid,
            'title': job_title})

# Print all extracted job ids
for jobid in job_ids:
    print("Job ID:", jobid)
    
    
## works aobe

job_url=f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{jobid}"

# Function to fetch job description using jobid
def fetch_job_description(jobid):
    job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{jobid}"
    time.sleep(2)
    response = requests.get(job_url)
    job_soup = BeautifulSoup(response.text, 'html.parser')
    description_div = job_soup.find('div', class_='description__text description__text--rich')
    if description_div:
        return description_div.get_text(strip=True)
    return None

# # Fetch job descriptions for all job ids
job_descriptions = {}
for jobid in job_ids:
    description = fetch_job_description(jobid)
    job_descriptions[jobid] = description

# Print all job descriptions
for jobid, description in job_descriptions.items():
    print(f"Job ID: {jobid}\nDescription: {description}\n")

# print(jobs_data)

########### Build it together 


# # Loop through each job card to extract jobid and title
# jobs_data = []
# for job_card in job_cards:
#     data_entity_urn = job_card.get('data-entity-urn')
#     if data_entity_urn:
#         jobid = data_entity_urn.split(':')[-1]
        
#         # Extract the job title
#         job_title_tag = job_card.find('h3', class_='base-search-card__title')
#         job_title = job_title_tag.get_text(strip=True) if job_title_tag else None
        
#         # Fetch job description
#         job_description = fetch_job_description(jobid)
        
#         # Store the job data
#         jobs_data.append({
#             'jobid': jobid,
#             'title': job_title,
#             'description': job_description
#         })
#         time.sleep(2)

# # Print all job data
# for job in jobs_data:
#     print(f"Job ID: {job['jobid']}")
#     print(f"Title: {job['title']}")
#     print(f"Description: {job['description']}\n")