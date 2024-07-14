import time
import getpass
import pandas as pd
from bs4 import BeautifulSoup
import requests







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

#### Get descriptions func
# Function to fetch job description using jobid
def fetch_job_description(jobid):
    job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{jobid}"
    time.sleep(3)
    response = requests.get(job_url)
    job_soup = BeautifulSoup(response.text, 'html.parser')
    description_div = job_soup.find('div', class_='description__text description__text--rich')
    if description_div:
        return description_div.get_text(strip=True)
    return None


# Loop through each job card and extract the jobid
jobs_data = []
for job_card in job_cards:
    data_entity_urn = job_card.get('data-entity-urn')
    if data_entity_urn:
        jobid = data_entity_urn.split(':')[-1]
        job_title_tag = job_card.find('h3', class_='base-search-card__title')
        job_title = job_title_tag.get_text(strip=True) if job_title_tag else None
        print(job_title)
        
        description = fetch_job_description(jobid)
        jobs_data.append({
            'jobid': jobid,
            'title': job_title,
            'description': description})

df = pd.DataFrame(jobs_data)
df.to_csv('.jobs.csv')
