import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from datetime import datetime
from google.cloud import storage
import os
import spacy
from spacy.pipeline import EntityRuler

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

today = datetime.today().strftime("%d%m%Y")

def main(): 

    # LinkedIn URL for data engineer posted in oslo last 24 h
    list_url = "https://www.linkedin.com/jobs/search?keywords=%22Data%20Engineer%22&location=Oslo&geoId=105719246&f_TPR=r86400&position=1&pageNum=0"

    response = requests.get(list_url)
    list_data = response.text
    soup = BeautifulSoup(list_data, 'html.parser')

    job_cards = soup.find_all('div', class_='job-search-card')

    def fetch_job_description(jobid, retries=5):
        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{jobid}"
        for attempt in range(retries):
            try:
                response = requests.get(job_url)
                time.sleep(2)
                if response.status_code == 200:
                    job_soup = BeautifulSoup(response.text, 'html.parser')
                    description_div = job_soup.find('div', class_='description__text description__text--rich')
                    if description_div:
                        return description_div.get_text(strip=True)
                logging.warning(f"Attempt {attempt+1} failed for jobid {jobid}")
            except Exception as e:
                logging.error(f"Error fetching job description for jobid {jobid} on attempt {attempt+1}: {e}")
            time.sleep(3)
        return None

    jobs_data = []
    for job_card in job_cards:
        data_entity_urn = job_card.get('data-entity-urn')
        if data_entity_urn:
            jobid = data_entity_urn.split(':')[-1]
            job_title_tag = job_card.find('h3', class_='base-search-card__title')
            job_title = job_title_tag.get_text(strip=True) if job_title_tag else None
            logging.info(f"Fetching description for job: {job_title} (ID: {jobid})")
            
            description = fetch_job_description(jobid)
            if description is None:
                logging.warning(f"No description found for job {jobid}")
            jobs_data.append({
                'jobid': jobid,
                'title': job_title,
                'description': description})

    df = pd.DataFrame(jobs_data)

    if len(df) > 0:
        service_account_key = os.getenv('GCP_SECRET')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_key

        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"../service-account-details.json" 

        file_path_gcp = f"gs://oslo-linkedin-dataengineer-jobs/transformed/jobs_{today}.csv"

        # Load the spaCy model
        nlp = spacy.load('en_core_web_lg')

        # Add the EntityRuler to the pipeline and load the patterns from the JSONL file
        ruler = nlp.add_pipe("entity_ruler", before="ner")
        ruler.from_disk("assets/skills_no_en.jsonl")

        # Function to extract skills from text
        def get_skills(text):
            doc = nlp(text)
            list_skills = [ent.text.lower() for ent in doc.ents if ent.label_ == "SKILL"]
            return list(set(list_skills))

        # Apply the get_skills function to the 'description' column
        df['skills'] = df['description'].apply(get_skills)

        # Drop rows with missing values and unnecessary columns
        df = df.dropna(subset=['description'])
        df = df.drop(columns=['title', 'description'])
        
        df.to_csv(file_path_gcp, index=False, sep=';')
        logging.info(f"File for {today} added to gcp")

    else: 
        logging.info('No new jobs posted in the last 24 h, no new file added')
    
    
    
if __name__ == "__main__":
    logging.info(f"Beginning execution for {today}...")
    main()