import requests
from bs4 import BeautifulSoup
import polars as pl
import time
import logging
from datetime import datetime
from google.cloud import storage
import spacy
from spacy.pipeline import EntityRuler
import gcsfs
import re
import urllib


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

today = datetime.today().strftime("%d%m%Y")


def fetch_job_description(jobid, retry_delay, retries):
    job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{jobid}"
    for attempt in range(retries):
        try:
            response = requests.get(job_url)
            time.sleep(retry_delay)
            if response.status_code == 200:
                job_soup = BeautifulSoup(response.text, 'html.parser')
                description_div = job_soup.find('div', class_='description__text description__text--rich')
                if description_div:
                    return description_div.get_text(strip=True)
            logging.warning(f"Attempt {attempt+1} failed for jobid {jobid}")
        except Exception as e:
            logging.error(f"Error fetching job description for jobid {jobid} on attempt {attempt+1}: {e}")
        time.sleep(retry_delay)
    return None

def main(pubsub_message, pubsub_context): 
    
# def main():
    # import os 
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"../service-account-details.json" 

    # Load the spaCy model
    import en_core_web_sm
    nlp = en_core_web_sm.load( )

        
    ###################### Receive job title from pubsub #########################

    # job_titles = ["Data%20Engineer", "Data%20Scientist", "Data%20Analyst"]. adding the %20 as this is used as space in the url 
    
    if 'data' not in pubsub_message:
        logging.warning('No data field found in the message. End process')
        return

    # Check if 'attributes' and 'job_title' exist in the message
    if 'attributes' in pubsub_message and 'job_title' in pubsub_message['attributes']:
        job_title_encoded = pubsub_message['attributes']['job_title']
        job_title = urllib.parse.unquote(job_title_encoded)
        job_title_found = True
        logging.info(f"Received job title: {job_title} from pubsub message by cloud scheduler: {pubsub_message}")
    else:
        logging.warning('No job_title attribute found in the message attributes. End process')

    
    ################## Extract job descriptions ##################
    if job_title_found == True: 
        logging.info(f"starting web scraping for {job_title}")
        
        # LinkedIn URL for jobs posted in last 24 h
        # Oslo
        # list_url = "https://www.linkedin.com/jobs/search?keywords=%22Data%20Engineer%22&location=Oslo&geoId=105719246&f_TPR=r86400&position=1&pageNum=0"
        # London
        list_url = f"https://www.linkedin.com/jobs/search?keywords=%22{job_title}%22&location=London%20Area%2C%20United%20Kingdom&geoId=90009496&f_TPR=r86400&position=1&pageNum=0"

        df = pl.DataFrame(schema={'jobid': pl.String, 'title': pl.String, 'description': pl.String, 'job_type': pl.String})
        max_retries = 4
        retry_delay = 3  # seconds

        for attempt in range(max_retries):
            try:
                # find all job postings job ids
                response = requests.get(list_url)
                response.raise_for_status()  # Raise an exception for HTTP errors
                list_data = response.text
                soup = BeautifulSoup(list_data, 'html.parser')
                job_cards = soup.find_all('div', class_='job-search-card')
                
                if job_cards:  # If job_cards is not empty
                    logging.info(f"Successfully retrieved job cards for {job_title} on attempt {attempt + 1}")
                    
                    # get description for each job posting in the page
                    jobs_data = []
                    # Get first 25 jobs only 
                    for job_card in job_cards[:25]: 
                        data_entity_urn = job_card.get('data-entity-urn')
                        if data_entity_urn:
                            jobid = data_entity_urn.split(':')[-1]
                            job_title_tag = job_card.find('h3', class_='base-search-card__title')
                            job_title_tag = job_title_tag.get_text(strip=True) if job_title_tag else None
                            logging.info(f"Fetching description for job ID: {jobid})")
                            
                            description = fetch_job_description(jobid, retry_delay, max_retries)
                            if description is None:
                                logging.warning(f"No description found for job {jobid}")
                            jobs_data.append({
                                'jobid': jobid,
                                'title': job_title_tag,
                                'description': description, 
                                'job_type': job_title})
                    
                    df_job = pl.DataFrame(jobs_data)
                    
                    if len(df_job) > 0: 
                        df = pl.concat([df, df_job])
                    break
                
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        else:
            logging.warning(f"Failed to retrieve job cards for {job_title} after {max_retries} attempts. end process for job title")


        ################## Transform dataframe ##################
        if len(df) > 0:

            # Add the EntityRuler to the pipeline and load the patterns from the JSONL file
            ruler = nlp.add_pipe("entity_ruler", before="ner")
            ruler.from_disk("assets/skills_no_en.jsonl")

            # Function to extract skills from text
            def get_skills(text):
                doc = nlp(text)
                list_skills = [ent.text.lower() for ent in doc.ents if ent.label_ == "SKILL"]
                return list(set(list_skills))
            
            # Extract pay text 
            def extract_numbers(text):
                pattern = r'£[\d,]+K?|[\d,]+ ?GBP'
                matches = re.findall(pattern, text)
                if matches:
                    numbers = [match.replace('£', '').replace('GBP', '').replace('K', '000').replace(',', '').replace(' ', '') for match in matches]
                    return float(numbers[-1])
                else:
                    return None

            # Drop rows with missing values in 'description'
            df = df.drop_nulls(subset=["description"])

            # extract pay 
            df = df.with_columns(
                pl.col('description').map_elements( lambda x : extract_numbers(x), return_dtype=pl.Float64).alias('pay'))    

            # Apply the get_skills function to the 'description' column
            df = df.with_columns(
                pl.col("description").map_elements(lambda x: get_skills(x), return_dtype=pl.List(pl.String)).alias("skills")
            )

            # Extract seniority 
            df = df.with_columns(
                pl.when(pl.col("title").str.contains("(?i)senior")).then(pl.lit("Senior"))
                .when(pl.col("title").str.contains("(?i)junior|graduate")).then(pl.lit("Junior"))
                .when(pl.col("title").str.contains("(?i)manager|head")).then(pl.lit("Manager"))
                .when(pl.col("title").str.contains("(?i)principal|lead")).then(pl.lit("Lead"))
                .otherwise(None)
                .alias("seniority")
            )
            
            df = df.with_columns(pl.lit(datetime.today()).alias("date"))
            
            df = df.drop(['title', 'description'])
            
            
            ############### Load data in gcs #############
            
            # store file in gcp
            file_path_gcp = f"gs://oslo-linkedin-dataengineer-jobs/transformed/{job_title}_jobs_{today}.parquet"
            
            fs = gcsfs.GCSFileSystem()
            with fs.open(file_path_gcp, mode='wb') as f:
                df.write_parquet(f)
            
            logging.info(f"File for {today} added to gcp")

        else: 
            logging.info('No new jobs posted in the last 24 h, no new file added')

    
if __name__ == "__main__":
    logging.info(f"Beginning execution for {today}...")
    main()