# Data Engineer Job Scraper ETL

try to add service_account_key: ${{ secrets.GCP_SA_KEY } to script? maybe it is not necessary to set vars there

This repository contains the source code for a data pipeline that automatically scrapes Data Engineer job postings every night using Google Cloud Platform (GCP) tools: Cloud Scheduler, Pub/Sub, Cloud Functions, and Cloud Storage. The program collects job descriptions for positions in Oslo, Norway posted in the last 24 hours and uses the spaCy NLP package to extract words describing "skills" needed for the positions. The purpose of the pipeline is to analyze in-demand skills for data engineering positions. A dashboard to visualize the results will be created in another repository.

#### Deployment Notes:
The program is deployed in Cloud Functions using GitHub Actions. It is triggered daily by a Pub/Sub message from Cloud Scheduler at 23:00 CET.

#### Development Notes:
To run the program for the first time:
```
$ python3 -m venv .venv
$ source .venv/bin/activate 
$ pip install -r requirements.txt
$ python -m spacy download en_core_web_lg 
```

Run the program after installation:
```
$ source .venv/bin/activate
$ python "main.py"
```


### Resources
I used this approach for scraping Linkedin data: 
https://medium.com/@alaeddine.grine/linkedin-job-scraper-and-matcher-85d0308ef9aa 

Used skills file from: 
https://raw.githubusercontent.com/kingabzpro/jobzilla_ai/main/jz_skill_patterns.jsonl