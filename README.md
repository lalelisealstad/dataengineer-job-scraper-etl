# dataengineer-skills-scraper-etl

Run first time:
$ python3 -m venv .venv
$ source .venv/bin/activate 
$ pip install -r requirements.txt
python -m spacy download en_core_web_lg 

run after installation:
$ source .venv/bin/activate
$ python "main.py"


I used this approach for scraping Linkedin data: 
https://medium.com/@alaeddine.grine/linkedin-job-scraper-and-matcher-85d0308ef9aa 

Used skills file from: 
https://raw.githubusercontent.com/kingabzpro/jobzilla_ai/main/jz_skill_patterns.jsonl