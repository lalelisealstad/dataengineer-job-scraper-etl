import requests

# Download the JSONL file from GitHub
list_skills_url = 'https://raw.githubusercontent.com/kingabzpro/jobzilla_ai/main/jz_skill_patterns.jsonl'
response = requests.get(list_skills_url)
skills_jsonl = response.text

with open("skills_en.jsonl", "w") as file:
    file.write(skills_jsonl)

with open('skille_norsk.jsonl', "r") as file:
    local_jsonl = file.read()
    
with open("skills_en.jsonl", "r") as file:
    remote_jsonl = file.read()

combined_jsonl = remote_jsonl + "\n" + local_jsonl

with open("skills_no_en.jsonl", "w") as file:
    file.write(combined_jsonl)
