# use corpus skills from SpaCy # load the required libraries and create an nlp object
# nlp = spacy.load('en_core_web_sm')


# https://www.escoe.ac.uk/the-skills-extractor-library/

#  https://github.com/kingabzpro/jobzilla_ai/blob/main/jz_skill_patterns.jsonl

# import requests

# list_skills = 'https://raw.githubusercontent.com/kingabzpro/jobzilla_ai/main/jz_skill_patterns.jsonl'


# # python -m spacy download en_core_web_lg 

import requests
import spacy
from spacy.pipeline import EntityRuler

# Load the spaCy model
nlp = spacy.load('en_core_web_lg')


# Add the EntityRuler to the pipeline and load the patterns from the JSONL file
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.from_disk("skills_no_en.jsonl")

# Function to extract skills from text
def get_skills(text):
    doc = nlp(text)
    list_skills = []
    for ent in doc.ents:
        if ent.label_ == "SKILL":
            list_skills.append(ent.text.lower())
    return list(set(list_skills))

# Example usage
sample_text = "airflow og python til utvikling"
found_skills = get_skills(sample_text)
print(found_skills)
