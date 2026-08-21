from datetime import date
import os
import json
import requests
# from dotenv import load_dotenv
# import os
# load_dotenv(dotenv_path=".env")

from airflow.decorators import task
from airflow.models import Variable

@task
def fetch_raw_animal_data():
    """
    Fetches raw animal data from the API Ninjas endpoint.
    """
    url = "https://api.api-ninjas.com/v1/animals"
    params = {"name": "a"}
    headers = {"X_Api_Key": Variable.get("X_Api_Key")}
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

@task
def extract_animal_data(raw_data_list):
    extracted_data = []
    
    for item in raw_data_list:
        name = item.get("name", "Unknown")
        
        taxonomy = item.get("taxonomy", {})
        animal_class = taxonomy.get("class", "Unknown")
        family = taxonomy.get("family", "Unknown")
        
        characteristics = item.get("characteristics", {})
        diet = characteristics.get("diet", "Unknown")
        group = characteristics.get("group", "Unknown")
        color = characteristics.get("color", "Unknown")
        lifespan = characteristics.get("lifespan", "Unknown")
        
        animal_data = {
            "name": name,
            "class": animal_class,
            "diet": diet,
            "family": family,
            "group": group,
            "color": color,
            "lifespan": lifespan
        }
        extracted_data.append(animal_data)
        
    return extracted_data

@task
def save_to_json(extracted_data):
    os.makedirs("./data", exist_ok=True)
    
    file_path = f"./data/Animal_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfiles:
        json.dump(extracted_data, json_outfiles, indent=4, ensure_ascii=False)
    print(f"Data successfully saved to {file_path}")

if __name__ == "__main__":
    raw_data = fetch_raw_animal_data()
    if raw_data:
        cleaned_data = extract_animal_data(raw_data)
        save_to_json(cleaned_data)
