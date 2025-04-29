from airflow import DAG
from airflow.decorators import task

import pandas as pd

from datetime import datetime
import requests
import re
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
MSG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants
RATES_CURRENCY = {
    'gbp': 1.56,
    'eur': 1.3,
    'usd': 1,
}
BRACKET_REMOVE = re.compile(r"\[.*?\]")
 

def fetch_budget(url: str) -> str:
    """
    Fetches the 'Budget' field from a given movie detail URL.
    Args:
        url (str): The URL to fetch the budget from.
    Returns:
        str: The budget value as a string. Returns "0.0" if not found, or an error message if the request fails.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        budget = response.json().get("Budget", "0.0")
        return budget
    except requests.RequestException as e:
        logger.error(f"Request error while fetching budget from {url}: {e}")
        return f"Error: {e}"
    except ValueError as e:
        logger.error(f"JSON decoding error from {url}: {e}")
        return "Error: Invalid JSON response"
    except Exception as e:
        logger.error(f"Unexpected error fetching budget from {url}: {e}")
        return f"Error: {e}"

        
def parse_budget_to_usd(budget_str: str):
    """
    Parses a budget string with currency symbols and units and converts it into an integer USD amount.
    Args:
        budget_str (str): The raw budget string to parse.
    Returns:
        int: The parsed budget in USD. Returns 0 if the input is invalid or cannot be parsed.
    """
    if not isinstance(budget_str, str) or budget_str.strip() in ('', '0.0', '0') or budget_str.startswith("Error"):
        return 0
    
    cleaned_budget_str = BRACKET_REMOVE.sub("", budget_str)   
    cleaned_budget_str = cleaned_budget_str.replace("US$", "").replace("USD$", "").replace("$", "").replace("–", "-").strip()
    
    currency = 'usd'
    if "£" in cleaned_budget_str or "₤" in cleaned_budget_str:
        currency = 'gbp'
    elif "€" in cleaned_budget_str:
        currency = 'eur'
    
    cleaned_budget_str = cleaned_budget_str.replace("£", "").replace("₤", "").replace("€", "").strip()

    parts = cleaned_budget_str.split()
    
    num_part = parts[0].replace(",", "")
    
    if num_part.count('.') > 1:
        num_part = num_part.replace('.', '')
    
    if "-" in num_part:
        num_part = num_part.split('-', 1)[0]
    
    try:
        value = float(num_part)
    except Exception as e:
        logger.error(f"Error parsing budget value from {budget_str}: {e}")
        return 0
        
    unit = parts[1].lower() if len(parts) > 1 else ""
    multiplier = 1_000_000 if unit.startswith("million") else 1
    
    rate = RATES_CURRENCY[currency]
    return int(value * multiplier * rate)


with DAG(
    dag_id="api_consume_dag",
    #schedule_interval=None,
    description="Process Oscars movies data",
    start_date=datetime(2025, 4, 28),
    catchup=False,
    tags=['yipitdata']
) as dag:
    
    @task
    def get_oscars_movies_information():
        response = requests.get("http://oscars.yipitdata.com/")
        response.raise_for_status()
        data = response.json()
        logger.info(f"Response from API: {response}")
        return data
    
    @task
    def json_to_df(data):
        df = pd.json_normalize(
            data,
            record_path=["results", "films"],
            meta=[["results", "year"]],
            errors="ignore"
        )
        
        rename_map = {
            "Film": "film",
            "Wiki URL": "wikipedia_url",
            "Winner": "oscar_winner",
            "Detail URL": "detail_url",
            "results.year": "year"
        }
        df = df.rename(columns=rename_map)
        
        select_fields = [
            'film',
            'wikipedia_url',
            'oscar_winner',
            'detail_url',
            'year',
        ]
        df = df[select_fields]

        dtype_map = {
            "film": "string",
            "wikipedia_url": "string",
            "oscar_winner": "boolean",
            "detail_url": "string",
            "year": "string",
        }
        df = df.astype(dtype_map)
        
        return df

    @task
    def get_budget(df):
        urls = df["detail_url"].tolist()
        with ThreadPoolExecutor(max_workers=32) as executor:
            budgets = list(executor.map(fetch_budget, urls))

        df['budget'] = budgets
        df['budget'] = df['budget'].astype('string')
        return df
    
    @task
    def process_budget(df):
        df['budget_usd'] = df['budget'].apply(parse_budget_to_usd)
        return df
    
    @task   
    def process_year(df):
        df['year'] = df['year'].astype(str)
        df['year'] = df['year'].apply(lambda x: x.split()[0] if isinstance(x, str) else "1900")
        df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(1900).astype(int)
        # Calculate decade
        df['decade'] = (df['year'] // 10) * 10
        return df
    
    @task
    def save_to_csv(df):
        logger.info("Schema of the DataFrame:")
        logger.info(df.dtypes)
        
        logger.info("Sample of the DataFrame:")
        logger.info("\n%s", df.sample(10).to_string())
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
        file_path = f'/tmp/output/{timestamp}_oscars_movies.csv'
        df.to_csv(file_path, index=False)
        return file_path
    

    raw = get_oscars_movies_information()
    df = json_to_df(raw)
    df = get_budget(df)
    df = process_budget(df)
    df = process_year(df)
    file_path = save_to_csv(df)
    