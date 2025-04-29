# YipitData

This project involves building a data pipeline using Apache Airflow to extract, transform, and load data on Oscar-nominated movies from 1927 to 2014. 

## How to set up the Airflow instance and run it locally

### Prerequisites
- Install [Docker](https://docs.docker.com/get-docker/)
- Install [Docker Compose](https://docs.docker.com/compose/install/)


### 1. Execute docker compose
This command build the image and create all containers defined in the docker-compose.yaml file

```bash 
docker compose up -d
```

### 2. Open the Airflow Webserver
🔗 [Access Airflow UI](http://localhost:8080/) 

🔑 **Login Credentials:**
- Username: `airflow`
- Password: `airflow`


### 3. Execute the DAG 'api_consume_dag'
Execute the **api_consume_dag** DAG and verify that a CSV file is generated in the /output folder. 
The filename will follow this structure:

/output/YYYY-MM-DD_HH-MM-SS_oscars_movies.csv

Example of Output Data:

| Film                  | Wikipedia URL                                                      | Oscar Winner | Detail URL                                                    | Year | Budget                   | Budget (USD) | Decade |
|------------------------|---------------------------------------------------------------------|--------------|---------------------------------------------------------------|------|--------------------------|--------------|--------|
| Wings                  | [Link](http://en.wikipedia.org/wiki/Wings_(1927_film))              | TRUE         | [Link](http://oscars.yipitdata.com/films/Wings_(1927_film))    | 1927 | US$ 2 million [4]        | 2000000      | 1920   |
| The Racket             | [Link](http://en.wikipedia.org/wiki/The_Racket_(1928_film))         | FALSE        | [Link](http://oscars.yipitdata.com/films/The_Racket_(1928_film)) | 1927 | 0.0                      | 0            | 1920   |
| Seventh Heaven         | [Link](http://en.wikipedia.org/wiki/Seventh_Heaven_(1927_film))     | FALSE        | [Link](http://oscars.yipitdata.com/films/Seventh_Heaven_(1927_film)) | 1927 | $1.3 million [1]         | 1300000      | 1920   |
| Alexander's Ragtime Band | [Link](http://en.wikipedia.org/wiki/Alexander%27s_Ragtime_Band_(film)) | FALSE   | [Link](http://oscars.yipitdata.com/films/Alexander%2527s_Ragtime_Band_(film)) | 1938 | $1,200,000–$2,275,000 [1] | 1200000      | 1930   |
<br>

## How to stop the Airflow instance 
- Stop services only
```bash 
docker compose stop
```

- Stop and remove containers
```bash 
docker compose down 
```
<br>

## How to Run Unit Tests

### 1. Set up the virtual environment and install dependencies
First, create a virtual environment, activate it, and install the required dependencies:

```bash 
python3.9 -m venv myenv39
source myenv39/bin/activate
pip install --upgrade pip
pip install -r requirements_test.txt
```

### 2. Run the tests
Run the unit tests using pytest:
```bash 
python -m pytest tests/api_consume_test.py
```

### 3. After the tests finish, you can deactivate the virtual environment:
```bash 
deactivate
```

## Assumptions

- If the year is missing, invalid, or cannot be parsed correctly, it will be set to a default value of "1900" to ensure that the processing continues without errors. 


