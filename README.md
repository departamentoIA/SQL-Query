# SQL-Query
Connection to a DB by using SQL Server and Polars for massive queries. The result is saved in an Excel file and this file is divided in parts of 1 million rows.

## 🌎 Repository Structure
```
SQL-Query/
├── main.py
├── .gitignore
├── env/                # Virtual enviroment (not provided)
└── requirements.txt
└── pkg                 # Contains all needed files (Python package)
    └── __init__.py     # Specifies that folder 'pkg' is a Python package
    └── extract.py      # Contains all functions related to extraction process
    └── transform.py    # Contains all functions related to transform process
    └── globals.py      # Contains all global variables
    └── config.py       # Contains all configuration params
    └── .env            # Contains all secret data (not provided)
```


## ✨ Details

**main.py:** This script calls 'extract.py' to obtain the .

## 🚀 How to run locally
1. Clone this repository:
```
git clone https://github.com/departamentoIA/SQL-Query.git
```
2. Set virtual environment and install dependencies.

For Windows:
```
python -m venv env
env/Scripts/activate
pip install -r requirements.txt
```
For Linux:
```
python -m venv env && source env/bin/activate && pip install -r requirements.txt
```
3. Create your ".env" file, which has the following form:
```
DB_SERVER=10.0.00.00,5000
DB_NAME=SAT
DB_USER=caarteaga
DB_PASSWORD=pa$$word
```
4. Run "main.py".
