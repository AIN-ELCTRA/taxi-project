# 🚖 Taxi Data Engineering & Analytics Dashboard

## 📌 Overview
This project is a **data engineering pipeline + analytics dashboard** built using the NYC Taxi dataset.  
It demonstrates how to process raw data and visualize key insights using modern tools.

The project focuses on:
- Efficient data ingestion
- Data transformation & storage
- Interactive analytics dashboard

---

## 🏗️ Architecture
Parquet Dataset
↓
Python Processing (Pandas / PyArrow)
↓
Data Cleaning & Transformation
↓
Batch Processing
↓
PostgreSQL Database
↓
Streamlit Dashboard


---

## ⚙️ Technologies Used

- Python 🐍
- Pandas / PyArrow
- PostgreSQL 🐘
- Docker 🐳
- Streamlit 📊

---

## 📊 Dashboard Features

- Total Trips
- Total Revenue
- Average Fare
- Average Distance
- Trips by Pickup Hour
- Daily Trip Trends
- Date Range Filtering (Sidebar)
- Modern Dark Theme UI

---

## 🚀 How to Run the Project

### 1. Clone the repository
```bash
git clone https://github.com/AIN-ELCTRA/taxi-project.git
cd taxi-project

2. Install dependencies
pip install pandas streamlit psycopg2 matplotlib

3. Run the dashboard
python -m streamlit run dashboard.py

4. Open in browser
http://localhost:8501
taxi-project/
│
├── dataset/                # Raw dataset (ignored in Git)
├── dashboard.py            # Streamlit dashboard
├── producer.py             # Data ingestion
├── load_data_copy.py       # Batch loading script
├── db_test.py              # Database connection test
├── docker-compose.yml      # PostgreSQL container
├── README.md               # Project documentation
└── .gitignore              # Ignore unnecessary files

