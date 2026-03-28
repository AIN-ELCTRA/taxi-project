🚖 Taxi Data Engineering & Analytics Dashboard
📌 Overview

This project is a data engineering pipeline and analytics dashboard built using the NYC Taxi dataset.
It demonstrates how to process large-scale data, store it efficiently, and visualize insights through an interactive dashboard.

The project is fully containerized using Docker, ensuring reproducibility and easy setup across different environments.


🏗️ Architecture

Parquet Dataset
      ↓
Python Data Loader (Pandas / PyArrow)
      ↓
PostgreSQL (Docker Container)
      ↓
Streamlit Dashboard (Docker Container)


⚙️ Technologies Used

Python 🐍
Pandas / PyArrow
PostgreSQL 🐘
Streamlit 📊
Docker 🐳 (Containerization)


📊 Dashboard Features

Total Trips KPI
Total Revenue KPI
Average Fare
Average Distance
Trips by Pickup Hour
Daily Trip Trends
Passenger Distribution
Distance Distribution
Fare vs Distance Analysis
Sidebar Filters (Date Range, Distance, Fare)
Modern Dark Theme UI


🐳 Run with Docker (Recommended)

1. Start all services
docker compose up --build -d
2. Load the dataset into PostgreSQL
docker compose exec dashboard python load_data_copy.py
3. Open the dashboard
http://localhost:8501


💻 Run Without Docker (Optional)

Install dependencies
pip install -r requirements.txt
Run dashboard
python -m streamlit run dashboard.py


📂 Project Structure

taxi-project/
│
├── dataset/                 # Raw dataset (ignored in Git)
├── dashboard.py             # Streamlit dashboard
├── load_data_copy.py        # Data loading script (COPY method)
├── docker-compose.yml       # Docker services
├── Dockerfile               # Streamlit container setup
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (optional)
├── .gitignore               # Ignored files
└── README.md                # Documentation
