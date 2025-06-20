# Spain New Vehicle Registration
This repository focuses on the analysis of new car registrations in Spain. The goal is to explore trends, patterns, and insights from vehicle registration data, such as monthly volumes, popular brands or models, and regional distribution.

The dataset is openly provided by the Dirección General de Tráfico (DGT), Spain's national traffic authority. The data is updated monthly and is available for free at the following URL:

🔗 [DGT - Monthly Car Registrations](https://www.dgt.es/menusecundario/dgt-en-cifras/matraba-listados/matriculaciones-automoviles-mensual.html)


This project can be useful for researchers, analysts, and policy makers interested in understanding the automotive market, mobility trends, or environmental impact based on the evolution of new vehicle registrations in Spain.

The code follows the following structure:

```text
car-registrations-spain/
│
├── data/
│   ├── bronze/        # Data without processing
│   ├── silver/        # Data clean and structured
│   └── gold/          # Data ready for visualization and insights
│
├── src/
│   ├── __init__.py
│   ├── data_ingestion.py       # Download data and process it
│   ├── data_cleaning.py        # Process data from bronze to silver
│   ├── feature_engineering.py  # Add some interesting columns/features and load them into gold
│   └── utils/
│       ├── __init__.py
│       ├── logger.py           # Logs config
│       └── paths.py            # Centralized paths
│
├── notebooks/                  # Jupyter notebooks for EDA
│
├── main.py                     # Main
├── requirements.txt            # Requirements
├── README.md
└── .gitignore
```