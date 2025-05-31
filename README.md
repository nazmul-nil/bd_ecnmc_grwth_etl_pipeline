# Project Title: “Bangladesh Economic Growth ETL Pipeline”

## Project Structure
bash
```
bangladesh_econ_pipeline/
│
├── data/                   # raw and processed CSVs
├── scripts/                # Python scripts for each stage
│   ├── ingest_data.py
│   ├── transform_data.py
│   └── load_to_s3.py
│
├── prefect_flows/         # Prefect orchestration scripts
│   └── pipeline_flow.py
│
├── requirements.txt       # libraries used
└── README.md
```
