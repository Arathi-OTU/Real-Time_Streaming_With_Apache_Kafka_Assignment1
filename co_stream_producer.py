import pandas as pd
import time
import json
from kafka import KafkaProducer

# Load dataset
file_path = "AirQualityUCI.xlsx"

# Read Excel file
dataset = pd.read_excel(file_path)

#  PASTE YOUR CONFLUENT CLOUD CREDENTIALS HERE
BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY          = "Y3DZUBEHYQIXSJYV"
API_SECRET       = "cfltYT3wgqFjigcGawsqRD9wp225hfJgqHiQovtTL2v4kkSIhtGpIKWRSqI26aGw"


# Shared config used by all producers and consumers
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=API_KEY,
    sasl_plain_password=API_SECRET,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Config ready — connecting to:", BOOTSTRAP_SERVER)

# Stream Data Row by row
print("Producing data...")
for index, row in dataset.iterrows():

  # Convert row to dictionary
    message={
      "PT08.S1(CO)": float(row["PT08.S1(CO)"]),
      "NMHC(GT)": float(row["NMHC(GT)"]),
      "C6H6(GT)": float(row["C6H6(GT)"]),
      "PT08.S2(NMHC)": float(row["PT08.S2(NMHC)"]),
      "NOx(GT)": float(row["NOx(GT)"]),
      "PT08.S3(NOx)": float(row["PT08.S3(NOx)"]),
      "NO2(GT)": float(row["NO2(GT)"]),
      "PT08.S4(NO2)": float(row["PT08.S4(NO2)"]),
      "PT08.S5(O3)": float(row["PT08.S5(O3)"]),
      "T": float(row["T"]),
      "RH": float(row["RH"]),
      "AH": float(row["AH"]),
    }
    producer.send("raw-sensor-data", value=message)    
    print("Message sent:", message)

    # Wait ~1 second to simulate real-time streaming
    time.sleep(1)

  

print("Finished Streaming Data")