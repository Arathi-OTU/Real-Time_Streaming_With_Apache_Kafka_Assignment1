import faust
import joblib
import pandas as pd
import ssl
from faust.auth import SASLCredentials


# Kafka Configuration
BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY          = "Y3DZUBEHYQIXSJYV"
API_SECRET       = "cfltYT3wgqFjigcGawsqRD9wp225hfJgqHiQovtTL2v4kkSIhtGpIKWRSqI26aGw"

# Create Faust App
app = faust.App(
    'co_stream_processor',
    broker=f'kafka://{BOOTSTRAP_SERVER}',
    value_serializer='json',
    broker_credentials=SASLCredentials(
        username=API_KEY,
        password=API_SECRET,
        ssl_context=ssl.create_default_context()),
    #key_serializer='raw',
    #autodiscover=False,
    topic_allow_declare=False

    )
    

# KAFKA TOPICS
raw_data_topic = app.topic('raw-sensor-data')
prediction_topic = app.topic('co-predictions')


# LOAD TRAINED MODEL
trained_model = joblib.load('co_prediction_model.pkl')

# STREAM PROCESSOR
@app.agent(raw_data_topic)
async def predict_co(stream):
    async for event in stream:

      try:
        # Prepare input features
        input_df = [[
            event["PT08.S1(CO)"],
            event["NMHC(GT)"],
            event["C6H6(GT)"],
            event["PT08.S2(NMHC)"],
            event["NOx(GT)"],
            event["PT08.S3(NOx)"],
            event["NO2(GT)"],
            event["PT08.S4(NO2)"],
            event["PT08.S5(O3)"],
            event["T"],
            event["RH"],
            event["AH"],
        ]]

        # Predict CO concentration
        predicted_co = trained_model.predict(input_df)[0]

        # Create output message

        output = {
            "input": event,
            "Predicted CO": round(float(predicted_co), 2)
        }
        

        # Publish prediction
        await prediction_topic.send(value=output)

        print(f"Predicted CO:", output)

      except Exception as e:
        print(f"Error processing message: {e}")


