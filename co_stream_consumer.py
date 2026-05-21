import json
from kafka import KafkaConsumer

#  PASTE YOUR CONFLUENT CLOUD CREDENTIALS HERE
BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
API_KEY          = "Y3DZUBEHYQIXSJYV"
API_SECRET       = "cfltYT3wgqFjigcGawsqRD9wp225hfJgqHiQovtTL2v4kkSIhtGpIKWRSqI26aGw"

consumer = KafkaConsumer(
	"co-predictions",
	bootstrap_servers=BOOTSTRAP_SERVER,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=API_KEY,
    sasl_plain_password=API_SECRET,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Loading CO predictions .....")

for message in consumer:
    print("CO Prediction Received:", message.value)
