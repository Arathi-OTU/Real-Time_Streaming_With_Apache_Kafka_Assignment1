# Real-Time_Streaming_With_Apache_Kafka_Assignment1
A small real-time streaming application using Apache Kafka is built. The app will read rows from a public dataset, stream them through Kafka as live events, apply a machine learning prediction using the Streams API, and publish results to an output topic. The goal is to show a complete, running pipeline from raw data to live ML inference.


Dataset: Air Quality - UCI Machine Learning Repository (https://archive.ics.uci.edu/dataset/360/air+quality)
ML Model used: RandomForestRegressor
Evaluation Metrics After Model was Trained: 
Mean Absolute Error (MAE): 0.244
Root Mean Square Error (RMSE): 0.391
R2 Score: 0.927

Steps followed:
1. Downloaded the dataset and data preprocessing was done.
2. Trained ML model (RandomForestRegressor) on the processed data.
3. Once model was trained properly, saved it as .pkl file.
4. A new cluster was created on Confluence and API key is generated and got the Bootstrap server, API key, and API secret deatails and used the same in the producer, processor, and consumer files to connect.
5. Code for Producer (co_stream_producer.py) was written which read rows from the dataset and publishes each one as a JSON message to the raw-data topic at ~1 row/second.
6. Code for Streams Processor (co_stream_processor.py) was written which uses Faust (Python) to consume raw-data, run the pre-trained ML model on each record, and produce a new message to the predictions topic.
7. Code for Output Consumer (co_stream_consumer.py) was written which reads from predictions and prints each result to the console in a readable format as it arrives.
8. All the three python files are run on separate terminals start with Streams Processor with command: faust -A co_stream_processor worker -l info, then in another window start consumer with command: python co_stream_consumer.py, then in third window run produder with command: python co_stream_producer.
9. We can observe the predictions appear in real-time.
