import json
import datetime, time
from kafka import KafkaProducer



def read_json(file_path):

    views_list = []
    # Using readlines()
    file = open(file_path, 'r')
    Lines = file.readlines()
    
    count = 0
    # Strips the newline character
    for line in Lines:
        object = json.loads(line.strip())
        views_list.append(object)
 
    return views_list


def push_to_kafka(views_list):

    producer = KafkaProducer(bootstrap_servers=['kafka:29092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    iter = 0
    while iter < len(views_list):
        #Get the data
        object = views_list[iter]
        ct = datetime.datetime.now()
        object["timestamp"] = ct.timestamp()
        #PUSH THE OBJECT TO KAFKA
        # Send the serialized message data to a Kafka topic
        topic = 'views'
        producer.send(topic, object)
        print("PUSHED : \n", object)
        time.sleep(1)
        iter = iter + 1

    producer.close()

if __name__ == "__main__":
    views_list = read_json("product-views.json")
    push_to_kafka(views_list)



