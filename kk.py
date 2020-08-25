from kafka import KafkaProducer
from kafka.errors import KafkaError
from time import sleep
from datetime import datetime
import paho.mqtt.client as mosquitto
import socket
import json


# Define event callbacks
def on_connect(self, self_data, self_flags, rc,):
    print("MQTT connected, rc: " + str(rc))

def on_subscribe(mid):
    print("on_subscribe: " + str(mid) )

def on_message(self, self_data, msg):
    #print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

    if msg.topic == "rher-poc/edge/current":
        read_unit = "amps"
        read_type = "electrical current"
    elif msg.topic == "rher-poc/edge/temperature":
        read_unit = "celsius"
        read_type = "temperature"
    else:
        read_unit = "?"
        read_type = "?"

    #print ("creating mission data")
    reading = {
        "sensor-processor": {
            "timestamp": datetime.now().isoformat(),
            "hostname": socket.gethostname()
    	},
        "sensor-reading": {
            "type": read_type,
            "value": msg.payload.decode('utf-8'),
            "unit": read_unit
        }
    }

    reading_bytes = json.dumps(reading).encode('utf-8') 
    producer.send('sensor-readings', key=b'TD46EF', value=reading_bytes)
    print (str(reading_bytes))
    #print ("sent to Kafka producer")


print ("setting up MQTT client connection to a broker")
# connect to MQTT
mqttc = mosquitto.Mosquitto("bae-poc")

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Connect
mqttc.connect("mqtt.rher-edge-poc.hopto.org", 1883)

# Start subscribe, with QoS level 0
mqttc.subscribe("rher-poc/edge/temperature", 0)
mqttc.subscribe("rher-poc/edge/current", 0)


print ("settting up Kafka producer")
producer = KafkaProducer(bootstrap_servers=['bae-es-kafka-bootstrap.mark-nr.svc:9092'])

# Continue the network loop, exit when an error occurs
print ("starting MQTT listen loop")
rc = 0
while rc == 0:
    rc = mqttc.loop()

print ("Exiting program. mqtt disconnected, rc=" + str(rc))
