from kafka import KafkaProducer
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

    reading = {
        "sensor-processor": {
            "timestamp": datetime.now().isoformat(),
            "hostname": socket.gethostname()
    	},
        "sensor-reading": {
            "type": read_type,
            "value": str(msg.payload),
            "unit": read_unit
        }
    };
    print (json.dumps(reading))
    producer.send('sensor-readings', key=b'TD46EF', value=reading)




print ("setting up MQTT client connection to a broker")
# connect to MQTT
mqttc = mosquitto.Mosquitto("bae-poc")

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Uncomment to enable debug messages
#mqttc.on_log = on_log

# Connect
mqttc.connect("mqtt.rher-edge-poc.hopto.org", 1883)

# Start subscribe, with QoS level 0
mqttc.subscribe("rher-poc/edge/temperature", 0)
mqttc.subscribe("rher-poc/edge/current", 0)


print ("settting up producer")
producer = KafkaProducer(bootstrap_servers='bae-es-kafka-bootstrap.mark-nr.svc:9092')

# Continue the network loop, exit when an error occurs
print ("starting MQTT listen loop")
rc = 0
while rc == 0:
    rc = mqttc.loop()

print ("Exiting program. mqtt disconnected, rc=" + str(rc))
