from kafka import KafkaProducer

print ("settting up producer")
producer = KafkaProducer(bootstrap_servers='bae-es-kafka-bootstrap.mark-nr.svc:9092')

while True:
    print ("... sending kafka data")
    producer.send('sensor-readings', b'Hello, World!')
    producer.send('sensor-readings', key=b'message-two', value=b'This is Kafka-Python')
    sleep(30)

