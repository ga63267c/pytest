from kafka import KafkaProducer

if __name__ == "__main__":
    while true:
        producer = KafkaProducer(bootstrap_servers='bae-es-kafka-bootstrap.mark-nr.svc:9092')
        producer.send('sensor-readings', b'Hello, World!')
        producer.send('sensor-readings', key=b'message-two', value=b'This is Kafka-Python')
        sleep(30)

