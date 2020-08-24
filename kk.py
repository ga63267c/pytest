from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='bae-es-kafka-bootstrap.mark-nr.svc:9092')
producer.send('temperature', b'Hello, World!')
producer.send('temperature', key=b'message-two', value=b'This is Kafka-Python')

