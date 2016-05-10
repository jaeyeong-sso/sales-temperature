import threading, logging, time

from kafka import KafkaProducer

class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='salest-master-server:9092')

        while True:
            producer.send('my-topic', "test message from kafka producer")
            time.sleep(1)

def main():
    threads = [
        Producer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)

if __name__ == "__main__":
    main()
