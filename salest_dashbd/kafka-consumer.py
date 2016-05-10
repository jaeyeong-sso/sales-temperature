import threading, logging, time

from kafka import KafkaConsumer

class Consumer(threading.Thread):
    daemon = True

    consumer = KafkaConsumer(bootstrap_servers='salest-master-server:9092', auto_offset_reset='earliest')
    consumer.subscribe(['tr-events'])

    def run(self):
        for message in self.consumer:
            print (message)


def main():
    threads = [
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)

if __name__ == "__main__":
    main()
