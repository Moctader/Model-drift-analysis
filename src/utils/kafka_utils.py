from confluent_kafka import Consumer, Producer
from utils.misc import log
import json

# GOOD DOCS FOR CONSUMER API
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#consumer
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer

###################################################################################################
###################################################################################################

# KAFKA_SERVERS = '130.233.193.117:10001,130.233.193.117:10002,130.233.193.117:10003'
KAFKA_SERVERS = '193.166.180.240:11001'
VERBOSE = True

###################################################################################################
###################################################################################################

class create_producer:

    # ON LOAD, CREATE KAFKA PRODUCER
    def __init__(self):
        self.kafka_client = Producer({
            'bootstrap.servers': KAFKA_SERVERS,
        })

        self.check_connection()

    # MAKE SURE KAFKA CONNECTION IS OK
    def check_connection(self):
        try:
            metadata = self.kafka_client.list_topics(timeout=2)
            return True
        except:
            raise Exception(f'COULD NOT CONNECT WITH KAFKA SERVER ({KAFKA_SERVERS})')

    # ON CONSUMER CALLBACK, DO..
    def ack_callback(self, error, message):
        if error:
            print('ACK ERROR', error)
        # else:
            # if VERBOSE: log(f'MESSAGE PUSHED')

    # DATA SERIALIZER: JSON/DICT => BYTES
    def json_serializer(self, json_data: dict) -> list[bool, bytes|str]:
        try:
            json_bytes = json.dumps(json_data).encode('UTF-8')
            return True, json_bytes
        except:
            return False, 'SERIALIZATION ERROR'

    # PUSH MESSAGE TO A KAFK TOPIC
    def push_msg(self, topic_name: str, data_dict: dict):

        # TRY TO CONVERT THE DICT TO BYTES
        success, bytes_data = self.json_serializer(data_dict)

        # SKIP IF SERIALIZATION FAILS
        if not success:
            if VERBOSE: log(bytes_data)
            return

        # OTHERWISE, PUSH MESSAGE TO KAFKA TOPIC
        self.kafka_client.produce(
            topic_name, 
            value=bytes_data,
            on_delivery=self.ack_callback,
        )

        # ASYNCRONOUSLY AWAIT CONSUMER ACK BEFORE SENDING NEXT MSG
        self.kafka_client.poll(1)
        # self.kafka_client.flush()
    
    # Close the producer    
    def close(self):
        self.kafka_client.flush()
###################################################################################################
###################################################################################################

class create_consumer:

    # ON LOAD, CREATE KAFKA CONSUMER CLIENT
    def __init__(self, kafka_topic: str):

        # SET STATIC CONSUMPTION CONFIGS
        self.kafka_topic = kafka_topic

        # CREATE THE CONSUMER CLIENT
        self.kafka_client = Consumer({
            'bootstrap.servers': KAFKA_SERVERS,
            'group.id': kafka_topic + '.consumers',
            'enable.auto.commit': False,
            'on_commit': self.ack_callback,
            'auto.offset.reset': 'latest',
            # 'auto.offset.reset': 'earliest'
        })

        # MAKE SURE THE KAFKA CONNECTION IS OK
        self.check_connection()

        # FINALLY, SUBSCRIBE TO THE PROVIDED KAFKA TOPIC
        self.kafka_client.subscribe([kafka_topic], self.assigned, self.revoked, self.lost)

    # WHEN CLASS DIES, KILL THE KAFKA CLIENT
    def __del__(self):
        log('KAFKA CONSUMER CLOSED')
        self.kafka_client.close()

    # PARTITION ASSIGNMENT SUCCESS
    def assigned(self, consumer, partition_data):
        if VERBOSE:
            partitions = [p.partition for p in partition_data]
            log(f'CONSUMER ASSIGNED PARTITIONS: {partitions}')

    # PARTITION ASSIGNMENT REVOKED
    def revoked(self, consumer, partition_data):
        if VERBOSE:
            partitions = [p.partition for p in partition_data]
            log(f'CONSUMER PARTITION ASSIGNMENT REVOKED: {partitions}')

    # PARTITION ASSIGNMENT LOST
    def lost(self, consumer, partition_data):
        log(f'CONSUMER ASSIGNMENT LOST: {consumer} {partition_data}')

    # MAKE SURE KAFKA CONNECTION IS OK
    def check_connection(self):
        try:
            metadata = self.kafka_client.list_topics(timeout=2)
            return True
        except:
            raise Exception(f'COULD NOT CONNECT WITH KAFKA SERVER ({KAFKA_SERVERS})') 

    # AUTO CALLBACK WHEN CONSUMER COMMITS MESSAGE
    def ack_callback(self, error, partitions):
        if error:
            return print('ACK ERROR', error)

    # DATA DESERIALIZER: BYTES => JSON DICT
    def json_deserializer(self, raw_bytes: bytes) -> list[bool, dict|str]:
        try:
            json_dict = json.loads(raw_bytes.decode('UTF-8'))
            return True, json_dict
        except:
            return False, 'DESERIALIZATION ERROR'

    # START CONSUMING TOPIC EVENTS
    def poll_next(self, thread_lock, on_message, nth_thread='nth'):
        log(f'CONSUMER NOW POLLING (topic: {self.kafka_topic})')
        
        # KEEP POLLING WHILE LOCK IS ACTIVE
        while thread_lock.is_active():
            try:
                # POLL NEXT MESSAGE
                msg = self.kafka_client.poll(1)

                # NULL MESSAGE -- SKIP
                if msg is None:
                    continue

                # CATCH ERRORS
                if msg.error():
                    print('FAULTY EVENT RECEIVED', msg.error())
                    continue

                # COMMIT THE EVENT TO PREVENT OTHERS FROM TAKING IT
                self.kafka_client.commit(msg, asynchronous=True)

                # HANDLE THE EVENT VIA CALLBACK FUNC
                if VERBOSE: log(f'EVENT RECEIVED')

                # CONVERT BYTES TO JSON DICT
                success, result = self.json_deserializer(msg.value())

                # SKIP IF DESERIALIZATION FAILS
                if not success:
                    if VERBOSE: log(result)
                    return

                # OTHERWISE, RUN THE CALLBACK FUNC
                try:
                    on_message(result)
                except Exception as error:
                    print('CUSTOM CALLBACK ERROR:', error)

                if VERBOSE: log(f'EVENT HANDLED')

            # SILENTLY DEAL WITH OTHER ERRORS
            except Exception as error:
                print('CONSUMER ERROR:', error)
                continue
            
        # LOCK WAS KILLED, THEREFORE THREAD LOOP ENDS
        log(f'CONSUMER MANUALLY KILLED')

###################################################################################################
###################################################################################################

class create_consumer_producer:

    # ON LOAD, CREATE KAFKA CONSUMER CLIENT
    def __init__(self, input_topic: str):

        # SAVE TOPICS IN STATE
        self.input_topic = input_topic

        # CREATE BOTH TYPES OF KAFKA CLIENTS
        self.producer = create_producer()
        self.consumer = create_consumer(input_topic)

    def poll_next(self, thread_lock, handle_event):
        if VERBOSE: log(f'NOW POLLING WITH CONSUMER_PRODUCER (topic: {self.input_topic})')

        # KEEP POLLING WHILE THE THREAD LOCK IS ACTIVE
        while thread_lock.is_active():
            try:

                # POLL NEXT MESSAGE
                msg = self.consumer.kafka_client.poll(1)

                # NULL MESSAGE -- SKIP
                if msg is None:
                    continue

                # CATCH ERRORS
                if msg.error():
                    print('FAULTY EVENT RECEIVED', msg.error())
                    continue

                # COMMIT THE EVENT TO PREVENT OTHERS FROM TAKING IT
                self.consumer.kafka_client.commit(msg, asynchronous=True)

                # HANDLE THE EVENT VIA CALLBACK FUNC
                if VERBOSE: log(f'EVENT RECEIVED')

                # PUSH THE RAW BYTES THROUGH THE GIVEN DESERIALIZER
                success, result = self.consumer.json_deserializer(msg.value())

                # IF IT ENCOUNTERS ERRORS, SKIP
                if not success:
                    if VERBOSE: log(result)
                    continue

                # OTHERWISE, RUN THE CALLBACK FUNC
                try:
                    handle_event(result, self.producer.push_msg)
                except Exception as error:
                    print('CUSTOM CALLBACK ERROR:', error)

                if VERBOSE: log(f'EVENT HANDLED')

            # SILENTLY DEAL WITH OTHER ERRORS
            except Exception as error:
                print('PRODUCER_CONSUMER ERROR:', error)
                continue
            
        # LOCK WAS KILLED, THEREFORE THREAD LOOP ENDS
        log('WORKER MANUALLY KILLED')