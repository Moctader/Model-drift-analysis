from utils.kafka_utils import create_consumer_producer, create_consumer
from utils.misc import log, create_lock
from typing import Callable
from utils.types import KAFKA_DICT, KAFKA_PUSH_FUNC

########################################################################################
########################################################################################

def start_consumer_producer(input_topic: str, handle_event: Callable[[KAFKA_DICT, KAFKA_PUSH_FUNC], None]):

    # CREATE THE KAKFA CLIENT & CONTROL LOCK
    kafka_client = create_consumer_producer(input_topic)
    thread_lock = create_lock()

    # FINALLY, START CONSUMING EVENTS
    try:
        kafka_client.poll_next(thread_lock, handle_event)

    # TERMINATE MAIN PROCESS AND KILL HELPER THREADS
    except KeyboardInterrupt:
        thread_lock.kill()
        log('CONSUMER_PRODUCER MANUALLY KILLED..', True)

########################################################################################
########################################################################################

def start_consumer(input_topic: str, handle_event: Callable[[KAFKA_DICT], None]):

    # CREATE THE KAKFA CLIENT & CONTROL LOCK
    kafka_client = create_consumer(input_topic)
    thread_lock = create_lock()
    
    # FINALLY, START CONSUMING EVENTS
    try:
        kafka_client.poll_next(thread_lock, handle_event)

    # TERMINATE MAIN PROCESS AND KILL HELPER THREADS
    except KeyboardInterrupt:
        thread_lock.kill()
        log('CONSUMER MANUALLY KILLED..', True)

########################################################################################
########################################################################################