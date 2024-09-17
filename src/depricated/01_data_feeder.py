import utilz.dataset_utils as dataset_utils
from utilz.misc import log, create_lock, resize_array
from utilz.kafka_utils import create_producer
from utilz.types import DICT_NAMESPACE

from threading import Thread, Semaphore
import time, math, random

########################################################################################
########################################################################################

config = DICT_NAMESPACE({

    # WHAT KAFKA TOPIC SHOULD WE PUSH DATA INTO?
    'output_topic': 'input_data',

    # YOLO PARAMS
    'yolo': {
        'dataset': 'yolo_mini.hdf5',
        'max_frames': -1,
        'max_vehicles': -1,
        'fps': 5,
        'repeat': 1,
    },

    # HOW MANY WORKER THREADS?
    'n_threads': 4,

    # SIMULATION PARAMS
    'max_mbs': 0.5,
    'duration_sec': 60, 
    'n_breakpoints': 10,
    'n_cycles': 1,
})

########################################################################################
########################################################################################

def run():
    
    # INSTANTIATE THREAD-SAFETY TOOLS
    thread_lock = create_lock()
    semaphore = Semaphore(1)

    # KEEP TRACK OF CREATED THREADS AND KAFKA PRODUCERS
    threads = []
    kafka_producers = []

    # CREATE KAFKA PRODUCERS FOR EACH THREAD
    for _ in range(config.n_threads):
        kafka_producer = create_producer()
        kafka_producers.append(kafka_producer)

    # ATTEMPT TO LOAD THE DATASET
    success, dataset = dataset_utils.load_dataset(config.yolo)
    dataset_length = len(dataset)

    # TERMINATE IF IT COULD NOT BE FOUND
    if not success:
        return

    ########################################################################################
    ########################################################################################

    def experiment_handler(lock):
        global action_cooldown

        # THE DEFAULT DAYNIGHT CYCLE WORKLOAD PERCENTAGES (01 => 23)
        default_cycle = [
            0.03,   0.06,   0.09,   0.12,   0.266,  0.412,
            0.558,  0.704,  0.85,   0.7625, 0.675,  0.587,
            0.5,    0.59,   0.68,   0.77,   0.86,   0.97,
            0.813,  0.656,  0.5,    0.343,  0.186,  0.03
        ] * config.n_cycles

        # SCALE THE ARRAY WHILE MAINTAINING RATIOS
        real_cycle = resize_array(
            default_cycle, 
            config.n_breakpoints
        )

        # COMPUTE THE EQUAL TIME SLIVER
        time_sliver = config.duration_sec / config.n_breakpoints

        # COMPUTE THE BYTESIZE OF THE AVERAGE DATASET ITEM
        avg_dataset_item_size = math.ceil(sum([len(x) for x in dataset]) / len(dataset))

        log('STARTING EXPERIMENT WITH:')
        log(f'N_BREAKPOINTS: ({config.n_breakpoints})')
        log(f'MAX MB/s:  ({config.max_mbps})')
        log(f'TOTAL DURATION: ({config.duration_sec})')
        log(f'SLIVER DURATION: ({time_sliver})')

        # STAY ACTIVE UNTIL LOCK IS MANUALLY KILLED
        while lock.is_active():

            # NO MORE BREAKPOINTS LEFT: KILL ALL THE THREADS
            if len(real_cycle) == 0:
                log('LAST EXPERIMENT BREAKPOINT RAN, TERMINATING..')
                lock.kill()
                break

            # OTHERWISE, FETCH THE NEXT INTERVAL
            mbs_interval = real_cycle.pop(0) * config.max_mbps
            log(f'SET NEW INPUT INTERVAL: ({time_sliver}s @ {mbs_interval} MB/s)')

            # COMPUTE THE NEW ACTION COOLDOWN
            events_per_second = (mbs_interval * 1000000) / avg_dataset_item_size
            new_cooldown = (1 / (events_per_second / config.n_threads))

            # SAFELY SET THE NEXT COOLDOWN
            with semaphore:
                action_cooldown = new_cooldown

            # ON THE FIRST RUN, BUSY WAIT TO SYNC THREADS
            while time.time() < experiment_start:
                pass
            
            # THEN SLEEP UNTIL THE NEXT BREAKPOINT
            time.sleep(time_sliver)

    ########################################################################################
    ########################################################################################

    # PRODUCER THREAD WORK LOOP
    def thread_work(nth_thread, lock):
        global action_cooldown

        # RANDOMLY PICK A STARTING INDEX FROM THE DATASET
        next_index = random.randrange(dataset_length)
        cooldown = None

        # BUSY WAIT FOR ABIT TO SYNC THREADS
        while time.time() < experiment_start:
            pass

        log(f'THREAD {nth_thread} HAS STARTED FROM INDEX {next_index}')

        # KEEP GOING UNTIL LOCK IS MANUALLY
        while lock.is_active():
            started = time.time()

            # SELECT NEXT BUFFER ITEM
            item = dataset[next_index]
            kafka_producers[nth_thread - 1].push_msg(config.output_topic, item.tobytes())
            
            # FETCH THE LATEST ACTION COOLDOWN
            with semaphore:
                cooldown = action_cooldown

            # COMPUTE THE ADJUSTED ACTION COOLDOWN, THEN TAKE A NAP
            ended = time.time()
            action_duration = ended - started
            adjusted_cooldown = max(cooldown - action_duration, 0)
            time.sleep(adjusted_cooldown)

            # INCREMENT ROLLING INDEX
            next_index = (next_index+1) % dataset_length

    ########################################################################################
    ########################################################################################

    try:
    
        # SHARED ACTION COOLDOWN FOR WORKER THREADS
        # THIS IS PROCEDUALLY FILLED BY THE HANDLER THREAD BELOW
        action_cooldown = None

        # TIMESTAMP FOR THREADS TO SYNC TO
        experiment_start = time.time() + 3

        # CREATE THE EXPERIMENT HANDLER
        log(f'CREATING EXPERIMENT HANDLER')
        handler_thread = Thread(target=experiment_handler, args=(thread_lock,))
        threads.append(handler_thread)
        handler_thread.start()

        log(f'CREATING PRODUCER THREAD POOL ({config.n_threads})')

        for nth in range(config.n_threads):
            thread = Thread(target=thread_work, args=(nth+1, thread_lock))
            threads.append(thread)
            thread.start()

        # WAIT FOR EVERY THREAD TO FINISH (MUST BE MANUALLY KILLED BY CANCELING LOCK)
        [[thread.join() for thread in threads]]

    # TERMINATE MAIN PROCESS AND KILL HELPER THREADS
    except KeyboardInterrupt:
        thread_lock.kill()
        log('WORKER & THREADS MANUALLY KILLED..', True)

run()