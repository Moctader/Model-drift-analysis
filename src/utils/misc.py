import datetime, os
import numpy as np
import json, time, math, logging
from datetime import datetime


def get_formatted_time():
    now = datetime.datetime.now()
    formatted_date = now.strftime("%A, %B %d, %Y %I:%M:%S %p")
    return formatted_date

# DESERIALIZE BYTES DATA -- INVERSE OF THE PRODUCERS SERIALIZER
def custom_deserializer(raw_bytes):
    return json.loads(raw_bytes.decode('UTF-8'))

# CUSTOM DATA => BYTES SERIALIZED -- INVERSE OF THE CONSUMERS DESERIALIZER
def custom_serializer(data):
    return json.dumps(data).encode('UTF-8')

# GET ACTION COOLDOWN BASED ON TIMESTAMP+SINE WAVE
def generate_cooldown(bonus=0):

    # STATIC VARS
    frequency = 0.5
    oscillation = 4
    buffer = 0.3

    # FETCH SIN-WAVE COOLDOWN
    return (0.2 * math.sin((time.time() + bonus) * frequency * math.pi / 60) + buffer) * oscillation

# TIMESTAMPED PRINT STATEMENT
def log(msg, with_break=False):
    now = datetime.now()
    timestamp = now.strftime("%H:%M:%S.%f")[:-3]  # Truncate microseconds to milliseconds
    if with_break:
        print(f'\n[{timestamp}]\t {msg}', flush=True)
    else:
        print(f'[{timestamp}]\t {msg}', flush=True)

    logging.info(f'[{timestamp}]\t {msg}')

# THREAD LOCK TO KILL HELPER THREADS
class create_lock:
    def __init__(self):
        self.lock = True
    
    def is_active(self):
        return self.lock
    
    def kill(self):
        self.lock = False

# RESIZE ARRAY WHILE ROUGHLY MAINTAINING VALUE RATIOS
def resize_array(original_array, new_length):
    original_length = len(original_array)
    indices = np.linspace(0, original_length - 1, new_length)
    stretched_array = np.interp(indices, np.arange(original_length), original_array)
    
    return stretched_array.tolist()