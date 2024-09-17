# import h5py, json, os, csv

# # READ & PARSE HDF5 DATASET
# def load_dataset(yolo_args):
#     container = []

#     # STITCH TOGETHER FILEPATH
#     file_path = f'./datasets/{yolo_args.dataset}'

#     # TERMINATE IF THE DATASET CANT BE FOUND
#     if not os.path.exists(file_path):
#         raise Exception(f'DATASET NOT FOUND ({file_path})')

#     # EXTRACT DATASET COMPONENTS
#     dataset = h5py.File(file_path, 'r')
#     activity = dataset['is_enabled']
#     sensors = dataset['sensors']
#     metadata = json.loads(dataset['metadata'][()])
#     n_frames = metadata["n_frames"]

#     # COLLECT SENSOR COMPONENTS
#     for nth_iter in range(yolo_args.repeat):
#         sensor_names = list(dataset["sensors"].keys())
#         sensor_data_iters = {key: iter(sensors[key]) for key in sensor_names}

#         if yolo_args.max_frames > 0:
#             n_frames = min(n_frames, yolo_args.max_frames)

#         for frame in range(n_frames):
#             frame_data = {}

#             # FILL THE CURRENT FRAME
#             for sensor_name, data_iter in sensor_data_iters.items():
#                 active = activity[sensor_name][frame]

#                 # Sensor has data for this frame only if it is marked as active
#                 if active:
#                     sensor_data = next(data_iter)
#                     frame_data[sensor_name] = sensor_data

#                     container.append(sensor_data)

#     dataset.close()
#     return True, container

import csv

# LOAD CSV INTO DICT ARRAY
def load_csv(file_name):
    container = []
    file_path = f'datasets/{file_name}'

    with open(file_path, mode='r', newline='') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            container.append(row)

    return container