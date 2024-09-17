import io, torch, os
from PIL import Image
from numpy import asarray

from utilz.misc import log

# LOAD A MODEL FROM FILESYSTEM
def load_model(model_name: str):

    # STITCH TOGETHER THE FILEPATH
    model_path = f'./models/{model_name}.pt'

    # TERMINATE IF THE DATASET CANT BE FOUND
    if not os.path.exists(model_path):
        raise Exception(f'MODEL NOT FOUND ({model_path})')

    # LOAD THE INTENDED YOLO MODEL OUTSIDE THE FUNC -- FOR REPEATED USE
    yolo_model = torch.hub.load(
        'ultralytics/yolov5', 
        'custom', 
        path=model_path, 
        trust_repo=True, 
        force_reload=True
    )

    # PRINT OUT WHAT HARDWARE DEVICE THE MODEL WAS LOADED ON -- CPU/GPU
    hardware_device = yolo_model.parameters().__next__().device
    log(f'LOADED MODEL ({model_name}) ON HARDWARE DEVICE ({hardware_device})')

    return yolo_model

# CONVERT BYTES TO YOLO DIGESTABLE IMAGE
def bytes_to_img(bytes_data):
    img = Image.open(io.BytesIO(bytes_data))
    return asarray(img)