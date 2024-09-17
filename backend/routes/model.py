from fastapi import APIRouter, Response, status
import os, time

router = APIRouter()

# AUXILLARY STUFF
root_dir = 'models/'
model_file_suffix = '.model'

# FETCH VALID MODEL FILES
def valid_models():
    raw_files = os.listdir(root_dir)
    return [x[:-len(model_file_suffix)] for x in raw_files if x.endswith(model_file_suffix)]

########################################################################################################
###############################################################################################

@router.get('/models/')
async def all_models(response: Response):
    response.status_code = status.HTTP_201_CREATED
    return valid_models()

########################################################################################################
########################################################################################################

@router.get('/models/{model_name}')
async def single_model(model_name: str, response: Response):

    # IF THE MODEL EXISTS IN DIR
    if model_name in valid_models():

        # ATTEMPT TO READ & PARSE THE FILESTATS
        try:
            file_stat = os.stat(f'{root_dir}{model_name}{model_file_suffix}')
            response.status_code = status.HTTP_200_OK

            return {
                'model_name': model_name,
                'num_bytes': file_stat.st_size,
                'unix_created': int(file_stat.st_ctime),
                # 'created': time.ctime(file_stat.st_ctime)
                'trained_on': {
                    'db_table': 'foo',
                    'start': '000000',
                    'end': '111111',
                }
            }

        # OTHERWISE, SOMETHING ELSE WENT WRONG
        except:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return {
                'error': 'Something went wrong'
            }
    
    # OTHERWISE, THE MODEL COULD NOT BE FOUND
    response.status_code = status.HTTP_404_NOT_FOUND
    return {
        'reason': 'No such model'
    }