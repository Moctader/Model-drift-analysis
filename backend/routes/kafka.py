from fastapi import APIRouter, Response, status
from pydantic import BaseModel
import utils.kafka as kafka_utils

########################################################################################################
########################################################################################################

router = APIRouter()
kafka_admin = kafka_utils.create_admin_client()

########################################################################################################
########################################################################################################


@router.get('/kafka/')
async def overview(response: Response):
    try:
        response.status_code = status.HTTP_200_OK
        return kafka_admin.topics_overview()

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return str(error)

########################################################################################################
########################################################################################################


class Topic(BaseModel):
    name: str
    num_partitions: int


@router.post('/kafka/create')
async def create_topic(topic: Topic, response: Response):
    try:
        response.status_code = status.HTTP_201_CREATED
        kafka_admin.create_topic(topic.name, topic.num_partitions)

        return {
            'topic_name': topic.name,
            'num_partitions': topic.num_partitions
        }

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return f'ERROR: {str(error)}'

########################################################################################################
########################################################################################################


@router.get('/kafka/init')
async def init_experiment_topics(response: Response):

    topics = [
        'data_feeder',
        'data_preprocessing',
        'feature_engineering',
        'model_training',
        'model_usage',
        'post_processing',
        'model_tracker',
        'model_deployment',
        'model_production',
        'pre_processing',
        'drift_tracker'
    ]

    try:
        response.status_code = status.HTTP_201_CREATED

        for topic in topics:
            try:
                kafka_admin.create_topic(topic, 1)
            except:
                pass

        return topics

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return f'ERROR: {str(error)}'

########################################################################################################
########################################################################################################


@router.get('/kafka/{topic_name}')
async def get_topic_by_name(topic_name: str, response: Response):
    try:
        response.status_code = status.HTTP_200_OK

        # FETCH ALL TOPICS
        all_topics = kafka_admin.topics_overview()

        # RETURN ERROR IF TOPIC DOES NOT EXIST
        if topic_name not in all_topics:
            response.status_code = status.HTTP_404_NOT_FOUND
            return str('TOPIC DOES NOT EXIST')

        # OTHERWISE, RETURN TOPIC DETAILS
        return all_topics[topic_name]

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return str(error)
