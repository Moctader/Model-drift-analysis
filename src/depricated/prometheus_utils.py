from prometheus_client import start_http_server, CollectorRegistry
from prometheus_client.core import GaugeMetricFamily
from utilz.misc import log

VERBOSE = True

class create_prometheus_endpoint:
    def __init__(self, server_port=8282):
        self.metrics_repository = {}

        # CREATE A METRICS REGISTRY & BOOT UP THE SCRAPING ENDPOINT
        registry = CollectorRegistry()
        registry.register(self)
        start_http_server(server_port, registry=registry)

        log(f'PROMETHEUS SCRAPING ENDPOINT STARTED (port: {server_port})')  

    ########################################################################################
    ########################################################################################

    # REGISTER A NEW MODEL
    def add_source(self, source_name: str, source_metrics: list):
        prometheus_objects = {}

        # OTHERWISE, LOOP IN THE DETAILS
        for metric_name in source_metrics:
            prometheus_objects[metric_name] = GaugeMetricFamily(
                metric_name,
                f'{metric_name}_description',
                labels=['source', 'foo']
            )

        # PUSH THE MODEL DETAILS IN
        self.metrics_repository[source_name] = prometheus_objects
        if VERBOSE: log(f'NEW SOURCE ({source_name}) REGISTERED')

    ########################################################################################
    ########################################################################################

    # ADD NEW METRIC READING FOR A MODEL
    def push_metric(self, kafka_input: dict):

        # CREATE SHORTHANDS FOR CLARITY
        source_name = kafka_input['source']
        output = kafka_input['output']

        # REGISTER ANY NEW SOURCES
        if source_name not in self.metrics_repository:
            self.add_source(source_name, output.keys())

        # FINALLY, PUSH THE METRIC VALUES
        for metric_name, metric_value in output.items():
            self.metrics_repository[source_name][metric_name].add_metric([source_name], metric_value)

    ########################################################################################
    ########################################################################################

    def collect(self):
        # self.update_metrics()  # Update the metrics
        for model_name, metrics in self.metrics_repository.items():
            for metric in metrics.values():
                yield metric