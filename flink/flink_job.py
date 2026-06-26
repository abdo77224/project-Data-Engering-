from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import json

env = StreamExecutionEnvironment.get_execution_environment()

kafka_source = FlinkKafkaConsumer(
    topics='my-topic',
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'test-group'
    }
)

stream = env.add_source(kafka_source)

def process(value):
    data = json.loads(value)
    return f"{data['username']} -> {data['message']}"

processed = stream.map(process)

processed.print()

env.execute("Kafka Flink Job")