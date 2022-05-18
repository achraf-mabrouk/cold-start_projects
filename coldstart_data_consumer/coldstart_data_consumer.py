from kafka import KafkaConsumer
import json
import argparse
import json, os

parser = argparse.ArgumentParser(description="YouTube videos data")
parser.add_argument("-i", "--input_topic", help="kafka topic of the YouTube channels data",required=True)
parser.add_argument("-b", "--bootstrap_server", help="Kafka server", required=True)
args = parser.parse_args()


def save_json(data, file_name):
    path = os.path.join('data/', file_name)
    with open(path + ".json", "w") as f:
        json.dump(data, f, indent=4)

def dump_jsonl(json_record, output_path, append=False):
    """
    Write list of objects to a JSON lines file.
    """
    mode = 'a+' if append else 'w'
    with open(output_path, mode, encoding='utf-8') as f:
        json_record = json.dumps(json_record, ensure_ascii=False)
        f.write(json_record + '\n')



def consume_messages(bootstrap_server, input_topic):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_server,
        group_id="yt-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        # consumer_timeout_ms=30000
    )
    consumer.subscribe(input_topic)

    return consumer

consumer = consume_messages(args.bootstrap_server, args.input_topic)
chunk_record_count=0
total_records = 0
chunk_num = 1
n = 100000
output_dir = "data/"
for message in consumer:
    # consumer.commit()
    output_path = os.path.join(output_dir, f"chunk{chunk_num}.jsonl")
    dump_jsonl(message.value, output_path, append=True)
    total_records += 1
    chunk_record_count += 1
    if chunk_record_count == n:
        chunk_num += 1
        chunk_record_count = 0
        
    print(f"chunk number {chunk_num} ==> total records : {total_records}")
    

consumer.close()
