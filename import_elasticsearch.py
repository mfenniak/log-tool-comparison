import common
import time
import re
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from multiprocessing import Process, JoinableQueue

regex = r"""
^(?P<log_datetime>[A-Za-z]{3} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})
 
(?P<log_ip>[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})
 
(?P<process_name>[A-Za-z]+)\[(?P<pid>[0-9]+)\]:
 
(?P<client_ip>[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):(?P<client_port>[0-9]+)
 
\[(?P<accept_datetime>[0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})\]
 
(?P<frontend_name>[^ ]+)
 
(?P<backend_name>[^/]+)/(?P<server_name>[^ ]+)
 
(?P<Tq>[^/]+)/(?P<Tw>[^/]+)/(?P<Tc>[^/]+)/(?P<Tr>[^/]+)/(?P<Tt>[^ ]+)
 
(?P<status_code>[^ ]+)
 
(?P<bytes_read>[^ ]+)
 
(?P<captured_request_cookie>[^ ]+)
 
(?P<captured_response_cookie>[^ ]+)
 
(?P<termination_state>[^ ]+)
 
(?P<actconn>[^/]+)/(?P<feconn>[^/]+)/(?P<beconn>[^/]+)/(?P<srv_conn>[^/]+)/(?P<retries>[^ ]+)
 
(?P<srv_queue>[^/]+)/(?P<backend_queue>[^ ]+)
 
.*
\"((?P<http_verb>[^ ]+) (?P<request_uri>[^ ]+) ?(?P<http_version>HTTP/[0-9]\.[0-9])?|<BADREQ>)\"?
"""

def stream(queue):
    preped_regex = re.compile(regex.replace("\n", ""))
    while True:
        lines_buffer = queue.get()
        if lines_buffer == None:
            queue.task_done()
            break
        for line in lines_buffer:
            source = {"_raw": line}
            m = preped_regex.match(line)
            if m != None:
                source.update(m.groupdict())
            yield { "_index": "logs", "_type": "haproxy", "_source": source }
        queue.task_done()

def internal_reimport(queue):
    es = Elasticsearch()
    for ok, result in streaming_bulk(es, stream(queue)):
        if not ok:
            print(repr(result))
            break

def reimport():
    es = Elasticsearch()

    if es.indices.exists(index="logs"):
        print("Deleting existing index")
        es.indices.delete(index="logs")
    print("Creating index")
    es.indices.create(index="logs")

    es.indices.put_mapping(index="logs", doc_type="haproxy", body={
        "dynamic": "strict",
        "properties": {
            "Tc":{"type":"integer"},
            "Tq":{"type":"integer"},
            "Tr":{"type":"integer"},
            "Tt":{"type":"integer"},
            "Tw":{"type":"integer"},
            "_raw":{"type":"string"},
            "accept_datetime": {
                "type": "date",
                "format": "dd/MMM/yyyy:HH:mm:ss.SSS",
            },
            "actconn":{"type":"integer"},
            "backend_name":{"type":"string"},
            "backend_queue":{"type":"integer"},
            "beconn":{"type":"integer"},
            "bytes_read":{"type":"integer"},
            "captured_request_cookie":{"type":"string"},
            "captured_response_cookie":{"type":"string"},
            "client_ip":{"type":"ip"},
            "client_port":{"type":"integer"},
            "feconn":{"type":"integer"},
            "frontend_name":{"type":"string"},
            "http_verb":{"type":"string"},
            "http_version":{"type":"string"},
            "log_datetime": {
                "type": "date",
                "format": "MMM dd HH:mm:ss",
            },
            "pid":{"type":"integer"},
            "process_name":{"type":"string"},
            "request_uri":{"type":"string"},
            "retries":{"type":"integer"},
            "server_name":{"type":"string"},
            "srv_conn":{"type":"integer"},
            "srv_queue":{"type":"integer"},
            "status_code":{"type":"integer"},
            "termination_state":{"type":"string"}
        }
    })

    import_start = time.time()

    queue = JoinableQueue(32)
    processes = [Process(target=internal_reimport, args=[queue]) for i in range(8)]
    for p in processes:
        p.start()

    total_count = 0
    for lines_buffer in common.chunk_lines(10000):
        queue.put(lines_buffer)
        total_count += len(lines_buffer)
        print("Queueing {0} more records, {1:,} total queued".format(len(lines_buffer), total_count))

    for p in processes:
        queue.put(None)

    print("All data is in-queue; waiting for processes to complete.")
    queue.join()
    for p in processes:
        p.join()

    import_end = time.time()
    print(
        "Imported & parsed {0:,} records in {1:.2f} seconds, {2:.2f} records / second".format(
            total_count, import_end - import_start, total_count / (import_end - import_start)))


if __name__ == "__main__":
    reimport()
