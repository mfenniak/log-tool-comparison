import common
import time
import re
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from multiprocessing import Process, JoinableQueue

regex = r"""
^(?P<log_month>[A-Za-z]{3}) (?P<log_day>[0-9]{2}) (?P<log_hour>[0-9]{2}):(?P<log_minute>[0-9]{2}):(?P<log_second>[0-9]{2})
 
(?P<log_ip>[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})
 
(?P<process_name>[A-Za-z]+)\[(?P<pid>[0-9]+)\]:
 
(?P<client_ip>[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):(?P<client_port>[0-9]+)
 
\[(?P<accept_day>[0-9]{2})/(?P<accept_month>[A-Za-z]{3})/(?P<accept_year>[0-9]{4}):(?P<accept_hour>[0-9]{2}):(?P<accept_minute>[0-9]{2}):(?P<accept_second>[0-9]{2}\.[0-9]{3})\]
 
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
            yield { "_index": "logs", "_type": "document", "_source": source }
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
