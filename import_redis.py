import common
import time
import redis
import re
from uuid import uuid4

r = redis.StrictRedis()

def reimport():
    print("Flushing all redis contents")
    r.flushall()

    import_start = time.time()

    total_count = 0
    for lines_buffer in common.chunk_lines(10000):
        total_count += len(lines_buffer)
        print("Inserting {0} more records, {1:,} total inserted".format(len(lines_buffer), total_count))
        for line in lines_buffer:
            key = uuid4()
            r.hset("haproxy::entry::" + str(key), "_raw", line)

    import_end = time.time()
    print(
        "Imported {0:,} records in {1:.2f} seconds, {2:.2f} records / second".format(
            total_count, import_end - import_start, total_count / (import_end - import_start)))


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

def parse():
    from multiprocessing import Pool
    pool = Pool(6)

    parse_start = time.time()

    results = []
    buff = []
    for key in r.scan_iter(match="haproxy::entry::*"):
        buff.append(key)
        if len(buff) >= 10000:
            results.append(pool.apply_async(internal_parse, [buff]))
            buff = []
            if len(results) % 10 == 0:
                print("{0} chunks enqueued".format(len(results)))

    pool.close()

    while True:
        ready = len([res for res in results if res.ready()])
        failed = len([res for res in results if res.ready() and not res.successful()])
        print("Ready: {0} out of {1}, {2} failed".format(ready, len(results), failed))
        if ready == len(results):
            break
        time.sleep(5)

    for res in results:
        if res.ready() and not res.successful():
            res.get()

    pool.join()
    parse_end = time.time()
    print("Parse completed in {0:.2f} seconds".format(parse_end - parse_start))


def internal_parse(buff):
    preped_regex = re.compile(regex.replace("\n", ""))
    for key in buff:
        raw = r.hget(key, "_raw").decode("utf-8")
        m = preped_regex.match(raw)
        r.hmset(key, m.groupdict())
    return None


#def index():
#    print("Creating indexes...")
#    index_start = time.time()
#    conn = psycopg2.connect(connection_string)
#    cur = conn.cursor()
#    cur.execute("CREATE INDEX http_verb ON haproxy (http_verb)")
#    cur.execute("CREATE INDEX status_code ON haproxy (status_code)")
#    cur.execute("ANALYZE haproxy")
#    conn.commit()
#    index_end = time.time()
#    print("Index creation completed in {0:.2f} seconds".format(index_end - index_start))


if __name__ == "__main__":
    #reimport()
    parse()
    #index()
