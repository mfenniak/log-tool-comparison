import common
import time
import psycopg2
import io
from uuid import UUID

connection_string = "dbname=log user=mfenniak"

def reimport():
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    print("Dropping & recreating table haproxy")
    cur.execute("DROP TABLE IF EXISTS haproxy")
    cur.execute("CREATE TABLE haproxy (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), _raw TEXT NOT NULL)")
    conn.commit()

    import_start = time.time()

    total_count = 0
    for lines_buffer in common.chunk_lines(10000):
        total_count += len(lines_buffer)
        print("Inserting {0} more records, {1:,} total inserted".format(len(lines_buffer), total_count))
        f = io.StringIO("\n".join(lines_buffer))
        cur.copy_from(f, "haproxy", columns=["_raw"])
        conn.commit()

    import_end = time.time()
    print(
        "Imported {0:,} records in {1:.2f} seconds, {2:.2f} records / second".format(
            total_count, import_end - import_start, total_count / (import_end - import_start)))


regex = r"""
^([A-Za-z]{3}) ([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})
 
([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})
 
([A-Za-z]+)\[([0-9]+)\]:
 
([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)
 
\[([0-9]{2})/([A-Za-z]{3})/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2}\.[0-9]{3})\]
 
([^ ]+)
 
([^/]+)/([^ ]+)
 
([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^ ]+)
 
([^ ]+)
 
([^ ]+)
 
([^ ]+)
 
([^ ]+)
 
([^ ]+)
 
([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^ ]+)
 
([^/]+)/([^ ]+)
 
.*
\"(([^ ]+) ([^ ]+) ?(HTTP/[0-9]\.[0-9])?|<BADREQ>)\"?
"""

def parse():
    from multiprocessing import Pool
    pool = Pool(6)

    parse_start = time.time()

    #conn = psycopg2.connect(connection_string)
    #cur = conn.cursor()
    #cur.execute("ALTER TABLE haproxy "
    #    "ADD COLUMN log_time TIMESTAMP WITH TIME ZONE, "
    #    "ADD COLUMN log_ip INET, "
    #    "ADD COLUMN process_name TEXT, "
    #    "ADD COLUMN pid INTEGER, "
    #    "ADD COLUMN client_ip INET, "
    #    "ADD COLUMN client_port INTEGER, "
    #    "ADD COLUMN accept_time TIMESTAMP WITH TIME ZONE, "
    #    "ADD COLUMN frontend_name TEXT, "
    #    "ADD COLUMN backend_name TEXT, "
    #    "ADD COLUMN server_name TEXT, "
    #    "ADD COLUMN Tq INTEGER, "
    #    "ADD COLUMN Tw INTEGER, "
    #    "ADD COLUMN Tc INTEGER, "
    #    "ADD COLUMN Tr INTEGER, "
    #    "ADD COLUMN Tt INTEGER, "
    #    "ADD COLUMN status_code INTEGER, "
    #    "ADD COLUMN bytes_read INTEGER, "
    #    "ADD COLUMN captured_request_cookie TEXT, "
    #    "ADD COLUMN captured_response_cookie TEXT, "
    #    "ADD COLUMN termination_state TEXT, "
    #    "ADD COLUMN actconn INTEGER, "
    #    "ADD COLUMN feconn INTEGER, "
    #    "ADD COLUMN beconn INTEGER, "
    #    "ADD COLUMN srv_conn INTEGER, "
    #    "ADD COLUMN retries INTEGER, "
    #    "ADD COLUMN srv_queue INTEGER, "
    #    "ADD COLUMN backend_queue INTEGER, "
    #    "ADD COLUMN http_verb TEXT, "
    #    "ADD COLUMN request_uri TEXT, "
    #    "ADD COLUMN http_version TEXT"
    #)
    #conn.commit()

    uuids = []
    for i in range(256):
        uuids.append(UUID(bytes=bytes([i]*16)))

    results = []
    for i in range(len(uuids) - 1):
        results.append(pool.apply_async(internal_parse, [uuids[i], uuids[i + 1]]))

    pool.close()

    while True:
        ready = len([r for r in results if r.ready()])
        failed = len([r for r in results if r.ready() and not r.successful()])
        print("Ready: {0} out of {1}, {2} failed".format(ready, len(results), failed))
        if ready == len(results):
            break
        time.sleep(5)

    for r in results:
        if r.ready() and not r.successful():
            r.get()

    pool.join()
    parse_end = time.time()
    print("Parse completed in {0:.2f} seconds".format(parse_end - parse_start))


def internal_parse(min_id, max_id):
    preped_regex = regex.replace("\n", "")

    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    cur.execute("""DO $$
DECLARE
    parsed TEXT[];
    r RECORD;
BEGIN
    FOR r IN SELECT id, _raw FROM haproxy WHERE id BETWEEN %s AND %s
    LOOP
        parsed := regexp_matches(r._raw, %s);
        UPDATE haproxy SET
            log_time = make_timestamptz(
                extract(year from now())::int, extract(month from to_date(parsed[1]::text, 'Mon'::text))::int, parsed[2]::int,
                parsed[3]::int, parsed[4]::int, parsed[5]::int, 'America/Edmonton'),
            log_ip = parsed[6]::inet,
            process_name = parsed[7],
            pid = parsed[8]::int,
            client_ip = parsed[9]::inet,
            client_port = parsed[10]::int,
            accept_time = make_timestamptz(
                parsed[13]::int, extract(month from to_date(parsed[12]::text, 'Mon'::text))::int, parsed[11]::int,
                parsed[14]::int, parsed[15]::int, parsed[16]::float, 'America/Edmonton'),
            frontend_name = parsed[17],
            backend_name = parsed[18],
            server_name = parsed[19],
            Tq = parsed[20]::int,
            Tw = parsed[21]::int,
            Tc = parsed[22]::int,
            Tr = parsed[23]::int,
            Tt = parsed[24]::int,
            status_code = parsed[25]::int,
            bytes_read = parsed[26]::int,
            captured_request_cookie = parsed[27],
            captured_response_cookie = parsed[28],
            termination_state = parsed[29],
            actconn = parsed[30]::int,
            feconn = parsed[31]::int,
            beconn = parsed[32]::int,
            srv_conn = parsed[33]::int,
            retries = parsed[34]::int,
            srv_queue = parsed[35]::int,
            backend_queue = parsed[36]::int,
            http_verb = CASE WHEN parsed[38] IS NULL THEN parsed[37] ELSE parsed[38] END,
            request_uri = parsed[39],
            http_version = parsed[40]
        WHERE id = r.id;
    END LOOP;
END
    $$""", [str(min_id), str(max_id), preped_regex])
    conn.commit()

    return None


def index():
    print("Creating indexes...")
    index_start = time.time()
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    cur.execute("CREATE INDEX http_verb ON haproxy (http_verb)")
    cur.execute("CREATE INDEX status_code ON haproxy (status_code)")
    cur.execute("ANALYZE haproxy")
    conn.commit()
    index_end = time.time()
    print("Index creation completed in {0:.2f} seconds".format(index_end - index_start))


if __name__ == "__main__":
    #reimport()
    #parse()
    index()
