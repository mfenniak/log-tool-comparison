import rethinkdb as r
import common
import time

reimport = True
parse = True
index = True

conn = r.connect('localhost', 28015)
conn.use("logs")

if reimport:
    if "logs" in r.db_list().run(conn):
        print("Dropping database 'logs'")
        r.db_drop("logs").run(conn)

    print("Creating database 'logs'")
    r.db_create("logs").run(conn)

    print("Creating table haproxy")
    r.table_create("haproxy").run(conn)

    import_start = time.time()

    total_count = 0
    for lines_buffer in common.chunk_lines(10000):
        total_count += len(lines_buffer)
        print("Inserting {0} more records, {1:,} total inserted".format(len(lines_buffer), total_count))
        r.table("haproxy").insert([{"_raw": l} for l in lines_buffer], durability="soft").run(conn)

    print("SYNC")
    r.table("haproxy").sync().run(conn)

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

if parse:
    regex = regex.replace("\n", "")
    print(repr(regex))

    parse_start = time.time()

    def regex_parse(rec):
        return { "_match": rec["_raw"].match(regex) }

    def parse_month(month):
        return \
            r.branch(month == "Jan", 1,
            r.branch(month == "Feb", 2,
            r.branch(month == "Mar", 3,
            r.branch(month == "Apr", 4,
            r.branch(month == "May", 5,
            r.branch(month == "Jun", 6,
            r.branch(month == "Jul", 7,
            r.branch(month == "Aug", 8,
            r.branch(month == "Sep", 9,
            r.branch(month == "Oct", 10,
            r.branch(month == "Nov", 11, 12)))))))))))

    def extract(rec):
        match = rec["_match"]
        return {
            #"log_month": match["groups"][0]["str"],
            #"log_day": match["groups"][1]["str"].coerce_to("number"),
            #"log_hour": match["groups"][2]["str"].coerce_to("number"),
            #"log_minute": match["groups"][3]["str"].coerce_to("number"),
            #"log_second": match["groups"][4]["str"].coerce_to("number"),
            "log_time": r.time(
                r.now().year(), # not part of the log format, weird
                parse_month(match["groups"][0]["str"]),
                match["groups"][1]["str"].coerce_to("number"),
                match["groups"][2]["str"].coerce_to("number"),
                match["groups"][3]["str"].coerce_to("number"),
                match["groups"][4]["str"].coerce_to("number"),
                "-06:00"
            ),
            "log_ip": match["groups"][5]["str"],
            "process_name": match["groups"][6]["str"],
            "pid": match["groups"][7]["str"].coerce_to("number"),
            "client_ip": match["groups"][8]["str"],
            "client_port": match["groups"][9]["str"].coerce_to("number"),
            #"accept_day": match["groups"][10]["str"].coerce_to("number"),
            #"accept_month": match["groups"][11]["str"],
            #"accept_year": match["groups"][12]["str"].coerce_to("number"),
            #"accept_hour": match["groups"][13]["str"].coerce_to("number"),
            #"accept_minute": match["groups"][14]["str"].coerce_to("number"),
            #"accept_second": match["groups"][15]["str"].coerce_to("number"),
            "accept_time": r.time(
                match["groups"][12]["str"].coerce_to("number"),
                parse_month(match["groups"][11]["str"]),
                match["groups"][10]["str"].coerce_to("number"),
                match["groups"][13]["str"].coerce_to("number"),
                match["groups"][14]["str"].coerce_to("number"),
                match["groups"][15]["str"].coerce_to("number"),
                "-06:00"
            ),
            "frontend_name": match["groups"][16]["str"],
            "backend_name": match["groups"][17]["str"],
            "server_name": match["groups"][18]["str"],
            "Tq": match["groups"][19]["str"].coerce_to("number"),
            "Tw": match["groups"][20]["str"].coerce_to("number"),
            "Tc": match["groups"][21]["str"].coerce_to("number"),
            "Tr": match["groups"][22]["str"].coerce_to("number"),
            "Tt": match["groups"][23]["str"].coerce_to("number"),
            "status_code": match["groups"][24]["str"].coerce_to("number"),
            "bytes_read": match["groups"][25]["str"].coerce_to("number"),
            "captured_request_cookie": match["groups"][26]["str"],
            "captured_response_cookie": match["groups"][27]["str"],
            "termination_state": match["groups"][28]["str"],
            "actconn": match["groups"][29]["str"].coerce_to("number"),
            "feconn": match["groups"][30]["str"].coerce_to("number"),
            "beconn": match["groups"][31]["str"].coerce_to("number"),
            "srv_conn": match["groups"][32]["str"].coerce_to("number"),
            "retries": match["groups"][33]["str"].coerce_to("number"),
            "srv_queue": match["groups"][34]["str"].coerce_to("number"),
            "backend_queue": match["groups"][35]["str"].coerce_to("number"),
            # 36 - full
            # 37 - null or http verb
            # 38 - null or request uri
            # 39 - null or http version
            "http_verb": r.branch(match["groups"][37] == None, match["groups"][36]["str"], match["groups"][37]["str"]),
            "request_uri": r.branch(match["groups"][38] == None, None, match["groups"][38]["str"]),
            "http_version": r.branch(match["groups"][39] == None, None, match["groups"][39]["str"]),
        }

    print("Beginning parse pass")
    retval = r.table("haproxy").update(regex_parse, durability="soft").run(conn)
    if retval["errors"] != 0:
        print("Errors during update")
        print(repr(retval))
        import sys; sys.exit(1)
    print("Parse pass complete")
    print("SYNC")
    r.table("haproxy").sync().run(conn)

    print("Beginning extract pass")
    retval = r.table("haproxy").update(extract, durability="soft").run(conn)
    if retval["errors"] != 0:
        print("Errors during update")
        print(repr(retval))
        import sys; sys.exit(1)
    print("Extract pass complete")
    print("SYNC")
    r.table("haproxy").sync().run(conn)

    print("Beginning delete pass")
    retval = r.table('haproxy').replace(r.row.without('_match'), durability="soft").run(conn)
    if retval["errors"] != 0:
        print("Errors during replace")
        print(repr(retval))
        import sys; sys.exit(1)
    print("Delete pass complete")
    print("SYNC")
    r.table("haproxy").sync().run(conn)

    parse_end = time.time()

    print("Parse completed in {0:.2f} seconds".format(parse_end - parse_start))

    #print("start")
    #for row in r.table("haproxy").filter(lambda rec: rec["_raw"].match(regex) == None).limit(1).run(conn):
    #    print(repr(row))
    #print("end")


if index:
    index_start = time.time()

    print("Creating accept_time index...")
    r.table("haproxy").index_create("accept_time").run(conn)
    print("Created; waiting for complete population...")
    r.table("haproxy").index_wait("accept_time").run(conn)

    index_end = time.time()

    print("Index creation completed in {0:.2f} seconds".format(index_end - index_start))
