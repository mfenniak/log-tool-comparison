import rethinkdb as r
import common
import time

reimport = False

conn = r.connect('localhost', 28015)

if reimport:
    if "logs" in r.db_list().run(conn):
        print("Dropping database 'logs'")
        r.db_drop("logs").run(conn)

    print("Creating database 'logs'")
    r.db_create("logs").run(conn)
    conn.use("logs")

    print("Creating table haproxy")
    r.table_create("haproxy").run(conn)

    import_start = time.time()

    total_count = 0
    for lines_buffer in common.chunk_lines(5000):
        total_count += len(lines_buffer)
        print("Inserting {0} more records, {1:,} total inserted".format(len(lines_buffer), total_count))
        r.table("haproxy").insert([{"_raw": l} for l in lines_buffer], durability="soft").run(conn)

    print("SYNC")
    r.table("haproxy").sync().run(conn)

    import_end = time.time()
    print(
        "Imported {0:,} records in {1:,} seconds, {2:.2f} records / second".format(
            total_count, import_end - import_start, total_count / (import_end - import_start)))
