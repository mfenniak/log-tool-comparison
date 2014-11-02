import config
import requests
import gzip

def chunk_lines(lines_buffer_size):
    lines_buffer = []
    lines_buffer_size = 5000

    for url in config.LOG_FILES:
        req = requests.get(url, stream=True)
        stream = gzip.GzipFile(fileobj=req.raw, mode="r")
        while True:
            line = stream.readline()
            line = line.strip()
            if not line:
                break

            lines_buffer.append(line.decode("utf-8"))
            if len(lines_buffer) >= lines_buffer_size:
                yield lines_buffer
                lines_buffer = []

    if len(lines_buffer):
        yield lines_buffer

