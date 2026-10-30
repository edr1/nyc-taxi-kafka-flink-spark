import os,json,time
from datetime import datetime,timezone
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer
root=Path('/app/results/lake');root.mkdir(parents=True,exist_ok=True)
c=Consumer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP','localhost:9092'),
            'group.id':'taxi-lake-v1','auto.offset.reset':'earliest','enable.auto.commit':False})
c.subscribe(['nyc.taxi.trips'])
rows=[];last=time.monotonic()
def flush():
    global rows,last
    if not rows:return
    folder=root / ('ingest_date='+datetime.now(timezone.utc).date().isoformat())
    folder.mkdir(parents=True,exist_ok=True)
    file=folder / ('part-'+str(time.time_ns())+'.parquet')
    # Payload completo preservado como JSON para evitar incompatibilidad de esquemas.
    pq.write_table(pa.table({'payload_json':[json.dumps(r,ensure_ascii=False) for r in rows]}),file)
    c.commit(asynchronous=False)
    print('Persistidos',len(rows),'eventos en',file,flush=True)
    rows=[];last=time.monotonic()
try:
    while True:
        msg=c.poll(1)
        if msg is not None and not msg.error():
            rows.append(json.loads(msg.value()))
        if len(rows)>=200 or (rows and time.monotonic()-last>=5):flush()
finally:
    flush();c.close()
