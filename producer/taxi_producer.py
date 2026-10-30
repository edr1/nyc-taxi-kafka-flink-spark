import os, glob, json, time, uuid, math
from datetime import datetime, timezone
import pyarrow.parquet as pq
from confluent_kafka import Producer

bootstrap=os.getenv('KAFKA_BOOTSTRAP','localhost:9092')
rate=max(float(os.getenv('EVENTS_PER_SECOND','20')),0.1)
limit=int(os.getenv('MAX_EVENTS','5000'))
files=sorted(glob.glob('/app/data/historical/*.parquet'))
if not files:
    raise SystemExit('Faltan archivos .parquet en data/historical/')
producer=Producer({'bootstrap.servers':bootstrap,'client.id':'nyc-taxi-replay'})
required=['tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID','total_amount','trip_distance']
sent=0
for file in files:
    pf=pq.ParquetFile(file)
    for batch in pf.iter_batches(batch_size=1024):
        for row in batch.to_pylist():
            if any(row.get(k) is None for k in required): continue
            pickup=row['tpep_pickup_datetime']
            if not isinstance(pickup,datetime):continue
            event={k:(v.isoformat() if isinstance(v,datetime) else (None if isinstance(v,float) and not math.isfinite(v) else v)) for k,v in row.items()}
            event.update(event_id=str(uuid.uuid4()), ingested_at=datetime.now(timezone.utc).isoformat(),
                         source_file=os.path.basename(file), replay=True)
            payload=json.dumps(event,default=str,allow_nan=False).encode('utf-8')
            
            while True:
                try:
                    producer.produce('nyc.taxi.trips',key=str(row['PULocationID']).encode(),value=payload)
                    break
                except BufferError:
                    producer.poll(1)
            producer.poll(0)
            sent+=1
            if sent%500==0:print(f'Eventos publicados: {sent}',flush=True)
            if sent>=limit:
                producer.flush(30)
                print(f'Finalizado: {sent} eventos')
                raise SystemExit(0)
            time.sleep(1/rate)
producer.flush(30)
print(f'Finalizado: {sent} eventos')
