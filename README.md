# NYC Taxi · Kafka + Flink + Spark (Docker Compose)

## Datos reales adjuntos
- `data/historical/yellow_tripdata_2024-{01,02,03}.parquet` (3 archivos, ~160 MB).
- `data/reference/taxi_zone_lookup.csv`: `LocationID,Borough,Zone,service_zone`.
- `data/weather/open-meteo-40.74N74.04W32m.csv`: exportación Open-Meteo con 4 líneas previas a los datos (metadatos, blanco, encabezado); columnas `time,temperature_2m (°C),precipitation (mm),rain (mm),snowfall (cm),wind_speed_10m (km/h)`.

## Inicio
Requiere Docker Engine + Docker Compose v2

```bash
docker compose build
docker compose up -d kafka init-kafka jobmanager taskmanager lake-writer spark
docker compose ps
# Consulta la estructura del topic nyc.taxi.trips en Kafka. Unico broker con 3 particiones
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic nyc.taxi.trips
```

Terminal A: Ejecutar Flink SQL (job de larga duración):
```bash
docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/usrlib/streaming.sql
```
Terminal B: Ejecutar el Productor para reproducir 5000 viajes (MAX_EVENTS) a 20 eventos/s (EVENTS_PER_SECOND) y publicarlos en Kafka
```bash
docker compose --profile ingest run --rm producer
```
Terminal C: observar resultados Flink y persistencia:
```bash
docker compose logs -f taskmanager
docker compose logs -f lake-writer
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic nyc.taxi.trips --from-beginning --max-messages 3
```
Ejecutar Spark después de tener eventos persistidos:
```bash
docker compose exec spark python spark/historical_analysis.py
docker compose exec spark python spark/stream_comparison.py
```
Flink UI: http://localhost:8081. Resultados: `results/batch/` y `results/comparison/`.

## Consola visual de Kafka

Kafka usa su propio protocolo en el puerto 9092; ese puerto no sirve una página web.
El servicio `kafka-ui` proporciona Kafbat UI en modo de solo lectura:

```bash
docker compose up -d kafka-ui
docker compose logs --tail=100 kafka-ui
```

Abrir http://localhost:8080 si Docker corre en la misma máquina, o
http://10.8.0.30:8080 para el servidor del laboratorio. Seleccionar el clúster
`nyc-taxi`, entrar en **Topics**, abrir `nyc.taxi.trips` y seleccionar **Messages**.
Para ver eventos existentes, elegir lectura desde el principio (earliest) y ejecutar
la búsqueda. Cada evento muestra la clave, el JSON, la partición y el offset.
La consola se conecta internamente a `kafka:29092`; el navegador solo necesita el
puerto HTTP 8080. Esta configuración no incluye autenticación; usar en la red de
confianza del laboratorio.

## Consolas visuales de Spark

```bash
docker compose build spark spark-history
docker compose up -d --no-deps spark spark-history
```

La recreación de `spark` interrumpe análisis activos; ejecutar estos comandos cuando
no haya análisis en curso. Ejecutar después los scripts Spark como se indica arriba.

- Aplicación activa: http://10.8.0.30:4040 (o http://localhost:4040).
- Segunda aplicación simultánea: http://10.8.0.30:4041.
- Historial: http://10.8.0.30:18080 (o http://localhost:18080).

La UI activa existe solamente mientras vive el SparkContext: `sleep infinity` no la
inicia y `spark.stop()` la cierra. En las pestañas Jobs, Stages, SQL y Executors se
pueden inspeccionar tareas, consultas y recursos. Los scripts usan `local[*]` dentro
del contenedor, sin un clúster Spark standalone separado.

El History Server lee los event logs persistidos en `results/spark-events/`.
Solo se podrán consultar ejecuciones que hayan generado estos logs después de
habilitar la configuración; no recupera ejecuciones antiguas sin event logs.
Para diagnosticar: `docker compose logs --tail=100 spark-history`.

## Preguntas
- P2 Flink: demanda por ventanas de un minuto de tiempo de procesamiento y agregación incremental por hora/día **históricos** del timestamp de pickup. Esta última es acumulativa y no es una ventana temporal de eventos.
- P1 Spark: viajes e ingresos por distrito/zona en enero-marzo de 2024.
- P7: Flink calcula top-3 global por zona en cada minuto; Spark calcula top-3 por distrito histórico. El replay Spark calcula top-3 por distrito y solapamiento histórico. **El ranking Flink no es aún por distrito**, pues la dimensión Borough no se distribuye en el topic; para comparabilidad exacta añadir broadcast dimension Flink o enriquecimiento en el productor.

## Precauciones
- Los eventos son una **simulación/replay**, no viajes de 2026.
- `MAX_EVENTS=5000` limita el muestreo a los primeros registros del archivo de enero; **no es muestra representativa de los tres meses**. Para comparar todo el trimestre usar muestreo estratificado o replay completo.
- `lake-writer` confirma offsets después de escribir Parquet: en fallas entre escritura y commit puede haber duplicados (at-least-once). Para análisis definitivo deduplicar por `event_id`.
- Los valores del clima provienen de un único punto de observación, no de cada zona de NYC; la comparación es exploratoria y no causal.
- No se ejecutó Docker en el entorno de elaboración; validar imágenes, SQL Flink y scripts en el host de destino.
