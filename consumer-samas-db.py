from kafka import KafkaConsumer
import json, psycopg2

conn = psycopg2.connect(
    dbname="samas_dstv_db",
    user="postgres",
    password="Mahlatsi#0310",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS social_activity (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    platform TEXT,
    activity_type TEXT,
    content TEXT,
    reaction_type TEXT,
    video_id TEXT,
    award_category TEXT,
    timestamp TIMESTAMP
)
''')
conn.commit()

consumer = KafkaConsumer(
    "samas-social",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Consuming SAMAs social activity...")
for msg in consumer:
    data = msg.value
    cursor.execute('''
        INSERT INTO social_activity (
            user_id, platform, activity_type, content, reaction_type, 
            video_id, award_category, timestamp
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ''', (
        data["user_id"], data["platform"], data["activity_type"], 
        data["content"], data["reaction_type"], data["video_id"], 
        data["award_category"], data["timestamp"]
    ))
    conn.commit()
    print("Inserted:", data)
