from kafka import KafkaProducer
import json, time, random
from datetime import datetime

platforms = ["Twitter", "Instagram", "Facebook", "YouTube"]
activity_types = ["post", "comment", "like", "share", "reaction"]
reaction_types = ["like", "love", "wow", "haha", "sad", "angry"]
award_categories = [
    "Best Male Artist", "Best Female Artist", "Best Hip Hop", 
    "Best Album", "Best Newcomer", "Lifetime Achievement"
]
video_ids = ["perf001", "perf002", "perf003", "perf004"]

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_event():
    activity = random.choice(activity_types)
    return {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "platform": random.choice(platforms),
        "activity_type": activity,
        "content": f"Random comment about {random.choice(award_categories)}" if activity in ["post", "comment"] else None,
        "reaction_type": random.choice(reaction_types) if activity == "reaction" else None,
        "video_id": random.choice(video_ids) if activity in ["comment", "reaction", "like", "share"] else None,
        "award_category": random.choice(award_categories),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Streaming SAMAs social activity...")
    while True:
        event = generate_event()
        producer.send("samas-social", value=event)
        print("Sent:", event)
        time.sleep(0.5)
