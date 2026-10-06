"""Kirim event pesanan sintetis ke Kafka dengan laju tertentu.

python -m orders_stream.producer            # jalan terus
python -m orders_stream.producer --count 500
"""

from __future__ import annotations

import argparse
import logging
import random
import time
from collections import Counter

from confluent_kafka import Producer

from orders_stream.config import KafkaSettings, ProducerSettings
from orders_stream.generator import generate_event

log = logging.getLogger("producer")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=0, help="jumlah event (0 = tanpa batas)")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    kafka = KafkaSettings.from_env()
    cfg = ProducerSettings.from_env()
    producer = Producer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "enable.idempotence": True,  # tidak ada duplikat saat producer retry
            "linger.ms": 50,
        }
    )
    rng = random.Random(args.seed)
    stats: Counter[str] = Counter()
    interval = 1 / cfg.events_per_second
    sent = 0
    try:
        while not args.count or sent < args.count:
            event = generate_event(
                rng,
                late_ratio=cfg.late_ratio,
                very_late_ratio=cfg.very_late_ratio,
                invalid_ratio=cfg.invalid_ratio,
            )
            producer.produce(kafka.topic, key=event.key, value=event.value)
            producer.poll(0)
            stats[event.kind] += 1
            sent += 1
            if sent % 500 == 0:
                log.info("sent=%d %s", sent, dict(stats))
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        log.info("done sent=%d %s", sent, dict(stats))


if __name__ == "__main__":
    main()
