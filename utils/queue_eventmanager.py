from typing import Callable, List, Dict
import asyncio
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from contextlib import contextmanager
from utils.logger import setup_logging

logger = setup_logging(__name__)

class EventBus:
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = {}
    
    def subscribe(self, callback: Callable, topic_name: str) -> int:
        """토픽별 콜백 함수(async func) 등록"""
        if topic_name not in self.subscribers:
            self.subscribers[topic_name] = []
        
        self.subscribers[topic_name].append(callback)
        return len(self.subscribers[topic_name]) - 1
    
    def unsubscribe(self, subscriber_id: int, topic_name: str) -> bool:
        """토픽별 구독 취소"""
        if topic_name not in self.subscribers:
            return False
            
        if subscriber_id < 0 or subscriber_id >= len(self.subscribers[topic_name]):
            return False
        
        self.subscribers[topic_name].pop(subscriber_id)
        return True

    def get_subscribers(self, topic_name: str) -> List[Callable]:
        """토픽의 구독자 목록 반환"""
        return self.subscribers.get(topic_name, [])

class MessageQueue:
    def __init__(self, kafka_brokers: str, topics: List[str], max_retries: int = 3):
        logger.info(f"Creating consumer for topics: {topics}")
        self.max_retries = max_retries
        self.bootstrap_servers = kafka_brokers.split(",")
        self.topics = topics
        self.client = self._create_consumer()
    
    def _create_consumer(self):
        for attempt in range(self.max_retries):
            try:
                return KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='alert-group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    key_deserializer=lambda x: x.decode('utf-8') if x else None,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    request_timeout_ms=35000,
                    connections_max_idle_ms=180000,
                    max_poll_interval_ms=300000,
                    api_version_auto_timeout_ms=60000,
                    security_protocol='PLAINTEXT',
                    fetch_max_wait_ms=500,
                    fetch_min_bytes=1,
                    fetch_max_bytes=52428800,
                    metadata_max_age_ms=300000,
                    reconnect_backoff_ms=5000,
                    reconnect_backoff_max_ms=10000
                )
            except NoBrokersAvailable:
                if attempt < self.max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    time.sleep(wait_time)
                else:
                    raise

    @contextmanager
    def get_consumer(self):
        """컨슈머 컨텍스트 매니저"""
        try:
            yield self.client
        finally:
            self.close()
    
    def close(self):
        """컨슈머 종료"""
        logger.info(f"Closing consumer for topics: {self.topics}")
        try:
            if self.client:
                self.client.close()
        except Exception as e:
            logger.error(f"Error while closing consumer: {str(e)}")
        
class Consumer(MessageQueue):
    def __init__(self, kafka_broker: str, topics: List[str], event_bus: EventBus, max_retries=3):
        super().__init__(kafka_broker, topics, max_retries)
        self.event_bus = event_bus
        self.is_running = False

    async def start(self):
        async def run_consumer():
            logger.info(f"Starting consumer for topics: {self.topics}")
            while True:
                try:
                    message = await asyncio.to_thread(next, self.client)
                    if message:
                        key = message.key
                        value = message.value
                        topic = message.topic
                        
                        logger.debug(f"Message topic: {topic}, partition: {message.partition}, offset: {message.offset}")
                        if isinstance(value, str):
                            value = json.loads(value)
                        
                        subscribers = self.event_bus.get_subscribers(topic)
                        logger.info(f"callbacks for topic {topic}({len(subscribers)}): {', '.join([str(callback) for callback in subscribers])}")
                        
                        result = await asyncio.gather(
                            *[callback(key, value) for callback in subscribers]
                        )
                        logger.debug(f"Gather completed for topic {topic}: {result}")
                except Exception as e:
                    import traceback
                    logger.error(traceback.format_exc())
                    logger.error(f"Error reading message: {e}")
                    await asyncio.sleep(1)
        
        if not hasattr(self, '_consumer_task'):
            self._consumer_task = asyncio.create_task(run_consumer())
    
    async def stop(self):
        if hasattr(self, '_consumer_task'):
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        self.close()

class EventManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(EventManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, kafka_broker: str, topics: List[str], max_retries=3):
        if not hasattr(self, 'initialized'):
            self.event_bus = EventBus()
            self.consumer = Consumer(kafka_broker, topics, self.event_bus, max_retries)
            self.initialized = True

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()

    def subscribe(self, callback: Callable, topic_name: str) -> int:
        return self.event_bus.subscribe(callback, topic_name)

    def unsubscribe(self, subscriber_id: int, topic_name: str) -> bool:
        return self.event_bus.unsubscribe(subscriber_id, topic_name)