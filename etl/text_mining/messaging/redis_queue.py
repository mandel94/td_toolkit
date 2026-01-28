"""
Redis Streams wrapper for event messaging
"""
import redis
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from etl.text_mining.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisQueue:
    """
    Simple wrapper around Redis Streams for event-driven communication
    
    Responsibilities:
    - Publish events to streams
    - Subscribe to streams
    - Handle message acknowledgment
    """
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        db: int = None
    ):
        self.host = host or config.REDIS_HOST
        self.port = port or config.REDIS_PORT
        self.db = db or config.REDIS_DB
        
        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=True
        )
        
        logger.info(f"Connected to Redis at {self.host}:{self.port}/{self.db}")
    
    def publish_event(self, stream_name: str, event_data: Dict[str, Any]) -> str:
        """
        Publish an event to a Redis stream
        
        Args:
            stream_name: Name of the Redis stream
            event_data: Event data as dictionary (will be JSON serialized)
            
        Returns:
            Message ID
        """
        # Add timestamp if not present
        if 'timestamp' not in event_data:
            event_data['timestamp'] = datetime.utcnow().isoformat()
        
        # Serialize to JSON
        serialized = {
            'data': json.dumps(event_data)
        }
        
        message_id = self.client.xadd(stream_name, serialized)
        logger.info(f"Published event to {stream_name}: {message_id}")
        
        return message_id
    
    def consume_events(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        block_ms: int = 1000,
        count: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Consume events from a Redis stream using consumer groups
        
        Args:
            stream_name: Name of the stream to consume from
            consumer_group: Consumer group name
            consumer_name: Consumer name within the group
            block_ms: Blocking timeout in milliseconds
            count: Max number of messages to read
            
        Returns:
            List of event data dictionaries
        """
        # Create consumer group if it doesn't exist
        try:
            self.client.xgroup_create(stream_name, consumer_group, id='0', mkstream=True)
            logger.info(f"Created consumer group {consumer_group} on {stream_name}")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        
        # Read from stream
        messages = self.client.xreadgroup(
            groupname=consumer_group,
            consumername=consumer_name,
            streams={stream_name: '>'},
            block=block_ms,
            count=count
        )
        
        events = []
        for stream, msg_list in messages:
            for msg_id, msg_data in msg_list:
                try:
                    event_data = json.loads(msg_data['data'])
                    event_data['_message_id'] = msg_id
                    event_data['_stream'] = stream
                    events.append(event_data)
                except Exception as e:
                    logger.error(f"Error parsing message {msg_id}: {e}")
        
        return events
    
    def acknowledge_message(self, stream_name: str, consumer_group: str, message_id: str):
        """Acknowledge message processing"""
        self.client.xack(stream_name, consumer_group, message_id)
        logger.debug(f"Acknowledged message {message_id} from {stream_name}")
    
    def get_stream_info(self, stream_name: str) -> Dict[str, Any]:
        """Get information about a stream"""
        try:
            return self.client.xinfo_stream(stream_name)
        except redis.ResponseError:
            return {}
    
    def close(self):
        """Close Redis connection"""
        self.client.close()
        logger.info("Closed Redis connection")


if __name__ == "__main__":
    # Test Redis connection
    queue = RedisQueue()
    
    # Test publish
    test_event = {
        "test": "data",
        "value": 123
    }
    msg_id = queue.publish_event("test_stream", test_event)
    print(f"Published: {msg_id}")
    
    # Test consume
    events = queue.consume_events("test_stream", "test_group", "test_consumer")
    print(f"Consumed: {events}")
    
    queue.close()
