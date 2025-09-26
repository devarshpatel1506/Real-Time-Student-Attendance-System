import json
import sys
import os
from datetime import datetime
from pulsar import Client, ConsumerType
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import redis
import logging
from faker import Faker

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    PULSAR_HOST, PULSAR_TOPIC, REDIS_HOST, REDIS_PORT,
    CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, BLOOM_FILTER_KEY,
    BLOOM_FILTER_ERROR_RATE, BLOOM_FILTER_CAPACITY, HLL_KEY_PREFIX
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AttendanceProcessor:
    def __init__(self):
        # Initialize Pulsar client
        self.pulsar_client = Client(PULSAR_HOST)
        self.consumer = self.pulsar_client.subscribe(
            PULSAR_TOPIC,
            "attendance_processor",
            consumer_type=ConsumerType.Shared
        )

        # Initialize Redis client
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )

        # Initialize Cassandra client
        self.cassandra_cluster = Cluster(CASSANDRA_HOSTS)
        self.cassandra_session = self.cassandra_cluster.connect()
        
        # Create keyspace and tables if they don't exist
        self._setup_cassandra()
        
        # Initialize Faker for generating student IDs
        self.faker = Faker()

    def _setup_cassandra(self):
        """Set up Cassandra keyspace and tables."""
        # Create keyspace
        self.cassandra_session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        
        self.cassandra_session.set_keyspace(CASSANDRA_KEYSPACE)
        
        # Create attendance table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS attendance (
                student_id int,
                lecture_id text,
                timestamp timestamp,
                is_valid boolean,
                PRIMARY KEY ((lecture_id), timestamp, student_id)
            )
        """)

    def _setup_bloom_filter(self):
        """Set up Bloom Filter in Redis if not already exists."""
        try:
            # Check if Bloom Filter exists
            exists = self.redis_client.execute_command('BF.EXISTS', BLOOM_FILTER_KEY, 'test')
            logger.info("Bloom Filter already exists")
        except redis.exceptions.ResponseError:
            # Bloom Filter doesn't exist, create it
            try:
                self.redis_client.execute_command(
                    'BF.RESERVE',
                    BLOOM_FILTER_KEY,
                    BLOOM_FILTER_ERROR_RATE,
                    BLOOM_FILTER_CAPACITY
                )
                logger.info("Created new Bloom Filter")
            except redis.exceptions.ResponseError as e:
                if "already exists" not in str(e):
                    raise

    def process_attendance(self):
        """Process attendance messages from Pulsar."""
        logger.info("Starting attendance processing...")
        self._setup_bloom_filter()

        try:
            while True:
                msg = self.consumer.receive()
                try:
                    data = json.loads(msg.data().decode())
                    student_id = data['student_id']
                    lecture_id = data['lecture_id']
                    timestamp = datetime.fromisoformat(data['timestamp'])

                    # Check if student ID is valid using Bloom Filter
                    is_valid = bool(self.redis_client.execute_command(
                        'BF.EXISTS',
                        BLOOM_FILTER_KEY,
                        student_id
                    ))

                    # Store in Cassandra regardless of validity
                    query = """
                        INSERT INTO attendance (
                            student_id, lecture_id, timestamp, is_valid
                        ) VALUES (%s, %s, %s, %s)
                    """
                    self.cassandra_session.execute(
                        query,
                        (student_id, lecture_id, timestamp, is_valid)
                    )

                    # Only add to HyperLogLog if valid
                    if is_valid:
                        hll_key = f"{HLL_KEY_PREFIX}{lecture_id}"
                        self.redis_client.pfadd(hll_key, student_id)

                    logger.info(f"Processed attendance for student {student_id} (valid: {is_valid})")
                    self.consumer.acknowledge(msg)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.consumer.negative_acknowledge(msg)

        except KeyboardInterrupt:
            logger.info("Stopping attendance processing...")
        finally:
            self._cleanup()

    def _cleanup(self):
        """Clean up connections."""
        self.pulsar_client.close()
        self.redis_client.close()
        self.cassandra_cluster.shutdown()

    def get_attendance_stats(self, lecture_id):
        """Get attendance statistics for a lecture."""
        hll_key = f"{HLL_KEY_PREFIX}{lecture_id}"
        unique_attendees = self.redis_client.pfcount(hll_key)
        
        # Query Cassandra for detailed stats
        query = """
            SELECT student_id, timestamp
            FROM attendance
            WHERE lecture_id = %s
        """
        rows = self.cassandra_session.execute(query, (lecture_id,))
        
        return {
            'unique_attendees': unique_attendees,
            'attendance_records': list(rows)
        }

if __name__ == "__main__":
    processor = AttendanceProcessor()
    processor.process_attendance() 