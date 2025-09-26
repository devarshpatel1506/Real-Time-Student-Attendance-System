import time
import random
import json
from datetime import datetime, timedelta
from faker import Faker
from pulsar import Client
import sys
import os
import logging
import redis

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    PULSAR_HOST, PULSAR_TOPIC, REDIS_HOST, REDIS_PORT,
    BLOOM_FILTER_KEY
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def generate_random_timestamp(base_date=None):
    """Generate a random timestamp for a lecture day."""
    if base_date is None:
        # Generate timestamps for the past week
        days_ago = random.randint(0, 7)
        base_date = datetime.now() - timedelta(days=days_ago)
    
    # Class hours between 8 AM and 5 PM
    hour = random.randint(8, 17)
    minute = random.randint(0, 59)
    
    return base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)

def generate_student_data():
    """Generate simulated student attendance data."""
    client = Client(PULSAR_HOST)
    producer = client.create_producer(PULSAR_TOPIC)
    faker = Faker()
    
    # Initialize Redis client to check valid student IDs
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )
    
    # Pre-generate a set of valid student IDs
    logger.info("Generating valid student IDs...")
    valid_student_ids = set(faker.unique.random_int(min=10000, max=99999) 
                          for _ in range(1000))
    
    # Add valid IDs to Redis Bloom Filter
    try:
        for student_id in valid_student_ids:
            redis_client.execute_command(
                'BF.ADD',
                BLOOM_FILTER_KEY,
                student_id
            )
        logger.info(f"Added {len(valid_student_ids)} valid student IDs to Bloom Filter")
    except redis.exceptions.ResponseError as e:
        if "already exists" not in str(e):
            logger.error(f"Error adding student IDs to Bloom Filter: {e}")
    
    # Generate attendance for the past week
    past_week_dates = [
        datetime.now() - timedelta(days=i)
        for i in range(7)
    ]
    
    try:
        message_count = 0
        invalid_attempts = 0
        
        # Generate some invalid student IDs
        invalid_student_ids = set(faker.unique.random_int(min=100000, max=999999) 
                                for _ in range(50))  # Generate 50 invalid IDs
        
        # Each student has different attendance patterns
        for student_id in valid_student_ids:
            # Randomly decide if this student is generally punctual or often late
            is_punctual = random.random() > 0.2  # 80% students are generally punctual
            
            # Randomly decide how many days this student attends (between 3-7 days)
            attendance_days = random.sample(past_week_dates, random.randint(3, 7))
            
            for day in attendance_days:
                # Generate entry time based on student's punctuality
                if is_punctual:
                    entry_hour = random.randint(8, 9)  # Arrives between 8-9 AM
                else:
                    entry_hour = random.randint(9, 11)  # Arrives between 9-11 AM
                
                entry_time = day.replace(
                    hour=entry_hour,
                    minute=random.randint(0, 59),
                    second=0,
                    microsecond=0
                )
                
                # Generate exit time (3-4 hours after entry)
                exit_time = entry_time + timedelta(
                    hours=random.randint(3, 4),
                    minutes=random.randint(0, 59)
                )
                
                # Create entry record
                entry_data = {
                    'student_id': student_id,
                    'timestamp': entry_time.isoformat(),
                    'lecture_id': f"LECTURE_{entry_time.strftime('%Y%m%d')}",
                    'is_valid': True,
                    'event_type': 'entry'
                }
                
                # Send entry message to Pulsar
                message = json.dumps(entry_data).encode('utf-8')
                producer.send(message)
                message_count += 1
                
                # Create exit record
                exit_data = {
                    'student_id': student_id,
                    'timestamp': exit_time.isoformat(),
                    'lecture_id': f"LECTURE_{exit_time.strftime('%Y%m%d')}",
                    'is_valid': True,
                    'event_type': 'exit'
                }
                
                # Send exit message to Pulsar
                message = json.dumps(exit_data).encode('utf-8')
                producer.send(message)
                message_count += 1
                
                # Generate invalid attendance attempts (15% chance)
                if random.random() < 0.15:
                    invalid_id = random.choice(list(invalid_student_ids))
                    invalid_data = {
                        'student_id': invalid_id,
                        'timestamp': entry_time.isoformat(),
                        'lecture_id': f"LECTURE_{entry_time.strftime('%Y%m%d')}",
                        'is_valid': False,
                        'event_type': 'entry'
                    }
                    message = json.dumps(invalid_data).encode('utf-8')
                    producer.send(message)
                    message_count += 1
                    invalid_attempts += 1
                    logger.info(f"Generated invalid attendance attempt for ID: {invalid_id}")
                
                if message_count % 100 == 0:  # Log every 100 messages
                    logger.info(f"Generated {message_count} attendance records ({invalid_attempts} invalid attempts)")
                
                # Small delay between records
                time.sleep(random.uniform(0.1, 0.5))
        
        # Generate some standalone invalid attempts
        for _ in range(20):  # Generate 20 additional invalid attempts
            invalid_id = random.choice(list(invalid_student_ids))
            random_day = random.choice(past_week_dates)
            random_time = random_day.replace(
                hour=random.randint(8, 17),
                minute=random.randint(0, 59),
                second=0,
                microsecond=0
            )
            
            invalid_data = {
                'student_id': invalid_id,
                'timestamp': random_time.isoformat(),
                'lecture_id': f"LECTURE_{random_time.strftime('%Y%m%d')}",
                'is_valid': False,
                'event_type': 'entry'
            }
            message = json.dumps(invalid_data).encode('utf-8')
            producer.send(message)
            message_count += 1
            invalid_attempts += 1
            logger.info(f"Generated standalone invalid attendance attempt for ID: {invalid_id}")
            
            time.sleep(random.uniform(0.1, 0.5))
            
    except KeyboardInterrupt:
        logger.info(f"\nStopping data generation... Total messages sent: {message_count} ({invalid_attempts} invalid attempts)")
    except Exception as e:
        logger.error(f"Error generating data: {e}")
    finally:
        client.close()
        redis_client.close()

if __name__ == "__main__":
    logger.info("Starting student attendance data generation...")
    generate_student_data() 