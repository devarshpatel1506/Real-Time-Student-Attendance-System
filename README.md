# Real-Time Student Attendance System

This system implements a real-time student attendance tracking solution using Apache Pulsar, Cassandra, and Redis. It simulates RFID-based attendance tracking with features for data generation, processing, and analysis.

## Architecture

- **Apache Pulsar**: Handles real-time messaging for attendance events
- **Redis**: Implements Bloom Filter for student ID validation and HyperLogLog for unique attendance counting
- **Cassandra**: Stores detailed attendance records
- **Python**: Implementation language with various data processing libraries

## Prerequisites

- Python 3.8+
- Apache Pulsar
- Redis Stack Server
- Apache Cassandra
- Docker (recommended for running services)

## Setup

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Start required services using Docker:

```bash
# Start Pulsar
docker run -it -p 6650:6650 -p 8080:8080 --name pulsar apachepulsar/pulsar:latest bin/pulsar standalone

# Start Redis Stack
docker run -p 6379:6379 redis/redis-stack-server

# Start Cassandra
docker run -p 9042:9042 --name cassandra cassandra:latest
```

## Project Structure

```
attendance_system/
├── config/
│   └── config.py         # Configuration settings
├── src/
│   ├── data_generator.py    # Simulates student attendance data
│   ├── attendance_processor.py  # Processes attendance events
│   └── attendance_analysis.py   # Generates attendance insights
├── requirements.txt
└── README.md
```

## Usage

1. Start the data generator to simulate student attendance:
```bash
python attendance_system/src/data_generator.py
```

2. Run the attendance processor in a separate terminal:
```bash
python attendance_system/src/attendance_processor.py
```

3. Generate insights from collected data:
```bash
python attendance_system/src/attendance_analysis.py
```

## Features

1. **Real-time Data Processing**
   - Simulated RFID swipe data generation
   - Real-time message processing with Apache Pulsar
   - Efficient student ID validation using Bloom Filter
   - Unique attendance counting with HyperLogLog

2. **Data Storage**
   - Detailed attendance records in Cassandra
   - Optimized for querying and analysis

3. **Analysis and Insights**
   - Identification of habitual latecomers
   - Attendance patterns by day of week
   - Most and least attended lectures
   - Student consistency analysis
   - Invalid attendance attempt tracking

## Implementation Details

### Bloom Filter Usage
- Used for efficient validation of student IDs
- Configured with error rate of 0.01
- Capacity set for 100,000 students

### HyperLogLog Implementation
- Tracks unique attendees per lecture
- Provides efficient cardinality estimation
- Minimal memory footprint

### Data Analysis
The system provides five key insights:
1. Habitual latecomers identification
2. Day-wise attendance patterns
3. Lecture attendance rankings
4. Consistent attendee analysis
5. Invalid attendance attempt tracking

## Error Handling

- Graceful handling of invalid student IDs
- Robust message processing with acknowledgments
- Proper cleanup of resources and connections
- Comprehensive logging for debugging

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request 