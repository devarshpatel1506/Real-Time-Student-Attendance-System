import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import CASSANDRA_HOSTS, CASSANDRA_KEYSPACE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AttendanceAnalyzer:
    def __init__(self):
        self.cluster = Cluster(CASSANDRA_HOSTS)
        self.session = self.cluster.connect(CASSANDRA_KEYSPACE)

    def _fetch_attendance_data(self):
        """Fetch attendance data from Cassandra."""
        # First get all unique lecture IDs
        lecture_query = "SELECT DISTINCT lecture_id FROM attendance"
        lecture_rows = self.session.execute(lecture_query)
        lecture_ids = [row.lecture_id for row in lecture_rows]
        
        if not lecture_ids:
            logger.warning("No lectures found in the database")
            return pd.DataFrame()
        
        # Fetch all attendance records
        all_records = []
        for lecture_id in lecture_ids:
            query = """
                SELECT student_id, lecture_id, timestamp, is_valid
                FROM attendance
                WHERE lecture_id = %s
                ALLOW FILTERING
            """
            rows = self.session.execute(query, [lecture_id])
            for row in rows:
                all_records.append({
                    'student_id': row.student_id,
                    'lecture_id': row.lecture_id,
                    'timestamp': row.timestamp,
                    'is_valid': row.is_valid
                })
        
        if not all_records:
            logger.warning("No attendance records found")
            return pd.DataFrame()
            
        return pd.DataFrame(all_records)

    def generate_insights(self):
        """Generate insights from attendance data."""
        logger.info("Generating attendance insights...")
        
        df = self._fetch_attendance_data()
        if df.empty:
            logger.warning("No attendance data found")
            return []
        
        insights = []
        
        # 1. Identify habitual latecomers
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        late_threshold = 9  # 9 AM
        late_students = df[df['hour'] >= late_threshold].groupby('student_id').size()
        frequent_late = late_students[late_students > late_students.median()]
        
        insights.append({
            'title': 'Habitual Latecomers',
            'description': f'Found {len(frequent_late)} students who frequently arrive after {late_threshold}:00 AM',
            'data': frequent_late.to_dict()
        })
        
        # 2. Attendance patterns by day of week
        df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.day_name()
        day_patterns = df.groupby('day_of_week').size()
        
        insights.append({
            'title': 'Attendance by Day',
            'description': 'Distribution of attendance across different days',
            'data': day_patterns.to_dict()
        })
        
        # 3. Most and least attended lectures
        lecture_attendance = df.groupby('lecture_id').size().sort_values(ascending=False)
        
        insights.append({
            'title': 'Lecture Attendance Rankings',
            'description': 'Most and least attended lectures',
            'data': {
                'most_attended': lecture_attendance.head(3).to_dict(),
                'least_attended': lecture_attendance.tail(3).to_dict()
            }
        })
        
        # 4. Consistency analysis
        student_consistency = df.groupby('student_id').size()
        consistent_students = student_consistency[
            student_consistency > student_consistency.median() + student_consistency.std()
        ]
        
        insights.append({
            'title': 'Most Consistent Attendees',
            'description': 'Students with above-average attendance',
            'data': consistent_students.to_dict()
        })
        
        # 5. Invalid attendance attempts
        invalid_attempts = df[~df['is_valid']].groupby('student_id').size()
        
        insights.append({
            'title': 'Invalid Attendance Attempts',
            'description': 'Number of invalid attendance attempts by student ID',
            'data': invalid_attempts.to_dict() if not invalid_attempts.empty else {}
        })
        
        return insights

    def print_insights(self, insights):
        """Print insights in a formatted way."""
        if not insights:
            print("\nNo insights available - no attendance data found.")
            return
            
        for insight in insights:
            print(f"\n=== {insight['title']} ===")
            print(insight['description'])
            print("Data:")
            if isinstance(insight['data'], dict) and insight['data']:
                for key, value in insight['data'].items():
                    if isinstance(value, dict):
                        print(f"\n{key}:")
                        for k, v in value.items():
                            print(f"  {k}: {v}")
                    else:
                        print(f"{key}: {value}")
            else:
                print("No data available")
            print("-" * 50)

    def cleanup(self):
        """Clean up Cassandra connection."""
        self.cluster.shutdown()

if __name__ == "__main__":
    analyzer = AttendanceAnalyzer()
    try:
        insights = analyzer.generate_insights()
        analyzer.print_insights(insights)
    finally:
        analyzer.cleanup() 