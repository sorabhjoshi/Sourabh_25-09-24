import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import pytz
import uuid
from flask import Flask, jsonify, send_file
import threading
import time
import os
from queue import Queue
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def init_database():
    conn = sqlite3.connect('store_monitoring.db')
    cursor = conn.cursor()

    store_status=pd.read_csv('store_status.csv')
    store_hours=pd.read_csv('store_hours.csv')
    store_timezones=pd.read_csv('store_timezones.csv')

    store_hours = store_hours.rename(columns={
        'day' : 'day_of_week',
        'start_time_local' : 'start_time_local',
        'end_time_local': 'end_time_local'
    })

    store_status.to_sql('store_status', conn, if_exists='replace', index=False)
    store_hours.to_sql('store_hours', conn, if_exists='replace', index=False)
    store_timezones.to_sql('store_timezones', conn, if_exists='replace', index=False)

    cursor.execute("CREATE INDEX idx_store_status_store_id ON store_status (store_id)")
    cursor.execute("CREATE INDEX idx_store_status_timestamp ON store_status (timestamp_utc)")
    cursor.execute("CREATE INDEX idx_store_hours_store_id ON store_hours (store_id)")
    cursor.execute("CREATE INDEX idx_store_timezones_store_id ON store_timezones (store_id)")

    conn.commit()
    conn.close()

init_database()


def generate_report(report_id):
    conn = sqlite3.connect('store_monitoring.db')
    cursor = conn.cursor()

    try:
        start_time = time.time()
        logging.info(f"Starting report generation for report_id:{report_id}")

        cursor.execute("SELECT MAX(timestamp_utc) FROM store_status")
        current_time = datetime.strptime(cursor.fetchone()[0], '%Y-%m-%d %H:%M:%S.%f %Z')

        cursor.execute("SELECT DISTINCT store_id FROM store_status")
        store_ids = [row[0] for row in cursor.fetchall()]
        
        report_data = []
        
        for i, store_id in enumerate(store_ids):
            if i % 100 == 0:
                logging.info(f"Processing store {i+1}/{len(store_ids)}")

            cursor.execute("SELECT timezone_str FROM store_timezones WHERE store_id = ?", (store_id,))
            timezone_str = cursor.fetchone()
            if timezone_str:
                timezone = pytz.timezone(timezone_str[0])
            else:
                timezone = pytz.timezone('America/Chicago')
            
            local_time = current_time.astimezone(timezone)
            last_hour_start = local_time - timedelta(hours=1)
            last_day_start = local_time - timedelta(days=1)
            last_week_start = local_time - timedelta(weeks=1)
            
            cursor.execute("""
                SELECT day_of_week, start_time_local, end_time_local 
                FROM store_hours 
                WHERE store_id = ?
            """, (store_id,))
            store_hours_data = cursor.fetchall()
            
            if not store_hours_data:
                store_hours_data = [(i, '00:00:00','23:59:59') for i in range(7)]
            
            uptime_last_hour, uptime_last_day, uptime_last_week = 0, 0, 0
            downtime_last_hour, downtime_last_day, downtime_last_week = 0, 0, 0
            
            for interval_start, interval_end in [(last_hour_start, local_time),
                                                 (last_day_start, local_time),
                                                 (last_week_start, local_time)]:
                cursor.execute("""
                    SELECT timestamp_utc, status 
                    FROM store_status 
                    WHERE store_id = ? AND timestamp_utc BETWEEN ? AND ?
                    ORDER BY timestamp_utc
                """, (store_id, interval_start, interval_end))
                status_data = cursor.fetchall()
                
                if status_data:
                    prev_status = status_data[0][1]
                    prev_time = datetime.strptime(status_data[0][0], '%Y-%m-%d %H:%M:%S.%f %Z')
                    
                    for timestamp_utc, status in status_data[1:]:
                        current_time = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S.%f %Z')
                        duration = (current_time - prev_time).total_seconds() / 60
                        
                        if is_store_hours(store_id, prev_time, store_hours_data, timezone):
                            if prev_status == 'active':
                                if interval_start == last_hour_start:
                                    uptime_last_hour += duration
                                if interval_start == last_day_start:
                                    uptime_last_day += duration / 60  
                                if interval_start == last_week_start:
                                    uptime_last_week += duration / 60  
                            else:
                                if interval_start == last_hour_start:
                                    downtime_last_hour += duration
                                if interval_start == last_day_start:
                                    downtime_last_day += duration / 60  
                                if interval_start == last_week_start:
                                    downtime_last_week += duration / 60  
                        
                        prev_status = status
                        prev_time = current_time
            
            report_data.append({
                'store_id': store_id,
                'uptime_last_hour': round(uptime_last_hour, 2),
                'uptime_last_day': round(uptime_last_day, 2),
                'uptime_last_week': round(uptime_last_week, 2),
                'downtime_last_hour': round(downtime_last_hour, 2),
                'downtime_last_day': round(downtime_last_day, 2),
                'downtime_last_week': round(downtime_last_week, 2)
            })
        
        report_df = pd.DataFrame(report_data)
        csv_filename = f'{report_id}.csv'
        report_df.to_csv(csv_filename, index=False)

        end_time = time.time()
        logging.info(f"Report generation completed for report_id: {report_id}. Time taken: {end_time - start_time:.2f} seconds")

        return csv_filename
    except Exception as e:
        logging.error(f"Error in report generation for report_id: {report_id}. Error: {str(e)}")
        raise e
    finally:
        cursor.close()
        conn.close()

def is_store_hours(store_id, time, store_hours_data, timezone):
    local_time = time.astimezone(timezone)
    day_of_week = local_time.weekday()
    time_str = local_time.strftime('%H:%M:%S')
    
    for sh_day, start_time, end_time in store_hours_data:
        if sh_day == day_of_week and start_time <= time_str <= end_time:
            return True
    
    return False


app = Flask(__name__)

reports = {}
report_queue = Queue()

def worker():
    while True:
        report_id = report_queue.get()
        if report_id is None:
            break
        try:
            csv_filename = generate_report(report_id)
            reports[report_id]['status'] = 'Complete'
            reports[report_id]['data'] = csv_filename
        except Exception as e:
            reports[report_id]['status'] = 'Failed'
            reports[report_id]['error'] = str(e)
        finally:
            report_queue.task_done()

worker_thread = threading.Thread(target=worker)
worker_thread.start()

@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    report_id = str(uuid.uuid4())
    reports[report_id] = {'status': 'Running', 'data': None, 'start_time': time.time()}
    report_queue.put(report_id)
    return jsonify({'report_id':report_id})

@app.route('/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    if report_id not in reports:
        return jsonify({'error': 'Report not found'}), 404
    
    report_info = reports[report_id]
    
    if report_info['status'] == 'Running':
        if time.time() - report_info['start_time'] > 900:  
            report_info['status'] = 'Failed'
            report_info['error'] = 'Report generation timed out'
        else:
            return jsonify({'status': 'Running', 'message': 'Report is still being generated'})
    
    if report_info['status'] == 'Failed':
        return jsonify({'status': 'Failed', 'error': report_info['error']}), 500
    
    if report_info['status'] == 'Complete':
        if os.path.exists(report_info['data']):
            return send_file(report_info['data'], as_attachment=True)
        else:
            return jsonify({'status':'Failed','error':'Report file not found'}), 500

    return jsonify({'status': 'Unknown', 'error': 'Unexpected report status'}), 500

if __name__ == '__main__':
    app.run(debug=True)

report_queue.put(None)
worker_thread.join()