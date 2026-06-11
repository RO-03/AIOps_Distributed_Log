import json
import psycopg2
import uuid
import sys

def main():
    try:
        conn = psycopg2.connect(
            dbname="aiops_analytics",
            user="aiops_user",
            password="aiops_pg_2024",
            host="postgres-db",
            port="5432"
        )
        cur = conn.cursor()
        
        with open('alerts.json', 'r', encoding='utf-16') as f:
            lines = f.readlines()
            
        count = 0
        for line in lines:
            line = line.strip()
            if not line or not line.startswith('{'):
                continue
            try:
                alert = json.loads(line)
                alert_id = str(uuid.uuid4())
                
                cur.execute("""
                    INSERT INTO anomaly_alerts_pg 
                    (alert_id, event_time, component, severity, message, host, anomaly_score, cluster_id)
                    VALUES (%s, %s::timestamp, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    alert_id,
                    alert.get("timestamp"),
                    alert.get("component"),
                    alert.get("severity"),
                    alert.get("message"),
                    alert.get("host"),
                    float(alert.get("anomaly_score")) if alert.get("anomaly_score") is not None else None,
                    int(alert.get("cluster_id")) if alert.get("cluster_id") is not None else None
                ))
                count += 1
            except Exception as e:
                print(f"Error parsing line: {line} - {e}")
                
        conn.commit()
        cur.close()
        conn.close()
        print(f"Successfully inserted {count} alerts into anomaly_alerts_pg.")
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main()
