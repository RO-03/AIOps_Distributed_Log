import glob

files = glob.glob('config/grafana/provisioning/dashboards/*.json')
for f in files:
    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    content = content.replace(" WHERE log_date >= NOW() - INTERVAL '30 days'", "")
    content = content.replace(" WHERE log_date >= NOW() - INTERVAL '7 days'", "")
    content = content.replace(" WHERE log_date = CURRENT_DATE", "")
    content = content.replace(" WHERE event_time >= NOW() - INTERVAL '7 days'", "")
    content = content.replace(" WHERE summary_date >= NOW() - INTERVAL '30 days'", "")
    
    with open(f, 'w', encoding='utf-8') as file:
        file.write(content)
print('Done modifying dashboards.')
