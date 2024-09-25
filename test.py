from datetime import datetime

updateFrom = 'None'

setting_updateFrom = datetime.strptime(updateFrom, '%d.%m.%Y').strftime("%Y-%m-%d") if updateFrom != 'None' else None

print(setting_updateFrom)
