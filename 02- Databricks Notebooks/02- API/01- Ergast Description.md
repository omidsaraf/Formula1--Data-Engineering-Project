### Data Source:
---
#### Main:
http://ergast.com/api/f1

#### extracting files:
````python
Subjects = ["qualifying", "circuits", "constructors", "drivers", "races", "results"]

# Assuming you want to iterate from 2020 to the current year, you need to use the correct range

from datetime import datetime
current_year = datetime.now().year
years = list(range(2020, current_year + 1))

for year in years:
    for Subject in Subjects:
        # Correct indentation for the print statement
        print(f"http://ergast.com/api/f1/{year}/{Subject}.json")

````html
http://ergast.com/api/f1/2020/qualifying.json
http://ergast.com/api/f1/2020/circuits.json
http://ergast.com/api/f1/2020/constructors.json
http://ergast.com/api/f1/2020/drivers.json
http://ergast.com/api/f1/2020/races.json
http://ergast.com/api/f1/2020/results.json
http://ergast.com/api/f1/2021/qualifying.json
http://ergast.com/api/f1/2021/circuits.json
http://ergast.com/api/f1/2021/constructors.json
http://ergast.com/api/f1/2021/drivers.json
http://ergast.com/api/f1/2021/races.json
http://ergast.com/api/f1/2021/results.json
http://ergast.com/api/f1/2022/qualifying.json
http://ergast.com/api/f1/2022/circuits.json
http://ergast.com/api/f1/2022/constructors.json
http://ergast.com/api/f1/2022/drivers.json
http://ergast.com/api/f1/2022/races.json
http://ergast.com/api/f1/2022/results.json
http://ergast.com/api/f1/2023/qualifying.json
http://ergast.com/api/f1/2023/circuits.json
http://ergast.com/api/f1/2023/constructors.json
http://ergast.com/api/f1/2023/drivers.json
http://ergast.com/api/f1/2023/races.json
http://ergast.com/api/f1/2023/results.json
http://ergast.com/api/f1/2024/qualifying.json
http://ergast.com/api/f1/2024/circuits.json
http://ergast.com/api/f1/2024/constructors.json
http://ergast.com/api/f1/2024/drivers.json
http://ergast.com/api/f1/2024/races.json
http://ergast.com/api/f1/2024/results.json
