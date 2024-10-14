### Data Source:
---
#### Main:
http://ergast.com/api/f1

#### extracting files:
````python
# Correctly initialize the Subject list with strings, not using range
Subjects = ["qualifying", "circuits", "constructors", "drivers", "races", "results"]

# Assuming you want to iterate from 2020 to the current year, you need to use the correct range
from datetime import datetime
current_year = datetime.now().year
years = list(range(2021, current_year + 1))

for year in years:
    for Subject in Subjects:
        # Correct indentation for the print statement
        print(f"http://ergast.com/api/f1/{year}/{Subject}.json")
    print (f" all files for {year} has been returned")
````
##### output (for 2021 only):

http://ergast.com/api/f1/2021/qualifying.json

http://ergast.com/api/f1/2021/circuits.json

http://ergast.com/api/f1/2021/constructors.json

http://ergast.com/api/f1/2021/drivers.json

http://ergast.com/api/f1/2021/races.json

http://ergast.com/api/f1/2021/results.json

