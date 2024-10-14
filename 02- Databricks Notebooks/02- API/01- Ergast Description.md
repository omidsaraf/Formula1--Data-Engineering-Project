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
