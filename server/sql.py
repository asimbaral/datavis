import sqlite3

# Create a SQL connection to our SQLite database
con = sqlite3.connect("FPA_FOD_20170508.sqlite")

cur = con.cursor()

# The result of a "cursor.execute" can be iterated over by row
for row in cur.execute('SELECT STATE, FIPS_CODE, FIPS_NAME, COUNTY FROM fires order by FIPS_CODE asc limit 10;'):
    print(row)
    print()
    

# Be sure to close the connection
con.close()