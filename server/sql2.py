import sqlite3
from sqlite3 import Error
import sqlite3
import csv

con = sqlite3.connect("FPA_FOD_20170508.sqlite")

cur = con.cursor()

with open("fire_data.csv", 'w', newline='', encoding="utf-8") as file:
  fire_writer = csv.writer(file)
  for row in cur.execute('SELECT FOD_ID, FPA_ID, NWCG_REPORTING_UNIT_ID, NWCG_REPORTING_UNIT_NAME, LATITUDE, LONGITUDE, STATE, COUNTY, FIPS_CODE, STATE, FIPS_NAME FROM FIRES limit 10;'):
    fire_writer.writerow(row)

with open("nwcg.csv", 'w', newline='', encoding="utf-8") as file:
  nwcg_writer = csv.writer(file)
  for row in cur.execute('SELECT * FROM NWCG_UnitIDActive_20170109 limit 10;'):
    nwcg_writer.writerow(row)

locationTableQuery = """
        CREATE TABLE locations (
            state TEXT,
            st TEXT,
            id TEXT
        );"""

cur.execute("DROP TABLE IF EXISTS locations;")
cur.execute(locationTableQuery)
con.commit()

with open("../states.csv", newline='', encoding='utf-8') as csvfile:
    csvReader = csv.reader(csvfile, delimiter=',')
    for row in csvReader:
        con.execute("INSERT INTO locations(state,st,id) VALUES (?,?,?)",(f"{row[0]}",f"{row[1]}", f"{row[2]}"))
    con.commit()

sql = "SELECT * FROM locations limit 10;"
# cursor = cur.execute(sql)
# print(cursor.fetchall())

sql = "SELECT count(*) from locations;"
cursor = cur.execute(sql)
print(cursor.fetchall())

sql = "DELETE FROM locations WHERE st IS NULL OR trim(st) = '';"
cursor = cur.execute(sql)
print(cursor.fetchall())

sql = "SELECT count(*) from locations;"
cursor = cur.execute(sql)
print(cursor.fetchall())

sql = "SELECT ST from locations limit 10;"
# cursor = cur.execute(sql)
# print(cursor.fetchall())

sql = "DELETE FROM FIRES WHERE COUNTY IS NULL OR trim(COUNTY) = '';"
cursor = cur.execute(sql)

sql = "DELETE FROM FIRES WHERE FIPS_CODE IS NULL OR trim(COUNTY) = '';"
cursor = cur.execute(sql)

sql = "DELETE FROM FIRES WHERE FIPS_NAME IS NULL OR trim(FIPS_NAME) = '';"
cursor = cur.execute(sql)

# sql = "select id || '' || FIPS_CODE as full_fips_code, st, FIPS_NAME, FIRE_YEAR from ((select st,id from locations) as loc inner join (SELECT * FROM FIRES) as fir on loc.st == fir.state) limit 10;"
# cursor = cur.execute(sql)
# print(cursor.fetchall())

with open("fire_data_locations.csv", 'w', newline='', encoding="utf-8") as file:
  fire_writer = csv.writer(file)
  for row in cur.execute("select * from (select full_fips_code, count(*), FIRE_YEAR, FIPS_NAME, st from (select id || '' || FIPS_CODE as full_fips_code, st, FIPS_NAME, FIRE_YEAR from ((select st,id from locations) as loc inner join (SELECT * FROM FIRES) as fir on loc.st == fir.state)) group by full_fips_code, FIRE_YEAR) where FIRE_YEAR = 2003 limit 100;"):
    fire_writer.writerow(row)

con.close()
