#!python3

import requests
from bs4 import BeautifulSoup
import csv
import pgeocode
import datetime
import shutil

import sqlite3
import os

def parse_edikt_type(ediktstring):
    if "Versteigerung" in ediktstring:
        return "Versteigerung"
    elif "Entfall des Termins" in ediktstring:
        return "Entfall"
    elif "Zuschlag mit Überbot" in ediktstring:
        return "Zuschlag+"
    elif "Zuschlag ohne Überbot" in ediktstring:
        return "Zuschlag-"
    
def parse_edikt_date(ediktstring):
    if "(" in ediktstring:
        return ediktstring[ediktstring.find("(")+1:ediktstring.find(")")]
    else:
        return None

def parse_edikt_plz(ediktstring):
    return ediktstring[0:4]

def createDBcursor(dbfile_path: str):
    if not os.path.exists(dbfile_path):
        # create a table if it does not exist in the database
        con = sqlite3.connect(dbfile_path)
        con.enable_load_extension(True)
        con.execute('''SELECT load_extension('mod_spatialite')''')
        con.execute("SELECT InitSpatialMetadata(1)")

        con.execute('''
            CREATE TABLE IF NOT EXISTS edikte (
                edikt TEXT,
                link TEXT,
                ortsstring TEXT,
                objektbezeichnung TEXT,
                edikttype TEXT,
                ediktdate DATE,
                plz INTEGER,
                geocode_placename TEXT,
                geocode_countyname TEXT,
                geocode_lat REAL,
                geocode_lon REAL,
                geocode_accuracy TEXT,
                fetchdate DATETIME,
                checked INTEGER DEFAULT 0,
                UNIQUE (link, edikt)
            )
        ''')

        con.execute('''SELECT AddGeometryColumn('edikte', 'geom', 4326, 'POINT', 'XY')''')
        con.execute('''SELECT CreateSpatialIndex('edikte', 'geom')''')

        con.execute('''
            CREATE VIEW IF NOT EXISTS edikte_only_latest AS
            SELECT * 
            FROM edikte 
            WHERE (link, fetchdate) IN 
                (SELECT link, MAX(fetchdate) 
                FROM edikte 
                GROUP BY link);
        ''')
        con.execute('''
            INSERT INTO views_geometry_columns 
                (view_name, view_geometry, view_rowid, f_table_name, f_geometry_column, read_only)
            VALUES
                ('edikte_only_latest', 'geom', 1, 'edikte', 'geom', 1)
        ''')

        con.commit()
    else:
        con = sqlite3.connect(dbfile_path)
        con.enable_load_extension(True)
        con.execute('''SELECT load_extension('mod_spatialite')''')
    cur = con.cursor()
    return((con,cur))



server = "edikte.justiz.gv.at"
basepath = "/edikte/ex/exedi3.nsf"
search = "/suchedi?SearchView&subf=eex&SearchOrder=4&SearchMax=4999&retfields=~VKat=EH&ftquery=&query=%28%5BVKat%5D%3D%28EH%29%29"
url = f"https://{server}{basepath}{search}"

timestamp = datetime.datetime.now().strftime("%Y%m%d")
timestamp_full = datetime.datetime.now().strftime("%Y%m%d %H%M%S")
csv_file_path = "extracted.csv"
sqlite_file_path = "edikte.sqlite"

con, cur = createDBcursor(sqlite_file_path)

geocoder = pgeocode.Nominatim("at")
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")
#print(response.text)

# get all rows of the table with the class "rowlink"
table = soup.find_all(class_="rowlink")
rows = table[0].find_all("tr")

print("done scraping")

selectedrows = []
for row in rows:
    # get all cells of the row
    cells = row.find_all("td")

    rowdata = {"edikt":"", "link":"", "ortsstring":"", "objektbezeichnung": ""}
    for i, cell in enumerate(cells):
        if   i == 0: continue # cell just contains a javascript count()

        elif i == 1: # cell contains "Edikt und Datum und Link"
            rowdata['edikt'] = cell.text
            rowdata['link'] = f"https://{server}{basepath}/{cell.find('a')['href']}"
            rowdata['edikttype'] = parse_edikt_type(cell.text)
#            if rowdata['edikttype'] in ['Versteigerung','Entfall']:
            rowdata['ediktdate'] = parse_edikt_date(cell.text)

        elif i == 2: # cell contains Ort
            rowdata['ortsstring'] = cell.text.replace("Einfamilienhaus","")
            rowdata['plz'] = parse_edikt_plz(cell.text)
            geocoded_data = geocoder.query_postal_code(rowdata['plz'])
            rowdata['geocode_placename'] = geocoded_data['place_name']
            rowdata['geocode_countyname'] = geocoded_data['county_name']
            rowdata['geocode_lat'] = geocoded_data['latitude']
            rowdata['geocode_lon'] = geocoded_data['longitude']
            rowdata['geocode_accuracy'] = geocoded_data['accuracy']
        elif i == 3: # cell contains Objektbezeichnung
            rowdata['objektbezeichnung'] = cell.text

    # add rowdata to the table edikte
    sqliterow = ( rowdata['edikt'],
                  rowdata['link'],
                  rowdata['ortsstring'],
                  rowdata['objektbezeichnung'],
                  rowdata['edikttype'],
                  rowdata['ediktdate'],
                  rowdata['plz'],
                  rowdata['geocode_placename'],
                  rowdata['geocode_countyname'],
                  rowdata['geocode_lat'],
                  rowdata['geocode_lon'],
                  rowdata['geocode_accuracy'],
                  timestamp_full,
                  0
                )
    try:
        cur.execute(
            f"INSERT INTO edikte (edikt,link,ortsstring,objektbezeichnung,edikttype,ediktdate,plz,geocode_placename,geocode_countyname,geocode_lat,geocode_lon,geocode_accuracy,fetchdate,checked,geom) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GeomFromText('POINT({rowdata['geocode_lon']} {rowdata['geocode_lat']})', 4326) )",
            sqliterow
        )
    except sqlite3.IntegrityError as e:
        #print(f"there was an error: {e}")
        pass
    con.commit()
    if rowdata['edikttype'] in ['Versteigerung']: selectedrows.append(rowdata)

#print(selectedrows)

# Define the field names for the CSV file
field_names = ['edikt', 'link', 'ortsstring', 'objektbezeichnung','edikttype','ediktdate','plz','geocode_placename','geocode_countyname','geocode_lat','geocode_lon','geocode_accuracy']

print("done analysing")

# Write the data from selectedrows to the CSV file
with open(csv_file_path, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(selectedrows)

shutil.copyfile(csv_file_path, f"{timestamp}_{csv_file_path}")

print("done writing csv")
con.close()