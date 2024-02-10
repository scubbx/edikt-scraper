#!python3

import requests
from bs4 import BeautifulSoup
import csv
import pgeocode
import datetime
import shutil

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
    return ediktstring[ediktstring.find("(")+1:ediktstring.find(")")]

def parse_edikt_plz(ediktstring):
    return ediktstring[0:4]

server = "edikte.justiz.gv.at"
basepath = "/edikte/ex/exedi3.nsf"
search = "/suchedi?SearchView&subf=eex&SearchOrder=4&SearchMax=4999&retfields=~VKat=EH&ftquery=&query=%28%5BVKat%5D%3D%28EH%29%29"
url = f"https://{server}{basepath}{search}"

#timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
timestamp = datetime.datetime.now().strftime("%Y%m%d")
csv_file_path = "extracted.csv"

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
            if rowdata['edikttype'] in ['Versteigerung','Entfall']:
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