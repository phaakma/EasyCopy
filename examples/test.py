import os
import sys
import json

#sys.path.insert(1, r"D:\DATA\Transformer")

from EasyCopy import EasyCopy

with open(r"D:\config\credentials.json") as f:
    credentials = json.loads(f.read())
scripts_credentials = credentials["scripts"]
arcgis_online_credentials = credentials["agol"]

# ## Optionally set any of these variables to None to disable logging to them.
logFolder = r"C:\temp\logs"
logTableUrl = r"https://services.arcgis.com/1234/ArcGIS/rest/services/Logs/FeatureServer/0"
profile = None

easycopy = EasyCopy(logFolder=logFolder, logTableUrl=logTableUrl, profile=profile)

# examples
SOURCE = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset1")
TARGET = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset2")

# methods can be TRUNCATE or COMPARE, idField must be the unique identifier
easycopy.refreshData(source=SOURCE, target=TARGET, method="TRUNCATE")

easycopy.refreshData(source=SOURCE, target=TARGET, method="COMPARE", idField="id")



