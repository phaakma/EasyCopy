import os
import sys
import json

#sys.path.insert(1, r"D:\DATA\Transformer")

from EasyCopy import EasyCopy

with open(r"D:\config\credentials.json") as f:
    credentials = json.loads(f.read())
scripts_credentials = credentials["scripts"]
arcgis_online_credentials = credentials["agol"]

portalUrl = scripts_credentials["portalUrl"]
username = scripts_credentials["username"]
password = scripts_credentials["password"]

# ## Optionally set any of these variables to None to disable logging to them.
logFolder = r"C:\temp\logs"
logTableUrl = r"https://services.arcgis.com/1234/ArcGIS/rest/services/Logs/FeatureServer/0"
profile = 'myprofile'

# create EasyCopy instance using a stored profile
easycopy = EasyCopy(logFolder=logFolder, logTableUrl=logTableUrl, profile=profile)

# create EasyCopy instance using credentials ready from a file
easycopy = EasyCopy(logFolder=logFolder, logTableUrl=logTableUrl, portalUrl=portalUrl, username=username, password=password)

# examples
SOURCE = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset1")
TARGET = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset2")
TARGET2 = r"https://services.arcgis.com/1234/ArcGIS/rest/services/target/FeatureServer/0"

# Truncate where target is a feature class
easycopy.refreshData(source=SOURCE, target=TARGET, method="TRUNCATE")

# Truncate where target is a feature service
easycopy.refreshData(source=SOURCE, target=TARGET2, method="TRUNCATE", targetPortalUrl=portalUrl, targetUsername=username, targetPassword=password)

# Compare where target is a feature class and the comparison id field is called 'id'
easycopy.refreshData(source=SOURCE, target=TARGET, method="COMPARE", idField="id")

# Compare where target is a feature service and the comparison id field is called 'id'
easycopy.refreshData(source=SOURCE, target=TARGET2, method="COMPARE", idField="id", targetPortalUrl=portalUrl, targetUsername=username, targetPassword=password)

