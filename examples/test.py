import os
from EasyCopy import EasyCopy

# ## Optionally set any of these variables to None to disable logging to them.
logFolder = r"C:\temp\logs"
logTableUrl = r"https://services.arcgis.com/1234/ArcGIS/rest/services/Logs/FeatureServer/0"
profile = None

easycopy = EasyCopy(logFolder=logFolder, logTableUrl=logTableUrl, profile=profile)

# examples
SOURCE = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset1")
TARGET = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset2")

# methods can be TRUNCATE or COMPARE, idField must be the unique identifier
easycopy.refreshData(source=SOURCE, target=TARGET, method="TRUNCATE", idField="id")



