# EasyCopy
A basic python script to aid copying between ArcGIS datasets using Arcpy and the ArcGIS Python API.

## Installation and Import  
Download the EasyCopy.py file and place it in a local folder where it can be accessed from any scripts calling it.
If this file is in the same folder as the script calling it then your calling script will be able to import it. If not, add an extra line to tell your script where to find EasyCopy.py, like this:

`sys.path.insert(1, r"D:\DATA\Transformer")`

Where the second argument is the local path where EasyCopy.py is located.

Import with this statement:  
`from EasyCopy import EasyCopy`

Create an instance of the class using this line:  
`easycopy = EasyCopy(logFolder=logFolder, logTableUrl=logTableUrl, profile=profile)`

Use any name you like as the variable. The three variables are for logging. You can set them all to None if you don't want any logging.

### Log Folder  
A local folder where log files will be saved. The folder must exist already.
> logFolder = r"C:\temp\logs"

### logTableUrl   
A feature service with a predefined schema that logs can be sent to. It must have these fields: log_datetime, levelname, topic, code, message and metric.
If provided then either a profile or a set of portalUrl, username and password must also be provided. 

### profile : str (optional)
The name of a profile stored on the server using the ArcGIS Python API which is used to sign into ArcGIS Enterprise or ArcGIS Online for the log table.
If a profile is provided then it is used and portalUrl, username and password are all ignored.

### portalUrl, username, password
portalUrl : str (optional)
The url of an ArcGIS portal, either ArcGIS Online or ArcGIS Enterprise for the log table.
username : str (optional)
The username to log into the ArcGIS portal for the log table.
password : str (optional)
The password for the user for the log table.

### Examples  

```
logFolder = r"C:\temp\logs"
logTableUrl = r"https://services.arcgis.com/1234/ArcGIS/rest/services/Logs/FeatureServer/0"
profile = 'roger'
portalUrl = 'https://www.arcgis.com"
username = 'joe_bloggs_user'
password = 'pa$$word1'

# use a profile
easycopy = EasyCopy(logFolder=logFolder, logTableUrl=logTableUrl, profile=profile)

#Use portal, username and password
easycopy = EasyCopy(logFolder=logFolder, logTableUrl=logTableUrl, portalUrl=portalUrl, username=username, password=password)
```

## Use to copy data  

Use the refreshData method to copy from a source to a target. Inputs for this method are:
* source - feature class to copy from.
* target - feature class or feature service to copy to.
* method - COMPARE or TRUNCATE.
* idField - if using COMPARE method, an identifier field must be specified which is used to compare source and target features.
* targetProfile - optional profile, if the target is a feature service that is secured. Use either this OR a combination of the following.
* targetPortalUrl - optional, URL of the target portal.
* targetUsername - optional, username for the target portal.
* targetPassword - optional, password for the target portal.
* chunkSize - optional, allow to control how many features are uploaded at a time. Defaults to 250. Reduce for unstable networks and/or if experiencing timeouts.

EasyCopy assumes the source and target have exactly the same schema. If the schemas don't match then an error is thrown and nothing is copied. Common editor tracking fields are ignored during this check.  

If the target is a feature class (e.g. file geodatabase or enterprise geodatabase) then no credential parameters are required. If the target is a feature service, then either a profile or set of url/username/password must be provided.   

If the method is 'TRUNCATE', then all records in the target will be deleted and then repopulated. The truncate batch removes features, so the time it takes is directly related to the number of features.

If the method is 'COMPARE', then a full copy of both the source and target data is pulled down into memory, and the comparison is done in memory. The time this takes is directly related to the number of features in the source and target. As is the memory usage.  

EasyCopy may not be the best option for very large datasets.

### Example usage  

Creating variables for the source and target can help made the code more readable.

```
SOURCE = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset1")
TARGET = os.path.join(r"C:\temp\mydb.sde", "mydb.sdeadmin.mydataset2")
TARGET2 = r"https://services.arcgis.com/1234/ArcGIS/rest/services/target/FeatureServer/0"

# Truncate the target and repopulate:
easycopy.refreshData(source=SOURCE, target=TARGET, method="TRUNCATE")

# Compare based on a field called 'id' and only push changes.
easycopy.refreshData(source=SOURCE, target=TARGET, method="COMPARE", idField="id")

# Comparison to a feature layer. Assuming you have variables for target portal credentials.
easycopy.refreshData(source=SOURCE, target=TARGET2, method="COMPARE", idField="id", targetPortalUrl=portalUrl, targetUsername=username, targetPassword=password)

```

## Future Notes  

Currently, you create an instance of the EasyCopy class and can reuse that instance to copy from different source/targets. In particular, the target portal credentials are set every time you call the refreshData method, causing a new sign in each time the method is used.  
In the future, this might change. The EasyCopy class might be created with target portal, and you could copy to multiple targets using those credentials. If you wanted to copy to a different target you would have to create a new EasyCopy instance.  
Note that this would be a breaking change.  


