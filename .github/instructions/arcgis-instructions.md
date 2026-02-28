---  
description: 'ArcGIS best practices'  
---  

When writing code targeting ArcGIS, it is expected that the Python environment comes from either ArcGIS Pro or ArcGIS Server being installed on the machine. ArcGIS uses Conda to manage Python environments, so do NOT attempt to create virtual environments with any other tool - i.e. do NOT use UV, Poetry, venv, etc. Where necessary, we expect the user to clone the default python environment and activate the environment they want us to work within, so do NOT attempt to create new conda environments. 

