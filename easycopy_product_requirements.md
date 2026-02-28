# Project Requirements Document: EasyCopy

The following table outlines the detailed functional requirements of the
**EasyCopy** Python package.

EasyCopy is a Python package designed for ArcGIS users to simplify
copying data between feature classes and feature services by
automatically selecting appropriate workflows, enforcing validation
rules, and applying sensible defaults.

------------------------------------------------------------------------

## Functional Requirements

  ------------------------------------------------------------------------------------------------------------------------------------
  Requirement ID   Description      User Story       Expected Behavior / Outcome
  ---------------- ---------------- ---------------- ---------------------------------------------------------------------------------
  FR001            Installable      As a developer,  The project must include packaging configuration and build scripts enabling
                   Python Package   I want EasyCopy  publishing to PyPI and installation via `pip install easycopy`.
                                    to be            
                                    installable via  
                                    pip so that it   
                                    can be easily    
                                    distributed and  
                                    used in          
                                    different        
                                    environments.    

  FR002            PyPI Publishing  As a maintainer, The repository must include build configuration (`pyproject.toml`) and documented
                   Support          I want automated release workflow supporting PyPI publication.
                                    build tooling so 
                                    releases can be  
                                    reliably         
                                    published.       

  FR003            Singleton        As a developer,  Only one EasyCopy instance may exist during runtime. Access is via
                   Architecture     I want EasyCopy  `EasyCopy.copy_data(...)`.
                                    implemented as a 
                                    singleton so     
                                    configuration is 
                                    centralized.     

  FR004            Copy Data Entry  As a user, I     The API exposes
                   Point            want a single    `EasyCopy.copy_data(source, target, copy_method, schema_comparison_type, ...)`.
                                    method to copy   
                                    data so usage is 
                                    simple.          

  FR005            Default          As a user, I     Default folders `./logs` and `./changesets` are created if not specified during
                   Configuration    want logs and    initialization.
                   Folders          change outputs   
                                    automatically    
                                    organized so I   
                                    don't need       
                                    manual setup.    

  FR006            Logging          As a developer,  Logging uses Python's `logging` module with timestamps, severity levels,
                   Implementation   I want           traceback details, and line numbers for errors.
                                    standardized     
                                    logging so       
                                    issues are       
                                    diagnosable.     

  FR007            Environment      As a user, I     EasyCopy verifies `arcpy` and `arcgis` modules are installed and licensed before
                   Validation       want validation  execution.
                                    of prerequisites 
                                    so failures      
                                    occur early.     

  FR008            Supported Copy   As a user, I     Supported methods include `TRUNCATE_APPEND` and `CHANGEDETECTION`.
                   Methods          want flexibility 
                                    in how data is   
                                    copied so        
                                    workflows match  
                                    my needs.        

  FR009            Change Detection As a user, I     When `CHANGEDETECTION` is selected, `idField` is mandatory; execution fails if
                   Requirements     want safeguards  omitted.
                                    when using       
                                    change detection 
                                    so comparisons   
                                    are valid.       

  FR010            Change Detection As a user, I     The system compares source and target datasets and produces add, update, and
                   Processing       want only        delete lists.
                                    changed records  
                                    applied so       
                                    updates are      
                                    efficient.       

  FR011            Spatially        As a developer,  Where viable, ArcGIS Spatially Enabled DataFrames (SDFs) are used for comparison
                   Enabled          I want efficient operations.
                   DataFrame        comparison       
                   Comparison       tooling so       
                                    change detection 
                                    is performant.   

  FR012            Geometry         As a GIS user, I Geometry comparisons must support Z values and curve geometries.
                   Comparison       want geometry    
                                    changes detected 
                                    accurately so    
                                    spatial edits    
                                    are preserved.   

  FR013            Change Set       As a user, I     If `log_changesets=True`, CSV files describing adds, updates, and deletes are
                   Logging          want optional    generated.
                                    audit outputs so 
                                    I can review     
                                    applied changes. 

  FR014            Feature Service  As a user, I     If target Feature Service has sync enabled, truncate is blocked and an error is
                   Truncate         want safeguards  raised.
                   Validation       against unsafe   
                                    truncation so    
                                    sync-enabled     
                                    services are     
                                    protected.       

  FR015            Feature Service  As a user, I     Changes are applied via ArcGIS REST API in configurable batches.
                   Change           want reliable    
                   Application      updates to       
                                    hosted services  
                                    so operations    
                                    don't fail due   
                                    to timeouts.     

  FR016            Feature Class    As a database    If a feature class is versioned, truncate is prevented and execution stops with
                   Truncate         user, I want     an error.
                   Validation       protection       
                                    against          
                                    truncating       
                                    versioned data.  

  FR017            Feature Class    As a user, I     Data append operations use the ArcPy Append geoprocessing tool.
                   Append Operation want efficient   
                                    local data       
                                    copying.         

  FR018            Schema           As a user, I     Source and target schemas are validated before copying begins.
                   Validation       want schema      
                                    validation so    
                                    incompatible     
                                    datasets are     
                                    detected early.  

  FR019            Soft Schema      As a user, I     Compatible conversions (e.g., int→string, smaller text→larger text) are accepted.
                   Comparison       want reasonable  
                   (Default)        flexibility      
                                    between schemas  
                                    so minor         
                                    differences      
                                    don't block      
                                    workflows.       

  FR020            Hard Schema      As a user, I     Hard comparison enforces stricter rules while accounting for known ArcGIS
                   Comparison       want strict      datatype equivalencies (e.g., float vs double).
                                    validation when  
                                    required for     
                                    data integrity.  

  FR021            Enterprise       As a user, I     `.sde` connection files embedded in dataset paths provide authentication
                   Geodatabase      want             automatically.
                   Authentication   authentication   
                                    handled          
                                    automatically so 
                                    I don't manage   
                                    credentials      
                                    manually.        

  FR023            Indexed Field    As a user, I     If `idField` lacks an index, a warning is logged including dataset details.
                   Warning          want performance 
                                    warnings so I    
                                    can optimize     
                                    datasets.        

  FR024            Supported Source As a developer,  Feature class inputs must be `arcpy.FeatureLayer` objects.
                   Types            I want clear     
                                    input contracts  
                                    so misuse is     
                                    prevented.       

  FR025            Supported        As a developer,  Feature services must be `arcgis.features.FeatureLayer` instances.
                   Feature Service  I want           
                   Types            authenticated    
                                    inputs enforced. 

  FR026            Pre-Execution    As a user, I     Validation includes environment checks, schema comparison, authentication
                   Validation       want errors      verification, and parameter validation.
                   Pipeline         detected before  
                                    processing       
                                    starts.          

  FR027            Sensible         As a user, I     Users may specify only source and target; EasyCopy determines workflow
                   Defaults         want minimal     automatically.
                                    configuration so 
                                    copying data is  
                                    easy.            

  FR028            Error Handling   As a user, I     Exceptions include actionable messages, stack trace, and failing component
                                    want meaningful  details.
                                    error messages   
                                    so issues can be 
                                    resolved         
                                    quickly.         

  FR029            Batch Processing As a user, I     Feature service edits are chunked into batches sized to avoid API
                   Controls         want large       limits/timeouts.
                                    operations       
                                    handled safely.  

  FR030            Execution        As a user, I     Logs clearly describe detected workflow decisions, validations, and applied
                   Transparency     want visibility  operations.
                                    into what        
                                    EasyCopy is      
                                    doing.           
  ------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## Developer Requirements

  -------------------------------------------------------------------------------
  Requirement ID   Description   User Story         Expected Behavior / Outcome
  ---------------- ------------- ------------------ -----------------------------
  DR001            Python Coding As a developer, I  The code must follow
                   Standards     want consistent    established Python best
                                 coding practices   practices (PEP8, type hints
                                 so the codebase    where appropriate, clear
                                 remains            naming conventions, and
                                 maintainable.      docstrings).

  DR002            Modular       As a maintainer, I Major functionality areas are
                   Namespace     want functionality separated into
                   Design        separated          namespaces/modules (e.g.,
                                 logically so       change detection, schema
                                 future             comparison, validation,
                                 enhancements are   execution engine).
                                 easier.            

  DR003            Change        As a developer, I  All change detection logic
                   Detection     want change        resides within a dedicated
                   Namespace     detection isolated module or namespace.
                                 so it can evolve   
                                 independently.     

  DR004            Schema        As a developer, I  Schema validation and
                   Comparison    want schema        comparison logic resides
                   Namespace     comparison         within its own
                                 isolated for       namespace/module.
                                 maintainability.   

  DR005            Testability   As a developer, I  Core logic must avoid tight
                                 want components    coupling to ArcPy where
                                 independently      feasible to enable unit
                                 testable.          testing through abstraction
                                                    layers.
  -------------------------------------------------------------------------------

------------------------------------------------------------------------

## Non-Functional Requirements

  ------------------------------------------------------------------------
  Requirement ID           Description         Expected Outcome
  ------------------------ ------------------- ---------------------------
  NFR001                   Performance         Change detection must scale
                                               to large datasets using
                                               efficient dataframe
                                               comparison strategies.

  NFR002                   Reliability         Operations must fail safely
                                               without partial truncation
                                               or corruption.

  NFR003                   Maintainability     Codebase must follow Python
                                               packaging standards and
                                               modular architecture.

  NFR004                   Observability       Logging must allow
                                               reconstruction of execution
                                               steps.

  NFR005                   Compatibility       Must support ArcGIS Pro
                                               Python environments where
                                               `arcpy` is licensed.

  NFR006                   Extensibility       Architecture must allow
                                               future copy strategies or
                                               validation rules.
  ------------------------------------------------------------------------

------------------------------------------------------------------------

## Assumptions

-   Users operate within licensed ArcGIS environments.
-   Authentication is completed prior to passing FeatureLayer objects.
-   Network connectivity exists for Feature Service operations.
-   Users understand ArcGIS dataset concepts (feature class vs feature
    service).

------------------------------------------------------------------------

## Out of Scope (Initial Release)

-   GUI interface
-   Automatic index creation
-   Conflict resolution workflows
-   Schema migration or field creation
-   Non-ArcGIS data sources
