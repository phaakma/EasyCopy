import os
import logging
import sys
import json
import shutil
import traceback
import arcgis
from arcgis import GIS
import arcpy
from typing import Optional, Dict
from colorama import Fore, Back, Style
import re
from datetime import date, datetime, timezone
import time
import importlib
from inspect import istraceback
from collections import OrderedDict
import pandas as pd
import copy
import math
import socket
import keyring


class EasyCopy():
    """
        A class used to copy datasets from a source to target(s).
        There are options to log to disk and/or an ArcGIS feature service,
        and the copy methodology can be a simple truncate/append
        or a comparison of the two datasets followed
        by applying adds, updates and deletes as necessary. Changeset
        files are saved to disk.

        ...

        Attributes
        ----------
        logFolder : str (optional)
            A folder where the log files are saved.
            Changesets are also saved in a subfolder of this folder,
            so if not supplied then changeset spreadsheets are
            not saved either.
        logTableUrl : str (optional)
            The url of an ArcGIS feature service table where
            log records can be added to.
            It must have these fields: log_datetime, levelname,
            topic, code, message and metric.
            If left as None then no logs will be added via
            this method.
            If provided then either a profile or a set of
            portalUrl, username and password must also be provided.
        profile : str (optional)
            The name of a profile stored on the server using
            the ArcGIS Python API which is used to sign into
            ArcGIS Enterprise or ArcGIS Online for the log table.
            If a profile is provided then it is used and portalUrl,
            username and password are all ignored.
        portalUrl : str (optional)
            The url of an ArcGIS portal, either ArcGIS Online or
            ArcGIS Enterprise for the log table.
        username : str (optional)
            The username to log into the ArcGIS portal for the log table.
        password : str (optional)
            The password for the user for the log table.

        Methods
        ----------
        refreshData(params)
            Takes a dict object with the source, targets, method
            to use and identifier field and initiates the process
            to update the targets from the sources.

    """

    def __init__(self, logFolder: str = None, logTableUrl: str = None, profile: str = None, portalUrl: str = None, username: str = None, password: str = None) -> None:
        self.logFolder = logFolder
        self.logTableUrl = logTableUrl
        self.profile = profile
        self.portalUrl = portalUrl
        self.username = username
        self.gis = None

        if logTableUrl:
            assert(profile or (portalUrl and username and password)
                   ), f"Log table url provided but insufficient login parameters supplied."
            if profile is not None:
                self.gis = GIS(profile=self.profile)
            else:
                self.gis = GIS(url=self.portalUrl,
                               username=self.username, password=password)
            assert(
                self.gis.users.me), f"Login attempt to ArcGIS was unsuccessful. Check portal and credential parameters."
        self.logger = configureLogging(
            logger_name=__name__, gis=self.gis, log_table_url=self.logTableUrl, logFolder=self.logFolder)
        fqdn = socket.getfqdn()
        if self.gis and self.gis.users.me is not None:
            self.logger.debug({"topic": "INITIALIZE", "code": "COMPLETED",
                               "message": f"{fqdn}, {self.gis.users.me.username}"})
        else:
            self.logger.debug(
                {"topic": "INITIALIZE", "code": "COMPLETED", "message": f"{fqdn}"})

    def log(self, params, level='debug'):
        if level.lower()=='warning':
            self.logger.warning(params)
        elif level.lower()=='info':
            self.logger.info(params)
        elif level.lower()=='error':
            self.logger.error(params)
        else:
            self.logger.debug(params)

    def sizeof_fmt(self, num, suffix="B"):
        for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f}Yi{suffix}"

    def doComparison(self, target, source, id_fieldname):
        """
            Compare two datasets and return changes.

            NOTE: This relies on there being a common and unique id field between the two datasets.
        """
        doComparisonStart = time.perf_counter()
        targetBasename = os.path.basename(target['path'])
        try:
            objectid_fieldname = None
            id_field = None
            field_list = []
            field_list_without_oid = []

            field_types_to_exclude = self.getFieldTypeExclusions()
            field_names_to_omit = self.getFieldNameExclusions(target, source)

            for field in target['describe'].get('fields'):
                if field.type not in field_types_to_exclude and field.name.lower() not in field_names_to_omit:
                    if field.name == id_fieldname:
                        id_field = field
                    if field.type == 'OID':
                        objectid_fieldname = field.name
                    else:
                        field_list.append(field.name)
                        field_list_without_oid.append(field.name)
            if target['describe'].get('dataType') == 'FeatureClass':
                field_list_without_oid.insert(0, "SHAPE@JSON")
                field_list.insert(0, "SHAPE@JSON")

            fieldTypes = {}
            for field in source['describe'].get("fields"):
                fieldTypes[field.name] = field.type

            origin_objectid_fieldname = "origin_objectid__"
            field_list.insert(0, origin_objectid_fieldname)

            self.logger.debug({"topic": "DETAILS", "code": "COMPARISON",
                               "message": f"field_list: {str(field_list)}"})
            self.logger.debug({"topic": "DETAILS", "code": "COMPARISON",
                               "message": f"id_fieldname: {str(id_fieldname)}, objectid_fieldname: {objectid_fieldname}"})

            # create empty datasets in memory and then append into them from the source and target
            if target['describe'].get('dataType') == 'FeatureClass':
                inmemory_comparison_source = arcpy.management.CreateFeatureclass(
                    out_path="memory", out_name="inmemory_comparison_source", template=target['path'], spatial_reference=target['path'])
                inmemory_comparison_target = arcpy.management.CreateFeatureclass(
                    out_path="memory", out_name="inmemory_comparison_target", template=target['path'], spatial_reference=target['path'])
            else:
                inmemory_comparison_source = arcpy.management.CreateTable(
                    out_path="memory", out_name="inmemory_comparison_source", template=target['path'])
                inmemory_comparison_target = arcpy.management.CreateTable(
                    out_path="memory", out_name="inmemory_comparison_target", template=target['path'])

            arcpy.management.AddField(inmemory_comparison_source, origin_objectid_fieldname,
                                      "LONG", None, None, None, '', "NULLABLE", "NON_REQUIRED", '')
            arcpy.management.Append(inputs=source['path'], target=inmemory_comparison_source,
                                    schema_type='NO_TEST', field_mapping="", subtype="", expression="")
            arcpy.management.AddField(inmemory_comparison_target, origin_objectid_fieldname,
                                      "LONG", None, None, None, '', "NULLABLE", "NON_REQUIRED", '')

            #check for duplicates in the source
            duplicates = []
            with arcpy.da.SearchCursor(inmemory_comparison_source, id_fieldname) as cursor:
                for row in cursor:
                    assert row[0] not in duplicates, f"There are duplicate records in the source of the id field {id_fieldname}, script has stopped. Please resolve duplicates and run again."
                    duplicates.append(row[0])

            fm_oid = arcpy.FieldMap()
            fms = arcpy.FieldMappings()
            fm_oid.addInputField(
                target['path'], target['describe'].get('OIDFieldName'))
            oid_name = fm_oid.outputField
            oid_name.name = origin_objectid_fieldname
            fm_oid.outputField = oid_name
            fms.addFieldMap(fm_oid)
            fields = [f.name for f in target['describe'].get(
                'fields') if f.type not in field_types_to_exclude]
            for field in fields:
                fm = arcpy.FieldMap()
                fm.addInputField(target['path'], field)
                field_name = fm.outputField
                field_name.name = field
                fm.outputField = field_name
                fms.addFieldMap(fm)

            arcpy.management.Append(inputs=target['path'], target=inmemory_comparison_target,
                                    schema_type='NO_TEST', field_mapping=fms, subtype="", expression="")

            changed_identifiers = []
            adds = []
            updates = {}
            deletes = {}
            sr = None
            spatialReference = None
            if target['describe'].get('dataType') == 'FeatureClass':
                sr = arcpy.Describe(
                    inmemory_comparison_target).spatialReference
                spatialReference = sr.exportToString()

            change_set = []

            ## DELETES
            record_ids = set()
            with arcpy.da.SearchCursor(inmemory_comparison_target, field_list) as targetCursor:
                for targetRow in targetCursor:
                    record_id = targetRow[field_list.index(id_fieldname)]
                    origin_objectid = targetRow[field_list.index(
                        origin_objectid_fieldname)]
                    if record_id is None or record_id == "" or record_id in record_ids:
                        deletes[origin_objectid] = (
                            [targetRow[field_list.index(f)] for f in field_list])
                        continue
                    record_ids.add(record_id)
                    if id_field.type == 'String':
                        where_clause = f"{id_fieldname} = '{record_id}'"
                    else:
                        where_clause = f"{id_fieldname} = {record_id}"
                    rowCount = None
                    with arcpy.da.SearchCursor(inmemory_comparison_source, field_list, where_clause=where_clause) as sourceCursor:
                        for rowCount in sourceCursor:
                            break
                    if rowCount is None:
                        deletes[origin_objectid] = (
                            [targetRow[field_list.index(f)] for f in field_list])

            ## ADDS AND UPDATES
            sourceCount = 0
            targetCount = 0
            with arcpy.da.SearchCursor(inmemory_comparison_source, field_list) as sourceCursor:
                for sourceRow in sourceCursor:
                    sourceCount += 1
                    record_id = sourceRow[field_list.index(id_fieldname)]
                    if id_field.type == 'String':
                        where_clause = f"{id_fieldname} = '{record_id}'"
                    else:
                        where_clause = f"{id_fieldname} = {record_id}"

                    with arcpy.da.SearchCursor(inmemory_comparison_target, field_list, where_clause=where_clause) as targetCursor:
                        targetRow = None
                        for targetRow in targetCursor:
                            objectid = targetRow[field_list.index(
                                origin_objectid_fieldname)]
                            if objectid in deletes.keys():
                                continue
                            targetCount += 1
                            for i, field in enumerate(field_list):
                                src = None if sourceRow[i] == "" else sourceRow[i]
                                tgt = None if targetRow[i] == "" else targetRow[i]

                                if field != objectid_fieldname and field != origin_objectid_fieldname and (src != tgt):
                                    # do a deeper check on the geometry. most of the time comparing the
                                    # json objects is enough and is quick, but if that check fails then it pays to
                                    # do a proper 'geometry.equals' to be sure
                                    if src and tgt and field == 'SHAPE@JSON':
                                        source_geom = arcpy.AsShape(src, True)
                                        target_geom = arcpy.AsShape(tgt, True)
                                        if source_geom.equals(target_geom):
                                            continue

                                    # Something has changed so we add to the updates object.
                                    origin_objectid = targetRow[field_list.index(
                                        origin_objectid_fieldname)]
                                    updates[origin_objectid] = [origin_objectid] + [sourceRow[field_list.index(
                                        f)] for f in field_list if f != origin_objectid_fieldname]
                                    break
                        if targetRow is None:
                            # no matching record found in the target, so needs to be added.
                            adds.append(([""]+[sourceRow[field_list.index(f)]
                                               for f in field_list_without_oid]))

            self.logger.debug({"topic": "COUNTS", "code": "COMPLETED",
                               "message": f"sourceCount: {sourceCount} vs targetCount: {targetCount}"})

            adds_length = len(adds)
            updates_length = len(updates.keys())
            deletes_length = len(deletes.keys())

            change_set = []
            for rec in adds:
                r = copy.deepcopy(rec)
                r.append("add")
                change_set.append(r)
            for key in updates.keys():
                r = copy.deepcopy(updates[key])
                r.append("update")
                change_set.append(r)
            for key in deletes.keys():
                r = copy.deepcopy(deletes[key])
                r.append("delete")
                change_set.append(r)

            df_headers = copy.deepcopy(field_list)
            df_headers.append("change_type")
            if len(change_set) > 0 and self.logFolder:
                change_set_df = pd.DataFrame(change_set, columns=df_headers)
                dtNow = datetime.now().strftime("%Y%m%d_%H%M%S")
                outFolder = os.path.join(self.logFolder, "changesets")
                os.makedirs(outFolder, exist_ok=True)
                safeTargetString = target['path'].replace(
                    "/", "_").replace("\\", "_")
                safeTargetString = "".join([c for c in safeTargetString if c.isalpha(
                ) or c.isdigit() or c == ' ' or c == "_"]).rstrip()[-100:]
                outFile = os.path.join(
                    outFolder, f"{dtNow}_{safeTargetString}.xlsx")
                change_set_df.to_excel(outFile)

            field_list[field_list.index(
                origin_objectid_fieldname)] = objectid_fieldname

            self.logger.debug({"topic": "COMPARISON", "code": "COMPLETED",
                               "message": f"Comparison finished for {target['path']}"})
            self.logger.debug({"topic": "ADDS", "code": "METRIC",
                               "message": f"{target['path']} adds count: {adds_length}", "metric": adds_length})
            self.logger.debug({"topic": "UPDATES", "code": "METRIC",
                               "message": f"{target['path']} updates count: {updates_length}", "metric": updates_length})
            self.logger.debug({"topic": "DELETES", "code": "METRIC",
                               "message": f"{target['path']} deletes count: {deletes_length}", "metric": deletes_length})

            result = {"id_fieldname": id_fieldname,
                      "objectid_fieldname": objectid_fieldname,
                      "fieldList": field_list,
                      "spatialReference": spatialReference,
                      "adds": adds,
                      "updates": updates,
                      "deletes": deletes}
            return result

        except Exception as e:
            err = buildErrorMessage(e)
            self.logger.error({"topic": "COMPARISON", "code": "ERROR",
                               "message": f"Target: {target['path']} Error: {err}"})
            traceback.print_tb(e.__traceback__)
        finally:
            for itm in [inmemory_comparison_target, inmemory_comparison_source]:
                try:
                    arcpy.management.Delete(in_data=itm)
                except:
                    pass

            doComparisonExecutionTime = math.ceil(
                time.perf_counter() - doComparisonStart)
            self.logger.debug({"topic": "TIMER", "code": "METRIC",
                               "message": f"doComparison Execution Time = {doComparisonExecutionTime} seconds, target={targetBasename}", "metric": doComparisonExecutionTime})

    def applyChanges(self, target, changes):
        try:
            circuit_breaker_number = 3
            fieldList = changes['fieldList']
            fieldList = [f for f in fieldList if f is not None]
            chunkSize = int(target.get('chunkSize'))

            if 'http' in target.get('path'):
                layer = target['layer']

                # updates are an object. each key is the id field identifier.
                # values is a list of field values in the same order as the fieldList

                updates = []
                for i, item in enumerate(changes.get('updates', {}).values()):
                    attributes = {}
                    geometry = None
                    for n, field in enumerate(fieldList):
                        if field == "SHAPE@JSON":
                            geometry = json.loads(item[n])
                        elif isinstance(item[n], datetime):
                            dt = item[n].replace(tzinfo=timezone.utc)
                            attributes[field] = dt
                        else:
                            attributes[field] = item[n]
                    updates.append(
                        {"attributes": attributes, "geometry": geometry})

                chunkGenerator = (updates[i:i+chunkSize]
                                  for i in range(0, len(updates), chunkSize))
                for i, chunk in enumerate(chunkGenerator):
                    chunk_size_bytes = self.sizeof_fmt(sys.getsizeof(chunk))
                    error_count = 0
                    try:
                        results = layer.edit_features(updates=chunk)
                        for res in results["updateResults"]:
                            if res["success"] is not True:
                                error_count += 1
                                if error_count < circuit_breaker_number:
                                    self.logger.warning({"topic": "UPDATES", "code": "ERROR",
                                                    "message": f"{str(res)}", "target_dataset": target['path']})
                                else:
                                    self.logger.warning({"topic": "UPDATES", "code": "ERROR",
                                                    "message": f"More than {circuit_breaker_number} errors, not logging any more.", "target_dataset": target['path']})

                    except Exception as e:
                        err = buildErrorMessage(e)
                        print(err)
                        traceback.print_tb(e.__traceback__)
                        if "504" in err:
                            self.logger.warning({"topic": "UPDATES", "code": "ERROR",
                                                "message": f"Timeout occured. Chunk {i} ({len(chunk)}, {chunk_size_bytes}). Changes may still have been applied, check final counts.", "target_dataset": target['path']})
                        else:
                            self.logger.warning({"topic": "UPDATES", "code": "ERROR",
                                            "message": f"More than {circuit_breaker_number} errors, not logging any more.", "target_dataset": target['path']})  

                adds = []
                ## Turn the add items into dictionaries if necessary
                if len(changes.get('adds', [])) > 0 and type(changes.get('adds', [])[0]) is not dict:
                    for item in changes.get('adds', []):
                        attributes = {}
                        geometry = {}
                        for n, field in enumerate(fieldList):
                            if field == "SHAPE@JSON":
                                geometry = json.loads(item[n])
                            else:
                                attributes[field] = item[n]
                        adds.append(
                            {"attributes": attributes, "geometry": geometry})
                else:
                    adds = changes.get('adds', [])

                chunkGenerator = (adds[i:i+chunkSize]
                                  for i in range(0, len(adds), chunkSize))
                for i, chunk in enumerate(chunkGenerator):
                    chunk_size_bytes = self.sizeof_fmt(sys.getsizeof(chunk))
                    error_count = 0
                    try:
                        results = layer.edit_features(adds=chunk)
                        for res in results["addResults"]:
                            if res["success"] is not True:
                                error_count += 1
                                if error_count < circuit_breaker_number:                                
                                    self.logger.warning({"topic": "ADDS", "code": "ERROR",
                                                    "message": f"{str(res)}", "target_dataset": target['path']})
                                else:
                                    self.logger.warning({"topic": "ADDS", "code": "ERROR",
                                                    "message": f"More than {circuit_breaker_number} errors, not logging any more.", "target_dataset": target['path']})  
                    except Exception as e:
                        err = buildErrorMessage(e)
                        print(err)
                        traceback.print_tb(e.__traceback__)
                        if "504" in err:
                            self.logger.warning({"topic": "ADDS", "code": "ERROR",
                                                "message": f"Timeout occured. Chunk {i} ({len(chunk)}, {chunk_size_bytes}). Changes may still have been applied, check final counts.", "target_dataset": target['path']})  
                        else:
                            self.logger.warning({"topic": "ADDS", "code": "ERROR",
                                                "message": err, "target_dataset": target['path']})   

                deletes = list(set(changes.get('deletes', {}).keys()))
                chunkGenerator = (deletes[i:i+chunkSize]
                                  for i in range(0, len(deletes), chunkSize))
                for i, chunk in enumerate(chunkGenerator):
                    chunk_size_bytes = self.sizeof_fmt(sys.getsizeof(chunk))
                    error_count = 0
                    try:
                        results = layer.edit_features(deletes=chunk)
                        for res in results["deleteResults"]:
                            if res["success"] is not True:
                                error_count += 1
                                if error_count < circuit_breaker_number:                                
                                    self.logger.warning({"topic": "DELETES", "code": "ERROR",
                                                    "message": f"{str(res)}", "target_dataset": target['path']})
                                else:
                                    self.logger.warning({"topic": "DELETES", "code": "ERROR",
                                                    "message": f"More than {circuit_breaker_number} errors, not logging any more.", "target_dataset": target['path']}) 
                    except Exception as e:
                        err = buildErrorMessage(e)
                        print(err)
                        traceback.print_tb(e.__traceback__)
                        if "504" in err:
                            self.logger.warning({"topic": "DELETES", "code": "ERROR",
                                                "message": f"Timeout occured. Chunk {i} ({len(chunk)}, {chunk_size_bytes}). Changes may still have been applied, check final counts.", "target_dataset": target['path']})
                        else:
                            self.logger.warning({"topic": "DELETES", "code": "ERROR",
                                                "message": err, "target_dataset": target['path']})                            

            else:
                workspace = target['describe'].get('path')

                if target['describe'].get('isVersioned') is True:
                    # Start an edit session
                    self.logger.debug({"topic": "EDIT SESSION", "code": "START",
                                       "message": f"Starting edit session on {workspace}"})
                    editSession = arcpy.da.Editor(workspace)
                    editSession.startEditing(False, True)
                    editSession.startOperation()

                fieldList_with_Shape = copy.deepcopy(fieldList)
                if target['describe'].get('dataType') == 'FeatureClass':
                    fieldList_with_Shape[changes['fieldList'].index(
                        'SHAPE@JSON')] = 'SHAPE@'

                # delete
                deletes = []
                for key in changes["deletes"].keys():
                    deletes.append(
                        changes["deletes"][key][changes["fieldList"].index(changes["id_fieldname"])])
                with arcpy.da.UpdateCursor(target['path'], changes["id_fieldname"]) as updateCursor:
                    for row in updateCursor:
                        if row[0] in deletes:
                            updateCursor.deleteRow()

                # update
                updates = changes["updates"]
                count = 0
                with arcpy.da.UpdateCursor(target['path'], fieldList_with_Shape) as updateCursor:
                    for row in updateCursor:
                        record_id = row[changes["fieldList"].index(
                            changes["id_fieldname"])]
                        objectid = row[fieldList.index(
                            changes["objectid_fieldname"])]
                        new_record = updates.get(objectid)

                        if new_record is not None:
                            count += 1
                            for i, field in enumerate(fieldList_with_Shape):
                                if field == "SHAPE@":
                                    #row[i] = arcpy.FromWKT(new_record[i], sr)
                                    row[i] = arcpy.AsShape(new_record[i], True)
                                else:
                                    row[i] = new_record[i]
                            updateCursor.updateRow(row)

                # add
                adds = changes["adds"]
                with arcpy.da.InsertCursor(target['path'], changes["fieldList"]) as insertCursor:
                    for row in adds:
                        insertCursor.insertRow(row)

                if target['describe'].get('isVersioned') is True:
                    # Stop the edit session and save the changes
                    self.logger.debug({"topic": "EDIT SESSION", "code": "FINISH",
                                       "message": f"Stopping edit session on {workspace}"})
                    editSession.stopOperation()
                    editSession.stopEditing(True)

            return True

        except Exception as e:
            err = buildErrorMessage(e)
            print(err)
            self.logger.error({"topic": "COMPARISON", "code": "ERROR",
                               "message": f"Target: {target.get('path')} Error: {err}"})
            traceback.print_tb(e.__traceback__)
        finally:
            pass

    def compareSchemas(self, source, target):
        if source.get('describe') is None:
            source["describe"] = arcpy.da.Describe(source["path"])
        if target.get('describe') is None:
            target["describe"] = arcpy.da.Describe(target["path"])

        field_names_to_omit = self.getFieldNameExclusions(source, target)
        field_names_to_omit.append('objectid')
        field_types_to_exclude = self.getFieldTypeExclusions()
        field_types_to_exclude.append('OID')

        source_fields = [field for field in source['describe'].get('fields') if field.type not in field_types_to_exclude and field.name.lower() not in field_names_to_omit]
        target_fields = [field for field in target['describe'].get('fields') if field.type not in field_types_to_exclude and field.name.lower() not in field_names_to_omit]

        in_source_not_in_target = []

        message = ""

        for field in source_fields:
            matchFound = False
            for f in target_fields:
                if field.name == f.name and field.type == f.type:
                    matchFound = True 
            if not matchFound:
                in_source_not_in_target.append(field)
                message += f"Source: {field.name}/{field.type}, Target: {f.name}/{f.type}. "

        match = True
        if len(in_source_not_in_target) > 0:
            match = False

        return { "match": match, "fields": in_source_not_in_target, "message": message }

    def getFieldTypeExclusions(self):
        """Return a list of field types that we don't want to compare.
        """
        return ['Blob', 'GlobalID', 'Raster', 'Geometry']
      
    def getFieldNameExclusions(self, target, source):
        """Return a list of reserved field names that we don't want to compare.
        """     
        field_names_to_omit = ['shape_starea__',
                                'shape.starea()',
                                'shape_stlength__',
                                'shape.stlength()',
                                'shape__length',
                                'shape_length',
                                'shape__area',
                                'shape_area',
                                'created_user',
                                'created_date',
                                'creationdate',
                                'last_edited_user',
                                'last_edited_date',
                                'edited_date',
                                'creator',
                                'createdate',
                                'editor',
                                'editdate']

        if target is not None:
            if target.get('describe') is None:
                target["describe"] = arcpy.da.Describe(target["path"])
            field_names_to_omit.append(str(target['describe'].get('creatorFieldName')).lower())
            field_names_to_omit.append(str(target['describe'].get('createdAtFieldName')).lower())
            field_names_to_omit.append(str(target['describe'].get('editorFieldName')).lower())
            field_names_to_omit.append(str(target['describe'].get('editedAtFieldName')).lower())                
            field_names_to_omit.append(str(target['describe'].get('lengthFieldName')).lower())
            field_names_to_omit.append(str(target['describe'].get('areaFieldName')).lower())
        if source is not None:
            if source.get('describe') is None:
                source["describe"] = arcpy.da.Describe(source["path"])
            field_names_to_omit.append(str(source['describe'].get('creatorFieldName')).lower())
            field_names_to_omit.append(str(source['describe'].get('createdAtFieldName')).lower())
            field_names_to_omit.append(str(source['describe'].get('editorFieldName')).lower())
            field_names_to_omit.append(str(source['describe'].get('editedAtFieldName')).lower())                
            field_names_to_omit.append(str(source['describe'].get('lengthFieldName')).lower())
            field_names_to_omit.append(str(source['describe'].get('areaFieldName')).lower()) 
        return field_names_to_omit

    def refreshData(self, source=None, target=None, method="COMPARE", idField=None, targetProfile=None, targetPortalUrl=None, targetUsername=None, targetPassword=None, chunkSize=250):
        """Update a target dataset from a source dataset"""

        params = {
            "id_fieldname": idField,
            "source": {
                "path": source
            },
            "target":
                {
                    "path": target,
                    "method": method,
                    "profile": targetProfile,
                    "portalUrl": targetPortalUrl,
                    "username": targetUsername,
                    "password": targetPassword,
                    "chunkSize": chunkSize
                }           
        }

        self.refreshDatafromParams(params)

    def refreshDatafromParams(self, params):

        refreshDataStart = time.perf_counter()
        result = None
        try:
            source_dataset = ""
            record_count = None
            adds_count = None
            updates_count = None
            deletes_count = None
            finalMessage = ""
            success = 0
            target_dataset = ""

            source = params['source']
            self.logger.debug({"topic": "SOURCE", "code": "START",
                               "message": f"Copy started from {source['path']}", "source_dataset": f"{source['path']}"})

            source_exists = arcpy.Exists(source['path'])
            assert source_exists, f"Source did not exist: {source['path']}"
            source["describe"] = arcpy.da.Describe(source["path"])

            target = params['target']
            chunkSize = int(target.get('chunkSize'))

            if 'http' in target["path"]:
                # check for credentials and log in
                assert(target['profile'] or (target['portalUrl'] and target['username'] and target['password'])
                        ), f"Target is a feature service but insufficient login parameters supplied."
                if target['profile'] is not None:
                    targetGIS = GIS(profile=target['profile'])
                    target['password'] = keyring.get_password("arcgis_python_api_profile_passwords", target['profile'])
                    if target['password'] is None:
                        target['password'] = keyring.get_password(f"{target['profile']}@arcgis_python_api_profile_passwords", target['profile'])
                else:
                    targetGIS = GIS(
                        url=target['portalUrl'], username=target['username'], password=target['password'])
                assert(
                    targetGIS.users.me), f"Login attempt to target portal was unsuccessful. Check portal and credential parameters."
                target['gis'] = targetGIS
                target['layer'] = arcgis.features.FeatureLayer(
                    target["path"])
                
                try:
                    if "arcgis.com" in targetGIS.url:
                        portalUrlToUse = r"https://www.arcgis.com"
                    else:
                        portalUrlToUse = targetGIS.url
                    arcpyLogin = arcpy.SignInToPortal(portalUrlToUse, targetGIS.users.me.username, target['password'])
                    self.logger.debug({"topic":"LOGIN", "code": "SUCCESS", "message":f"Arcpy login to {targetGIS.url} with user {targetGIS.users.me.username} was successful."})
                except Exception as e:
                    err = buildErrorMessage(e)
                    print(err)
                    self.logger.error({"topic": "LOGIN", "code": "ERROR",
                                    "message": f"Arcpy login to {targetGIS.url} failed. Error: {err}"})
                    traceback.print_tb(e.__traceback__)
                    raise Exception("Arcpy login failed. Please troubleshoot login details and try again.")

                self.logger.debug({"topic": "LOGIN", "code": "COMPLETED",
                            "message": f"Logged into target portal: {targetGIS.url}, {targetGIS.users.me.username}"})

            target_exists = arcpy.Exists(target['path'])
            assert target_exists, f"Target did not exist: {target['path']}"
            
            target["describe"] = arcpy.da.Describe(target["path"])
            target["workspace"] = arcpy.da.Describe(
                target['describe'].get('path'))
            target['schema_type'] = target.get('schema_type') if target.get(
                'schema_type') is not None else "NO_TEST"
            target['field_mapping'] = target.get('field_mapping') if target.get(
                'field_mapping') is not None else ""

            schemaCheck = self.compareSchemas(source, target)
            if schemaCheck.get('match') is not True:
                self.logger.error({"topic": "SCHEMA", "code": "MISMATCH",
                            "message": f"Source fields not matching target: {schemaCheck.get('message')}"})
                return
            else:
                self.logger.debug({"topic": "SCHEMA","code":"PASS", "message":f"Schema check passed.", "source_dataset": source_dataset, "target_dataset": target_dataset})               

            source_dataset = str(source['path'])

            try:
                targetRefreshDataStart = time.perf_counter()
                record_count = None
                adds_count = None
                updates_count = None
                deletes_count = None
                finalMessage = ""
                success = 0
                target_dataset = str(target['path'])
                self.logger.debug({"topic": "TARGET", "code": "START",
                                    "message": target.get('method'), "source_dataset": source_dataset, "target_dataset": target_dataset})

                assert not (target['describe'].get('isVersioned') is True and target.get(
                    'method') == "TRUNCATE"), "Versioned target datasets cannot be truncated. Please use the COMPARE method."

                if target.get('method') == "TRUNCATE":
                    if "http" in target['path']:

                        ## Get ALL objects ids and add to the deletes object
                        objectIds_result = target['layer'].query(
                            where='1=1', return_ids_only=True)
                        objectid_fieldname = objectIds_result.get(
                            "objectIdFieldName")
                        objectIds = objectIds_result["objectIds"]
                        deletes = dict.fromkeys(objectIds, None) 


                        field_list = []
                        field_types_to_exclude = self.getFieldTypeExclusions()
                        field_types_to_exclude.append('OID')
                        field_names_to_omit = self.getFieldNameExclusions(target, source)

                        for field in target['describe'].get('fields'):
                            if field.type not in field_types_to_exclude and field.name.lower() not in field_names_to_omit:
                                field_list.append(field.name)
                        if target['describe'].get('dataType') == 'FeatureClass':
                            field_list.insert(0, "SHAPE@JSON")
                        fieldTypes = {}
                        for field in source['describe'].get("fields"):
                            fieldTypes[field.name] = field.type

                        count = 0
                        adds = []
                        with arcpy.da.SearchCursor(source['path'], field_list, where_clause='1=1') as sourceCursor:
                            for sourceRow in sourceCursor:
                                count += 1
                                geometry = {}
                                attributes = {}
                                for i, fieldName in enumerate(field_list):
                                    if fieldName == "SHAPE@JSON":
                                        geometry = json.loads(sourceRow[i])
                                    elif fieldTypes.get(fieldName) == "Date" and sourceRow[i] is not None:
                                        attributes[fieldName] = sourceRow[i].replace(
                                            tzinfo=timezone.utc)
                                    else:
                                        attributes[fieldName] = sourceRow[i]

                                adds.append(
                                    {"attributes": attributes, "geometry": geometry})

                        if target['gis'].properties.isPortal == True and "hosted" in target['path'].lower():
                            self.logger.debug({"message": f"The target is a hosted feature layer, converting all field names to lowercase.", "target_dataset": target_dataset})
                            for add in adds:
                                add["attributes"] = {k.lower(): v for k,v in add["attributes"].items()}

                        changes = {
                            "id_fieldname": "irrelevant",
                            "objectid_fieldname": objectid_fieldname,
                            "fieldList": field_list,
                            "spatialReference": "irrelevant",
                            "adds": adds,
                            "updates": {},
                            "deletes": deletes
                        }

                        # Process adds, deletes and updates
                        changesApplied = self.applyChanges(target, changes)
                        assert changesApplied == True, f"There was a problem encountered when applying changes to {target['path']}"

                        # Check final row counts
                        Row_Count_of_Source_Table = int(
                            arcpy.management.GetCount(in_rows=source['path'])[0])
                        Row_Count_of_Target_Table = target['layer'].query(
                            where='1=1', return_count_only=True)
                        record_count = Row_Count_of_Target_Table
                        if Row_Count_of_Source_Table == Row_Count_of_Target_Table:
                            finalMessage += f"Data successfully refreshed"
                            success = 1
                        else:
                            finalMessage += f"Refresh data finished but counts do not match. Source count: {Row_Count_of_Source_Table} Target count: {Row_Count_of_Target_Table}"
                    else:
                        # Target is a database connection to be truncated and all features copied back in
                        arcpy.management.TruncateTable(
                            in_table=target['path'])
                        arcpy.management.Append(inputs=[source['path']], target=target['path'], schema_type=target['schema_type'],
                                                field_mapping=target['field_mapping'], subtype="", expression="")

                        if target['workspace'].get('workspaceFactoryProgID') == "esriDataSourcesGDB.FileGDBWorkspaceFactory":
                            arcpy.Compact_management(
                                target['workspace'].get('catalogPath'))
                            self.logger.debug(
                                {"topic": "TARGET", "code": "COMPACTED", "message": f"{target['workspace'].get('catalogPath')}"})
                        elif target['workspace'].get('workspaceFactoryProgID') == "esriDataSourcesGDB.SdeWorkspaceFactory":
                            arcpy.AnalyzeDatasets_management(target['describe'].get('path'), "NO_SYSTEM", target['describe'].get(
                                'baseName'), "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")
                            self.logger.debug(
                                {"topic": "TARGET", "code": "ANALYZED", "message": f"{target['workspace'].get('catalogPath')}"})

                        Row_Count_of_Source_Table = int(
                            arcpy.management.GetCount(in_rows=source.get('path'))[0])
                        Row_Count_of_Target_Table = int(
                            arcpy.management.GetCount(in_rows=target.get('path'))[0])
                        record_count = Row_Count_of_Target_Table
                        if Row_Count_of_Source_Table == Row_Count_of_Target_Table:
                            finalMessage += f"Data successfully refreshed"
                            success = 1
                        else:
                            finalMessage += f"Refresh data finished but counts do not match. Source count: {Row_Count_of_Source_Table} Target count: {Row_Count_of_Target_Table}"
                else:
                    # Use the comparison method to update the target
                    assert params.get(
                        'id_fieldname') is not None, f"Comparison was requested but no id_fieldname was provided."

                    changes = self.doComparison(
                        target=target, source=source, id_fieldname=params.get('id_fieldname'))
                    assert changes is not None, f"Comparison of datasets failed to complete."

                    # Process adds, deletes and updates
                    changesApplied = self.applyChanges(target, changes)
                    assert changesApplied == True, f"There was a problem encountered when applying changes to {target['path']}"

                    adds_count = len(changes["adds"])
                    deletes_count = len(changes["deletes"].keys())
                    updates_count = len(changes["updates"].keys())

                    if target['workspace'].get('workspaceFactoryProgID') == "esriDataSourcesGDB.FileGDBWorkspaceFactory":
                        arcpy.Compact_management(
                            target['workspace'].get('catalogPath'))
                        self.logger.debug(
                            {"topic": "TARGET", "code": "COMPACTED", "message": f"{target['workspace'].get('catalogPath')}"})
                    elif target['workspace'].get('workspaceFactoryProgID') == "esriDataSourcesGDB.SdeWorkspaceFactory":
                        arcpy.AnalyzeDatasets_management(target['describe'].get('path'), "NO_SYSTEM", target['describe'].get(
                            'baseName'), "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")
                        self.logger.debug(
                            {"topic": "TARGET", "code": "ANALYZED", "message": f"{target['workspace'].get('catalogPath')}"})

                    Row_Count_of_Source_Table = int(
                        arcpy.management.GetCount(in_rows=source['path'])[0])
                    Row_Count_of_Target_Table = int(
                        arcpy.management.GetCount(in_rows=target['path'])[0])
                    record_count = Row_Count_of_Target_Table
                    if Row_Count_of_Source_Table == Row_Count_of_Target_Table:
                        finalMessage += f"Data successfully refreshed"
                        success = 1
                    else:
                        finalMessage += f"Refresh data finished but counts do not match. Source count: {Row_Count_of_Source_Table} Target count: {Row_Count_of_Target_Table}"

            except Exception as e:
                success = 0
                finalMessage = buildErrorMessage(e)
                traceback.print_tb(e.__traceback__)
            finally:
                targetRefreshDataExecutionTime = math.ceil(
                    time.perf_counter() - targetRefreshDataStart)
                logMessage = {"topic": "COMPLETED", "code": target.get('method'),
                                "message": finalMessage, "source_dataset": source_dataset, "target_dataset": target_dataset, "adds": adds_count, "updates": updates_count, "deletes": deletes_count, "success": success, "elapsed_time": targetRefreshDataExecutionTime, "record_count": record_count}
                if success == 1:
                    self.logger.info(logMessage)
                else:
                    self.logger.error(logMessage)

            return True

        except Exception as e:
            err = buildErrorMessage(e)
            self.logger.error({"topic": "REFRESH", "code": "ERROR",
                               "message": err, "source_dataset": source_dataset})
            traceback.print_tb(e.__traceback__)
        finally:
            refreshDataExecutionTime = math.ceil(
                time.perf_counter() - refreshDataStart)
            self.logger.debug({"topic": "TIMER", "code": "METRIC",
                               "message": f"refreshData Execution Time = {refreshDataExecutionTime} seconds", "source_dataset": source_dataset, "target_dataset": target_dataset, "elapsed_time": refreshDataExecutionTime})

    def deleteOldChangesets(self, path, num_days=7):
        try:
            assert path is not None, f"No path supplied for deleting changesets from"
            assert os.exists(
                path), f"Changeset path does not exist, no deletes attempted: {path}"

            now = time.time()
            for f in os.listdir(path):
                if os.stat(f).st_mtime < now - (num_days * 86400):
                    if os.path.isfile(f):
                        os.remove(os.path.join(path, f))
        except Exception as e:
            err = buildErrorMessage(e)
            self.logger.error({"topic": "CLEANUP", "code": "ERROR",
                               "message": f"Changeset path: {path} Error: {err}"})
            traceback.print_tb(e.__traceback__)


def buildErrorMessage(e):
    errorMessage = ""
    # Build and show the error message
    # If many arguments
    if (e.args):
        for i in range(len(e.args)):
            if (i == 0):
                errorMessage = str(e.args[i]).encode('utf-8').decode('utf-8')
            else:
                errorMessage = errorMessage + " " + \
                    str(e.args[i]).encode('utf-8').decode('utf-8')
    # Else just one argument
    else:
        errorMessage = str(e)
    return errorMessage.strip().replace("\n", " ").replace("\r", "").replace("'", "")[:1000]

# JSON Logger code taken from python-json-logger
# https://github.com/madzak/python-json-logger


# skip natural LogRecord attributes
# http://docs.python.org/library/logging.html#logrecord-attributes
RESERVED_ATTRS = (
    'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
    'funcName', 'levelname', 'levelno', 'lineno', 'module',
    'msecs', 'message', 'msg', 'name', 'pathname', 'process',
    'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName')


def merge_record_extra(record, target, reserved):
    """
    Merges extra attributes from LogRecord object into target dictionary
    :param record: logging.LogRecord
    :param target: dict to update
    :param reserved: dict or list with reserved keys to skip
    """
    for key, value in record.__dict__.items():
        # this allows to have numeric keys
        if (key not in reserved
            and not (hasattr(key, "startswith")
                     and key.startswith('_'))):
            target[key] = value
    return target


class JsonEncoder(json.JSONEncoder):
    """
    A custom encoder extending the default JSONEncoder
    """

    def default(self, obj):
        if isinstance(obj, (date, datetime, time)):
            return self.format_datetime_obj(obj)

        elif istraceback(obj):
            return ''.join(traceback.format_tb(obj)).strip()

        elif type(obj) == Exception \
                or isinstance(obj, Exception) \
                or type(obj) == type:
            return str(obj)

        try:
            return super(JsonEncoder, self).default(obj)

        except TypeError:
            try:
                return str(obj)

            except Exception:
                return None

    def format_datetime_obj(self, obj):
        return obj.isoformat()


class JsonFormatter(logging.Formatter):
    """
    A custom formatter to format logging records as json strings.
    Extra values will be formatted as str() if not supported by
    json default encoder
    """

    def __init__(self, *args, **kwargs):
        """
        :param json_default: a function for encoding non-standard objects
            as outlined in http://docs.python.org/2/library/json.html
        :param json_encoder: optional custom encoder
        :param json_serializer: a :meth:`json.dumps`-compatible callable
            that will be used to serialize the log record.
        :param json_indent: an optional :meth:`json.dumps`-compatible numeric value
            that will be used to customize the indent of the output json.
        :param prefix: an optional string prefix added at the beginning of
            the formatted string
        :param rename_fields: an optional dict, used to rename field names in the output.
            Rename message to @message: {'message': '@message'}
        :param static_fields: an optional dict, used to add fields with static values to all logs
        :param json_indent: indent parameter for json.dumps
        :param json_ensure_ascii: ensure_ascii parameter for json.dumps
        :param reserved_attrs: an optional list of fields that will be skipped when
            outputting json log record. Defaults to all log record attributes:
            http://docs.python.org/library/logging.html#logrecord-attributes
        :param timestamp: an optional string/boolean field to add a timestamp when
            outputting the json log record. If string is passed, timestamp will be added
            to log record using string as key. If True boolean is passed, timestamp key
            will be "timestamp". Defaults to False/off.
        """
        self.json_default = self._str_to_fn(kwargs.pop("json_default", None))
        self.json_encoder = self._str_to_fn(kwargs.pop("json_encoder", None))
        self.json_serializer = self._str_to_fn(
            kwargs.pop("json_serializer", json.dumps))
        self.json_indent = kwargs.pop("json_indent", None)
        self.json_ensure_ascii = kwargs.pop("json_ensure_ascii", True)
        self.prefix = kwargs.pop("prefix", "")
        self.rename_fields = kwargs.pop("rename_fields", {})
        self.static_fields = kwargs.pop("static_fields", {})
        reserved_attrs = kwargs.pop("reserved_attrs", RESERVED_ATTRS)
        self.reserved_attrs = dict(zip(reserved_attrs, reserved_attrs))
        self.timestamp = kwargs.pop("timestamp", False)

        # super(JsonFormatter, self).__init__(*args, **kwargs)
        logging.Formatter.__init__(self, *args, **kwargs)
        if not self.json_encoder and not self.json_default:
            self.json_encoder = JsonEncoder

        self._required_fields = self.parse()
        self._skip_fields = dict(zip(self._required_fields,
                                     self._required_fields))
        self._skip_fields.update(self.reserved_attrs)

    def _str_to_fn(self, fn_as_str):
        """
        If the argument is not a string, return whatever was passed in.
        Parses a string such as package.module.function, imports the module
        and returns the function.
        :param fn_as_str: The string to parse. If not a string, return it.
        """
        if not isinstance(fn_as_str, str):
            return fn_as_str

        path, _, function = fn_as_str.rpartition('.')
        module = importlib.import_module(path)
        return getattr(module, function)

    def parse(self):
        """
        Parses format string looking for substitutions
        This method is responsible for returning a list of fields (as strings)
        to include in all log messages.
        """
        standard_formatters = re.compile(r'\((.+?)\)', re.IGNORECASE)
        return standard_formatters.findall(self._fmt)

    def add_fields(self, log_record, record, message_dict):
        """
        Override this method to implement custom logic for adding fields.
        """
        for field in self._required_fields:
            if field in self.rename_fields:
                log_record[self.rename_fields[field]
                           ] = record.__dict__.get(field)
            else:
                log_record[field] = record.__dict__.get(field)
        log_record.update(self.static_fields)
        log_record.update(message_dict)
        merge_record_extra(record, log_record, reserved=self._skip_fields)

        if self.timestamp:
            key = self.timestamp if type(
                self.timestamp) == str else 'timestamp'
            log_record[key] = datetime.fromtimestamp(
                record.created, tz=timezone.utc)

    def process_log_record(self, log_record):
        """
        Override this method to implement custom logic
        on the possibly ordered dictionary.
        """
        return log_record

    def jsonify_log_record(self, log_record):
        """Returns a json string of the log record."""
        return self.json_serializer(log_record,
                                    default=self.json_default,
                                    cls=self.json_encoder,
                                    indent=self.json_indent,
                                    ensure_ascii=self.json_ensure_ascii)

    def serialize_log_record(self, log_record):
        """Returns the final representation of the log record."""
        return "%s%s" % (self.prefix, self.jsonify_log_record(log_record))

    def format(self, record):
        """Formats a log record and serializes to json"""
        message_dict = {}
        if isinstance(record.msg, dict):
            message_dict = record.msg
            record.message = None
        else:
            record.message = record.getMessage()
        # only format time if needed
        if "asctime" in self._required_fields:
            record.asctime = self.formatTime(record, self.datefmt)

        # Display formatted exception, but allow overriding it in the
        # user-supplied dict.
        if record.exc_info and not message_dict.get('exc_info'):
            message_dict['exc_info'] = self.formatException(record.exc_info)
        if not message_dict.get('exc_info') and record.exc_text:
            message_dict['exc_info'] = record.exc_text
        # Display formatted record of stack frames
        # default format is a string returned from :func:`traceback.print_stack`
        try:
            if record.stack_info and not message_dict.get('stack_info'):
                message_dict['stack_info'] = self.formatStack(
                    record.stack_info)
        except AttributeError:
            # Python2.7 doesn't have stack_info.
            pass

        try:
            log_record = OrderedDict()
        except NameError:
            log_record = {}

        self.add_fields(log_record, record, message_dict)
        log_record = self.process_log_record(log_record)

        return self.serialize_log_record(log_record)


class CustomJsonFormatter(JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(
            log_record, record, message_dict)
        if not log_record.get('log_datetime'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow().timestamp()
            log_record['log_datetime'] = now
        if log_record.get('levelname'):
            log_record['levelname'] = log_record['levelname'].upper()
        else:
            log_record['levelname'] = record.levelname


class ColoredFormatter(CustomJsonFormatter):
    """Colored log formatter."""

    def __init__(self, *args, colors: Optional[Dict[str, str]] = None, **kwargs) -> None:
        """Initialize the formatter with specified format strings."""
        super().__init__(*args, **kwargs)
        self.colors = colors if colors else {}

    def format(self, record) -> str:
        """Format the specified record as text."""
        record.color = self.colors.get(record.levelname, '')
        record.reset = Style.RESET_ALL
        msg = super().format(record)
        msg_dictionary = json.loads(msg)
        messageList = []
        log_datetime = msg_dictionary.get(
            "log_datetime", datetime.now(timezone.utc).timestamp())
        log_datetime = datetime.fromtimestamp(
            log_datetime).replace(tzinfo=timezone.utc)
        log_datetime = log_datetime.astimezone().strftime('%Y-%m-%d %H:%M:%S')

        messageList.append(msg_dictionary.get(
            "color", "")+log_datetime.ljust(27))
        messageList.append(msg_dictionary.get("levelname", "LEVEL").center(
            15, "_") + msg_dictionary.get("reset"))
        messageList.append(msg_dictionary.get(
            "topic", "TOPIC").strip()[:256].center(15))
        messageList.append(msg_dictionary.get(
            "code", "CODE").strip()[:25].center(10))
        messageList.append(str(msg_dictionary.get("metric", "-")).center(5))
        messageList.append(msg_dictionary.get(
            "message", "---").strip()[:1000].ljust(50))
        return "|".join(messageList)


class ArcGISHandler(logging.StreamHandler):
    def __init__(self, gis, table_url):
        logging.StreamHandler.__init__(self)
        self.gis = gis
        self.table_url = table_url
        self.log_table = arcgis.features.Table(self.table_url)

    def emit(self, record) -> None:
        """Add the record to the ArcGIS table"""
        try:
            msg = json.loads(self.format(record))
            log_datetime = msg.get(
                "log_datetime", datetime.now(timezone.utc).timestamp())
            log_datetime = datetime.fromtimestamp(
                log_datetime).replace(tzinfo=timezone.utc)
            log_datetime = log_datetime.astimezone()
            msg['log_datetime'] = log_datetime
            adds = [{"attributes": msg}]
            results = self.log_table.edit_features(adds=adds)
            success_check = set(res["success"]
                                for res in results["addResults"])
            if all(success_check) is not True:
                print("ERROR:  Log upload failed.....")
        except:
            self.handleError(record)


def configureLogging(logger_name=__name__, gis=None, log_table_url=None, logFolder=None):
    logger = logging.getLogger(logger_name)
    logger.handlers = []
    logger.setLevel(logging.DEBUG)
    jsonFormatter = CustomJsonFormatter(
        '%(log_datetime)s %(levelname)s %(topic)s %(code)s %(message)s %(metric)s')
    if logFolder is not None:
        directory = logFolder.strip()  # os.path.dirname(logFileName).strip()
        if len(directory) > 0:
            logFileName = "logfile"
            logFileWithExtension = os.path.join(directory, logFileName+".log")
            os.makedirs(directory, exist_ok=True)
            if (os.path.isfile(logFileWithExtension)):
                # If file is larger than 10MB
                if ((os.path.getsize(logFileWithExtension) / 1048576) > 10):
                    # Archive file
                    shutil.copy(logFileWithExtension, os.path.join(
                        directory, logFileName + "-" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log"))
                    with open(logFileWithExtension, "r+") as f:
                        f.truncate(0)
            file_logging_handler = logging.FileHandler(logFileWithExtension)
            file_logging_handler.setLevel(logging.DEBUG)
            file_logging_handler.setFormatter(jsonFormatter)
            logger.addHandler(file_logging_handler)
    if log_table_url is not None:
        arcgis_logging_handler = ArcGISHandler(gis, log_table_url)
        arcgis_logging_handler.setLevel(logging.INFO)
        arcgis_logging_handler.setFormatter(jsonFormatter)
        logger.addHandler(arcgis_logging_handler)

    coloredFormatter = ColoredFormatter(
        '',
        style='{', datefmt='%Y-%m-%d %H:%M:%S',
        colors={
            'DEBUG': Fore.CYAN,
            'INFO': Fore.GREEN,
            'WARNING': Fore.YELLOW,
            'ERROR': Fore.RED,
            'CRITICAL': Fore.RED + Back.WHITE + Style.BRIGHT,
        }
    )

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(coloredFormatter)
    consoleHandler.setLevel(logging.DEBUG)
    logger.addHandler(consoleHandler)
    return logger
