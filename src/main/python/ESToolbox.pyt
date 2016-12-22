import arcpy
import json
import math
import traceback
import urllib
import urllib2
from elasticsearch import Elasticsearch

from hexcell import HexCell
from hexgrid import HexGrid


class Toolbox(object):
    def __init__(self):
        self.label = "ESToolbox"
        self.alias = "ES Toolbox"
        self.tools = [HexTool, QueryTool, CreateIndexTool]


class BaseTool(object):
    def __init__(self):
        self.RAD = 6378137.0
        self.RAD2 = self.RAD * 0.5
        self.LON = self.RAD * math.pi / 180.0
        self.D2R = math.pi / 180.0

    def lonToX(self, l):
        return l * self.LON

    def latToY(self, l):
        rad = l * self.D2R
        sin = math.sin(rad)
        return self.RAD2 * math.log((1.0 + sin) / (1.0 - sin))

    def deleteFC(self, fc):
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

    def getParamString(self, name="in", displayName="Label", value=""):
        param = arcpy.Parameter(
            name=name,
            displayName=displayName,
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param.value = value
        return param

    def getParamHost(self, displayName="Host", value="sandbox"):
        return self.getParamString(name="in_host", displayName=displayName, value=value)

    def getParamSize(self, displayName="Hex size in meters", value="1000"):
        return self.getParamString(name="in_size", displayName=displayName, value=value)

    def getParamName(self, displayName="Layer name", value="output"):
        return self.getParamString(name="in_name", displayName=displayName, value=value)

    def getParamPath(self, displayName="HDFS path", value="/user/hadoop/output"):
        return self.getParamString(name="in_path", displayName=displayName, value=value)

    def getParamFC(self):
        paramFC = arcpy.Parameter(
            name="outputFC",
            displayName="outputFC",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")
        # paramFC.symbology = "Z:/Share/population.lyr"
        return paramFC

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return


class HexTool(BaseTool):
    def __init__(self):
        super(HexTool, self).__init__()
        self.label = "Hex Density"
        self.description = "Calculate density based on hex cells"
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramCell = self.getParamSize(value="100")
        paramCell.filter.type = "ValueList"
        paramCell.filter.list = ["10", "100", "200", "500", "1K"]
        paramWhere = self.getParamString(name="in_where", displayName="Where", value="servicetypecode='SNOW'")
        paramName = self.getParamName(value="hex100")
        return [paramCell, paramWhere, paramName, self.getParamFC()]

    def execute(self, parameters, messages):
        arcpy.env.overwriteOutput = True
        try:
            cell_value = parameters[0].value
            if cell_value == "10":
                size = 10
                key = "loc_10"
            elif cell_value == "100":
                size = 100
                key = "loc_100"
            elif cell_value == "200":
                size = 200
                key = "loc_200"
            elif cell_value == "500":
                size = 500
                key = "loc_500"
            elif cell_value == "1K":
                size = 1000
                key = "loc_1K"

            hex_cell = HexCell(size=size)
            hex_grid = HexGrid(size=size)

            where = parameters[1].value
            name = parameters[2].value

            # fc = os.path.join(arcpy.env.scratchGDB, name)
            # ws = os.path.dirname(fc)

            ws = "in_memory"
            fc = ws + "/" + name

            spref = arcpy.SpatialReference(102100)

            arcpy.management.CreateFeatureclass(ws, name, "POLYGON", spatial_reference=spref)
            arcpy.management.AddField(fc, "POPULATION", "LONG")

            url = "http://mansour-mac:9200/_sql"
            params = {
                'sql': "SELECT " + key + " AS mykey,count(" + key + ") AS myval FROM dc/sr311 WHERE " + key +
                       " <> '0:0' AND " + where + " GROUP BY " + key + " LIMIT 20000"
            }
            data = urllib.urlencode(params)
            req = urllib2.Request(url + "?" + data)
            res = urllib2.urlopen(req)
            doc = json.load(res)
            with arcpy.da.InsertCursor(fc, ['SHAPE@', 'POPULATION']) as cursor:
                for bucket in doc['aggregations'][key]['buckets']:
                    col1 = bucket['key']
                    col2 = bucket['doc_count']
                    rc = col1.split(":")
                    r = float(rc[0])
                    c = float(rc[1])
                    xy = hex_grid.rc2xy(r, c)
                    x = float(xy[0])
                    y = float(xy[1])
                    cursor.insertRow([hex_cell.toShape(x, y), col2])
            del doc
            parameters[3].value = fc
        except:
            arcpy.AddMessage(traceback.format_exc())


class QueryTool(BaseTool):
    def __init__(self):
        super(QueryTool, self).__init__()
        self.label = "Query Tool"
        self.description = "Tool to query Elasticsearch using the SQL plugin rest engpoint"
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramWhere = self.getParamString(name="in_where", displayName="Where", value="*")
        paramName = self.getParamName(value="ServiceRequest")
        return [paramWhere, paramName, self.getParamFC()]

    def execute(self, parameters, messages):
        arcpy.env.overwriteOutput = True
        try:
            where = parameters[0].value
            if where == '*':
                where = ""
            if len(where) > 0:
                where = " AND " + where

            name = parameters[1].value

            # fc = os.path.join(arcpy.env.scratchGDB, name)
            # ws = os.path.dirname(fc)

            ws = "in_memory"
            fc = ws + "/" + name

            spref = arcpy.SpatialReference(102100)

            arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=spref)
            arcpy.management.AddField(fc, "SERVICE", "TEXT", field_length=64)
            arcpy.management.AddField(fc, "ZIPCODE", "TEXT", field_length=10)

            url = "http://mansour-mac:9200/_sql"
            params = {
                'sql': "SELECT loc_xm,loc_ym,servicecodedescription,zipcode FROM dc/sr311 WHERE loc_100 <> '0:0'" +
                       where + " LIMIT 100000"
            }
            data = urllib.urlencode(params)
            req = urllib2.Request(url + "?" + data)
            res = urllib2.urlopen(req)
            doc = json.load(res)
            with arcpy.da.InsertCursor(fc, ['SHAPE@XY', 'SERVICE', 'ZIPCODE']) as cursor:
                for hit in doc['hits']['hits']:
                    src = hit['_source']
                    x = float(src['loc_xm'])
                    y = float(src['loc_ym'])
                    service = src['servicecodedescription']
                    zipcode = src['zipcode']
                    cursor.insertRow([(x, y), service, zipcode])
            del doc
            parameters[2].value = fc
        except:
            arcpy.AddMessage(traceback.format_exc())


class CreateIndexTool(object):
    def __init__(self):
        self.label = "Create Index"
        self.description = "Create Elasticsearch index from JSON file"
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramName = arcpy.Parameter(
            name="in_host",
            displayName="ES Host",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        paramName.value = "mansour-mac"

        paramIndex = arcpy.Parameter(
            name="in_index",
            displayName="Index Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        paramIndex.value = "crime"

        paramFile = arcpy.Parameter(
            name="in_file",
            displayName="JSON file",
            direction="Input",
            datatype="DEFile",
            parameterType="Required")
        paramFile.value = r"Z:/Share/DCOCTO/crime.json"
        paramFile.filter.list = ['json']
        return [paramName, paramIndex, paramFile]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        with open(parameters[2].valueAsText, 'rb') as f:
            body = json.load(f)
            es = Elasticsearch(hosts=parameters[0].valueAsText)
            index = parameters[1].valueAsText
            if not es.indices.exists(index=index):
                es.indices.create(index=index, body=body)
            else:
                arcpy.AddWarning("{0} already exists !".format(index))
