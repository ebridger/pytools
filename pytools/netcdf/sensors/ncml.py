#!python
# coding=utf-8

import os
import sys
import json
import shutil
import argparse
from datetime import datetime
from collections import OrderedDict

import netCDF4
from lxml import etree

import logging
logger = logging.getLogger()
logger.addHandler(logging.NullHandler())


def create_ncml(output_path, output_filename=None, target_file=None):
    # Crawl output_path for sensor directories and create an NcML file in them
    ncml_files = []
    for root, dirs, files in os.walk(output_path):
        try:
            # Make sure we are in a sensor directory
            assert os.path.basename(root).split(":")[2] == "sensor"
            # Make sure we have at least one NetCDF file in the directory
            files = [ncfile for ncfile in files if os.path.splitext(ncfile)[-1][0:3] == ".nc" and os.path.splitext(ncfile)[-1][0:5] != ".ncml"]
            if target_file is not None and target_file not in files:
                logger.warn("No target file (%s) found in %s, skipping!", (target_file, root))
                continue
            assert len(files) > 0
        except (IndexError, AssertionError):
            # Not a sensor directory, or no NetCDF files in folder.  Keep moving!
            continue

        # Now we need to figure out the entire time duration for all NetCDF files so we can
        # update the new global attributes to the NcML file.
        starting = []
        ending   = []
        # Use the "target_file" if it is available
        if target_file is not None:
            nc = netCDF4.Dataset(os.path.join(root, target_file))
            starting.append(nc.variables["time"][0])
            ending.append(nc.variables["time"][-1])
            nc.close()
        else:
            for f in files:
                nc = netCDF4.Dataset(os.path.join(root, f))
                starting.append(nc.variables["time"][0])
                ending.append(nc.variables["time"][-1])
                nc.close()

        sensor_urn = os.path.basename(root)
        starting   = datetime.utcfromtimestamp(min(starting))
        ending     = datetime.utcfromtimestamp(max(ending))
        
	outfile = output_filename
        if outfile is None:
            outfile = "%s.ncml" % sensor_urn

        with open(os.path.join(root, outfile), "w") as ncml:
            ncml.write(get_ncml_text(root, sensor_urn, starting, ending))
            ncml_files.append(os.path.join(root, outfile))
            logger.info("Finished writing: %s" % os.path.join(root, outfile))

    # Write out a list of NCML files
    with open("ncml_files.json", "w") as ncmlout:
        ncmlout.write(json.dumps(ncml_files))


def get_ncml_text(path, sensor_urn, starting, ending):

    duration = "P%sS" % unicode(int(round((ending - starting).total_seconds())))
    station_urn = ":".join(sensor_urn.replace("sensor", "station").split(":")[0:-1])
    fillvars = { 'sensor_urn'   : sensor_urn,
                 'station_urn'  : station_urn,
                 'station_uid'  : station_urn.split(":")[-1],
                 'naming_auth'  : station_urn.split(":")[3],
                 'starting'     : starting.strftime("%Y-%m-%dT%H:%M:00Z"),
                 'ending'       : ending.strftime("%Y-%m-%dT%H:%M:00Z"),
                 'duration'     : duration,
                 'now'          : datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00Z"),
                 'path'         : path }

    text = """
        <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" location="merged.nc">

            <attribute name="id" value="%(station_uid)s" />
            <attribute name="naming_authority" value="%(naming_auth)s" />
            <attribute name="time_coverage_start" value="%(starting)s" />
            <attribute name="time_coverage_end" value="%(ending)s" />
            <attribute name="time_coverage_duration" value="%(duration)s" />
            <attribute name="date_created" value="%(now)s" />

            <variable name="time">
                <attribute name="calendar" value="gregorian" />
            </variable>

            <variable name="feature_type_instance">
                <values>%(station_urn)s</values>
            </variable>

            <variable name="instrument">
                <attribute name="long_name" type="string" value="%(sensor_urn)s" />
            </variable>

            <variable name="platform">
                <attribute name="ioos_code" type="string" value="%(station_urn)s" />
                <attribute name="short_name" type="string" value="%(station_uid)s" />
                <attribute name="long_nane" type="string" value="%(station_urn)s" />
            </variable>

            <!--aggregation dimName="time" type="joinExisting" timeUnitsChange="false">
                <scan location="." suffix=".nc"/>
            </aggregation-->
        </netcdf>

        """ % fillvars

    # Normalize XML by passing through lxml
    return etree.tostring(etree.XML(unicode(text)), pretty_print=True, xml_declaration=True, encoding='UTF-8')
