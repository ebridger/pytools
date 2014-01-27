#!python
# coding=utf-8

import os
import json
from datetime import datetime

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
            ncml.write(get_ncml_text(root, sensor_urn, starting, ending, target_file))
            ncml_files.append(os.path.join(root, outfile))
            logger.info("Finished writing: %s" % os.path.join(root, outfile))

    # Write out a list of NCML files
    with open("ncml_files.json", "w") as ncmlout:
        ncmlout.write(json.dumps(ncml_files))


def get_ncml_text(path, sensor_urn, starting, ending, target_file):

    fillvars = { 'target_file' : target_file }
    text = """<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" location="%(target_file)s"></netcdf>""" % fillvars
    # Normalize XML by passing through lxml
    return etree.tostring(etree.XML(unicode(text)), pretty_print=True, xml_declaration=True, encoding='UTF-8')
