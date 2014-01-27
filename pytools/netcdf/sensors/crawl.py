#!python
# coding=utf-8

import os
import json
import shutil
from datetime import datetime
from collections import OrderedDict

import netCDF4

import logging
logger = logging.getLogger("pytools")
logger.addHandler(logging.NullHandler())


def crawl_and_copy(crawl_paths, authority_map, station_map, write_output=None, perform_copy=None, output_path=None):

    out = {}

    for p in crawl_paths:
        for root, dirs, files in os.walk(p):
            for f in files:
                if os.path.splitext(f)[-1][0:3] == ".nc":
                    filepath = os.path.join(root, f)
                    nc = netCDF4.Dataset(filepath)

                    sensor_urn   = nc.variables["instrument"].long_name
                    split_urn    = sensor_urn.split(":")
                    authority    = authority_map.get(split_urn[3], None) or split_urn[3]
                    varname      = split_urn[-1]

                    auth_station = "%s:%s" % (authority, split_urn[4])
                    new_auth     = station_map.get(auth_station, None) or auth_station

                    uid          = new_auth.split(":")[-1]
                    authority    = new_auth.split(":")[0]

                    # Now get the final sensor_urn, and make it lowercase
                    mapped_sensor_urn      = ":".join(split_urn[0:3] + [authority] + [uid] + [varname]).lower()
                    mapped_station_urn     = ":".join(split_urn[0:2] + ["station"] + [authority] + [uid]).lower()

                    latitude  = float(nc.variables["latitude"][:])
                    longitude = float(nc.variables["longitude"][:])
                    starting  = datetime.utcfromtimestamp(nc.variables["time"][0]).strftime("%Y-%m-%d %H:%M:%S")
                    ending    = datetime.utcfromtimestamp(nc.variables["time"][-1]).strftime("%Y-%m-%d %H:%M:%S")
                    nc.close()

                    if out.get(mapped_station_urn, None) is None:
                        out[mapped_station_urn] = {}

                    if out[mapped_station_urn].get(varname, None) is None:
                        out[mapped_station_urn][varname] = []

                    meta  = {  'file'          : filepath,
                               'start'         : starting,
                               'end'           : ending,
                               'lat'           : latitude,
                               'lon'           : longitude,
                               'mapped_station': mapped_station_urn,
                               'sensor_urn'    : sensor_urn,
                               'mapped_sensor' : mapped_sensor_urn }

                    out[mapped_station_urn][varname].append(meta)

                    if perform_copy is True and output_path is not None:
                        # Make destination if it doesn't exist
                        sensor_path = os.path.join(output_path, mapped_sensor_urn)
                        dest_path   = os.path.join(sensor_path, f)
                        if not os.path.exists(sensor_path):
                            os.makedirs(sensor_path)

                        logger.debug("Copying '%s' to '%s'." % (filepath, dest_path))
                        shutil.copy2(filepath, dest_path)

    # Sort by station
    out = OrderedDict(sorted(out.items(), key=lambda x: x[0]))

    if write_output is True:
        # Save to file for reference
        with open("sensor_files.json", "w") as f:
            f.write(json.dumps(out, sort_keys=False, indent=4, separators=(',', ' : ')))

    return out
