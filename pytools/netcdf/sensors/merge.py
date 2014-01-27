#!python
# coding=utf-8

import os

import netCDF4
import numpy as np

from .create import create_timeseries_file

import logging
logger = logging.getLogger("pytools")
logger.addHandler(logging.NullHandler())


def merge_timeseries(crawl_path, output_filename=None):

    if output_filename is None:
        output_filename = "merged.nc"
        logger.info("Setting output file to %s" % output_filename)

    for root, dirs, files in os.walk(crawl_path):
        try:
            # Make sure we are in a sensor directory
            assert os.path.basename(root).split(":")[2] == "sensor"
            ncfiles = [ncfile for ncfile in files if os.path.splitext(ncfile)[-1][0:3] == ".nc" and os.path.splitext(ncfile)[-1] != ".ncml" and ncfile != output_filename]
            # Make sure we have at least one NetCDF file in the directory
            assert len(ncfiles) > 0
            logger.warn("Merging %s" % root)
        except (IndexError, AssertionError):
            continue

        sensor_urn  = os.path.basename(root)
        station_urn = ":".join(sensor_urn.replace("sensor", "station").split(":")[0:-1])
        varname     = sensor_urn.split(":")[-1]
        fillvalue   = -9999.9

        times   = np.ma.asarray([])
        values  = np.ma.asarray([])
        verticals = np.ma.asarray([])
        lats    = []
        lons    = []
        # Track attributes from every file
        global_attributes   = {}
        variable_attributes = {}

        dims_of_values = None
        continue_on = False
        f = None
        for f in ncfiles:
            nc = netCDF4.Dataset(os.path.join(root, f))

            if dims_of_values is not None and dims_of_values != nc.variables[varname].ndim:
                logger.warn("Error with sensor: %s.  Different dimensions on the data variable between files" % sensor_urn)
                continue_on = True
                nc.close()
                break
            dims_of_values = nc.variables[varname].ndim

            # Update running global_attributes, overwriting any already existing key.
            global_attributes.update(nc.__dict__)
            # update running variable_attributes, overwriting any already existing key.
            variable_attributes.update(nc.variables[varname].__dict__)

            # This is generalized to work with both timeseries and timeseries profile.
            vardata = np.ma.ravel(nc.variables[varname][:])
            values  = np.ma.concatenate([values, vardata])
            ts      = nc.variables["time"][:]
            zs      = nc.variables["height"][:]

            # Location of the station
            lats.append(nc.variables["latitude"][:][0])
            lons.append(nc.variables["longitude"][:][0])

            # Repeat each time value by the number of verticals
            # This turns [1,2,3] into [1,1,1,2,2,2,3,3,3] if zs.size was three.
            # if zs.size is one, it just returns the ts array.
            times   = np.ma.concatenate([times, np.ma.repeat(ts, zs.size)])

            # Get actual verticals, repeat if necessary
            verticals = np.ma.concatenate([verticals, np.ma.ravel(np.ma.repeat([zs], ts.size, axis=0))])

            # Be sure we are on the right track...
            assert times.size == verticals.size == values.size

            nc.close()

        if continue_on:
            continue

        if len(list(set(lats))) > 1:
            logger.warn("%s : Some component files contained differing latitudes: %s.  Using the first." % (sensor_urn, lats))
        if len(list(set(lons))) > 1:
            logger.warn("%s : Some component files contained differing longitudes: %s.  Using the first." % (sensor_urn, lons))

        create_timeseries_file(root, lats[0], lons[0], station_urn, sensor_urn, global_attributes, variable_attributes, data=None, times=times, values=values, verticals=verticals, fillvalue=fillvalue, output_filename=output_filename)

        """
        if dims_of_values == 1:
            # Get all unique times (unique sorts them as well).  If there are duplicate times, this is
            # selecting just one of them.  No priority here, just whatever numpy choses (first occurance most likely)
            times, indices = np.unique(times, return_index=True)
            values = values[indices]
            verticals = verticals[indices]
        elif dims_of_values == 2:
            logger.debug("Getting unique time/vertical combinations")
            # Get all unique time/vertical combinations and select those.
            times_verticals  = np.ma.dstack((times, verticals))
            outer            = times_verticals.reshape(times.size, 2)

            # Unique rows in a numpy array
            # http://stackoverflow.com/questions/16970982/find-unique-rows-in-numpy-array
            void_to_unique = np.ascontiguousarray(outer).view(np.dtype((np.void, outer.dtype.itemsize * outer.shape[1])))
            _, indices  = np.unique(void_to_unique, return_index=True)
            times       = times[indices]
            verticals   = verticals[indices]
            values      = values[indices]

            # Now sort them
            indices     = np.lexsort((verticals,times))
            times       = times[indices]
            verticals   = verticals[indices]
            values      = values[indices]

        create_timeseries_file(root, lats[0], lons[0], station_urn, sensor_urn, global_attributes, variable_attributes, data=None, times=times, values=values, verticals=verticals, fillvalue=fillvalue, output_filename=output_filename)
        """
