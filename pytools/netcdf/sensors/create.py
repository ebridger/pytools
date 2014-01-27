#!python
# coding=utf-8

import os
import bisect
from datetime import datetime

import netCDF4
import numpy as np

import logging
logger = logging.getLogger("pytools")
logger.addHandler(logging.NullHandler())


def create_timeseries_file(output_directory, latitude, longitude, full_station_urn, full_sensor_urn, global_attributes, attributes, data=None, times=None, verticals=None, values=None, fillvalue=-9999.9, output_filename=None):

    if data is not None:
        try:
            assert len(data) > 0
            # Data was passed in as a list of (time, vertical, value) tuples. Conver to numpy arrays.
            zipped    = zip(*data)
            times     = np.asarray(zipped[0]).astype(np.int)
            verticals = np.ma.asarray(zipped[1]).astype(np.float)
            values    = np.ma.asarray(zipped[2]).astype(np.float)
        except (AssertionError, IndexError):
            logger.warn("No data passed in for '%s'.  Skipping file creation" % full_sensor_urn)
            return

    verticals = np.ma.masked_values(verticals, fillvalue)
    values    = np.ma.masked_values(values, fillvalue)

    logger.debug("Getting unique time/vertical combinations")
    # Get all unique time/vertical combinations.
    times_verticals  = np.ma.dstack((times, verticals))
    outer            = times_verticals.reshape(times.size, 2)
    # Get unique time/vertical rows
    # http://stackoverflow.com/questions/16970982/find-unique-rows-in-numpy-array
    void_to_unique = np.ascontiguousarray(outer).view(np.dtype((np.void, outer.dtype.itemsize * outer.shape[1])))
    _, indices  = np.unique(void_to_unique, return_index=True)
    # Subset by the unique indices
    times       = times[indices]
    verticals   = verticals[indices]
    values      = values[indices]

    # Now sort them
    indices     = np.lexsort((verticals, times))
    times       = times[indices]
    verticals   = verticals[indices]
    values      = values[indices]

    assert times.size == verticals.size == values.size

    # Get unique time and verticals (the data used for the each variable)
    unique_times     = np.unique(times)
    unique_verticals = np.unique(verticals)

    used_values = None
    try:
        # These two cases should work for all but a few cases, which are caught below
        if unique_verticals.size == 0:
            used_values = np.ma.reshape(values, (unique_times.size))
        else:
            used_values = np.ma.reshape(values, (unique_times.size, unique_verticals.size))
    except ValueError:
        if unique_verticals.size > 1:
            # Try removing the null heights first.
            try:
                used_values = np.ma.reshape(values, (unique_times.size, unique_verticals.compressed().size))
                unique_verticals = unique_verticals.compressed()
            except ValueError:
                # Hmmm, we have two actual height values for this station.
                # Not cool man, not cool.
                # Reindex the entire values array.  This is slow.
                indexed = ((bisect.bisect_left(unique_times, times[i]), bisect.bisect_left(unique_verticals, verticals[i]), values[i]) for i in xrange(values.size))
                used_values = np.ndarray((unique_times.size, unique_verticals.size), dtype=float)
                used_values.fill(float(fillvalue))
                for (tzi, zzi, vz) in indexed:
                    used_values[tzi, zzi] = vz
        else:
            raise

    # Ain't got no data!
    if unique_times.size < 2:
        logger.error("Skipping: %s, no time!" % full_sensor_urn)
        return
    starting = datetime.utcfromtimestamp(unique_times[0])
    ending   = datetime.utcfromtimestamp(unique_times[-1])

    # Make directory
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    # Make file
    if output_filename is None:
        output_filename = "%s_TO_%s.nc" % (starting.strftime("%Y-%m-%dT%H:%MZ"), ending.strftime("%Y-%m-%dT%H:%MZ"))

    filepath = os.path.join(output_directory, output_filename)
    if os.path.exists(filepath):
        os.unlink(filepath)

    variable_name = full_sensor_urn.split(":")[-1]

    nc = netCDF4.Dataset(filepath, "w")
    logger.debug("Opened file for writing: %s" % filepath)

    # Globals
    # These are set by this script, we don't someone to be able to set them manually
    global_skips = ["time_coverage_start", "time_coverage_end", "time_coverage_duration", "time_coverage_resolution",
                    "featureType", "geospatial_vertical_positive", "geospatial_vertical_min", "geospatial_vertical_max",
                    "geospatial_vertical_resolution", "Conventions"]
    for k, v in global_attributes.iteritems():
        if v is None:
            v = "None"
        if k not in global_skips:
            nc.setncattr(k, v)

    nc.setncattr("Conventions", "CF-1.6")

    # Station name
    nc.createDimension("feature_type_instance", len(full_station_urn))
    name = nc.createVariable("feature_type_instance", "S1", ("feature_type_instance",))
    name.cf_role = "timeseries_id"
    name.long_name = "Identifier for each feature type instance"
    name[:] = list(full_station_urn)

    logger.debug("Setting up time...")
    # Time extents
    nc.setncattr("time_coverage_start",    starting.isoformat())
    nc.setncattr("time_coverage_end",      ending.isoformat())
    # duration (ISO8601 format)
    nc.setncattr("time_coverage_duration", "P%sS" % unicode(int(round((ending - starting).total_seconds()))))
    # resolution (ISO8601 format)
    # subtract adjacent times to produce an array of differences, then get the most common occurance
    diffs = unique_times[1:] - unique_times[:-1]
    uniqs, inverse = np.unique(diffs, return_inverse=True)
    time_diffs = diffs[np.bincount(inverse).argmax()]
    nc.setncattr("time_coverage_resolution", "P%sS" % unicode(int(round(time_diffs))))

    # Time - 32-bit unsigned integer
    nc.createDimension("time")
    time = nc.createVariable("time",    "f8", ("time",), chunksizes=(1000,))
    time.units          = "seconds since 1970-01-01T00:00:00Z"
    time.standard_name  = "time"
    time.long_name      = "time of measurement"
    time.calendar       = "gregorian"
    logger.debug("Setting data array...")
    time[:] = unique_times

    # Location
    lat = nc.createVariable("latitude", "f4")
    lat.units           = "degrees_north"
    lat.standard_name   = "latitude"
    lat.long_name       = "sensor latitude"
    lat[:] = latitude

    lon = nc.createVariable("longitude", "f4")
    lon.units           = "degrees_east"
    lon.standard_name   = "longitude"
    lon.long_name       = "sensor longitude"
    lon[:] = longitude

    # Metadata variables
    crs = nc.createVariable("crs", "i4")
    crs.long_name           = "http://www.opengis.net/def/crs/EPSG/0/4326"
    crs.grid_mapping_name   = "latitude_longitude"
    crs.epsg_code           = "EPSG:4326"
    crs.semi_major_axis     = float(6378137.0)
    crs.inverse_flattening  = float(298.257223563)

    platform = nc.createVariable("platform", "i4")
    platform.ioos_code      = full_station_urn
    platform.short_name     = global_attributes.get("title", full_station_urn)
    platform.long_name      = global_attributes.get("description", full_station_urn)

    instrument = nc.createVariable("instrument", "i4")
    instrument.definition   = "http://mmisw.org/ont/ioos/definition/sensorID"
    instrument.long_name    = full_sensor_urn

    # Sync file structure
    nc.sync()

    # The coordinates attribute.  This may get appended to below before being written to the sensor variable.
    coordinates = ["time", "height", "latitude", "longitude"]

    # Figure out if we are creating a Profile or just a TimeSeries
    if unique_verticals.size <= 1:
        # TIMESERIES
        nc.setncattr("featureType", "timeSeries")

        # Always create the height variable
        logger.debug("Setting up height...")
        z = nc.createVariable("height",     "f4", fill_value=fillvalue)
        z.long_name       = "height of the sensor relative to sea surface"
        z.standard_name   = "height"
        z.positive        = "down"
        z.units           = "m"
        z.axis            = "Z"
        logger.debug("Setting data array...")

        # Fill in variable if we have an actual height. Else, the fillvalue remains.
        if unique_verticals.size == 1:
            # Vertical extents
            nc.setncattr("geospatial_vertical_positive", "down")
            nc.setncattr("geospatial_vertical_min",      unique_verticals[0])
            nc.setncattr("geospatial_vertical_max",      unique_verticals[0])
            z[:] = unique_verticals

        # Sensor
        logger.debug("Setting values...")
        var = nc.createVariable(variable_name,    "f4", ("time",), fill_value=fillvalue, chunksizes=(1000,))
        # Set the variable attributes as passed in
        for k, v in attributes.iteritems():
            if k != '_FillValue':
                setattr(var, k, v)
        # Set 'coordinates' attribute
        setattr(var, "coordinates", " ".join(coordinates))
        setattr(var, "standard_name", variable_name)

        # Set data
        logger.debug("Setting data array...")
        var[:] = used_values

    elif unique_verticals.size > 1:
        # TIMESERIES PROFILE
        # Vertical extents
        minvertical    = float(np.min(unique_verticals))
        maxvertical    = float(np.max(unique_verticals))
        vertical_diffs = unique_verticals[1:] - unique_verticals[:-1]
        nc.setncattr("geospatial_vertical_positive",   "down")
        nc.setncattr("geospatial_vertical_min",        minvertical)
        nc.setncattr("geospatial_vertical_max",        maxvertical)
        nc.setncattr("geospatial_vertical_resolution", " ".join(map(unicode, list(vertical_diffs))))

        nc.setncattr("featureType", "timeSeriesProfile")
        # There is more than one vertical value for this variable, we need to create a vertical dimension
        logger.debug("Setting up height...")
        nc.createDimension("z", unique_verticals.size)
        z = nc.createVariable("height",     "f4", ("z", ), fill_value=fillvalue)
        z.long_name       = "height of the sensor relative to sea surface"
        z.standard_name   = "height"
        z.positive        = "down"
        z.units           = "m"
        z.axis            = "Z"
        logger.debug("Setting data array...")
        z[:] = unique_verticals

        # Sensor
        logger.debug("Setting up values...")
        var = nc.createVariable(variable_name,    "f4", ("time", "z",), fill_value=fillvalue, chunksizes=(1000, unique_verticals.size,))
        # Set the variable attributes as passed in
        for k, v in attributes.iteritems():
            if k != '_FillValue':
                setattr(var, k, v)
        # Set 'coordinates' attribute
        setattr(var, "coordinates", " ".join(coordinates))
        setattr(var, "standard_name", variable_name)

        # Set data
        logger.debug("Setting data array...")
        var[:] = used_values

    nc.close()
