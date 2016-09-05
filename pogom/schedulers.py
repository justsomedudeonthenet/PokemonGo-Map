#!/usr/bin/python
# -*- coding: utf-8 -*-

# Schedulers determine how worker's queues get filled. They control which locations get scanned,
# in what order, at what time. This allows further optimizations to be easily added, without
# having to modify the existing overseer and worker thread code.
#
# Schedulers will recieve:
# - A list of queues for the workers they control.
# - A list of status dicts for the workers.  This will have information like last scan location and time
#   which the scheduler can use to make intelligent scheduling decisions
# - A modified version of args.  They will not recieve the full list of arguments, just the ones relevant
#   to this particular set of workers.
#
# Their job is to fill the queues with items for the workers to scan.
# Queue items are a list containing:
#   [step, (latitude, longitude, altitude), appears_seconds, disappears_seconds)]
# Where:
#   - step is the step number. Used only for display purposes.
#   - (latitude, longitude, altitude) is the location to be scanned.
#   - appears_seconds is the number of seconds after the start of the hour that the pokemon appears
#   - disappears_seconds is the number of seconds after the start of the hour that the pokemon disappears
#
#   appears_seconds and disappears_seconds are used to skip scans that are too late, and wait for scans the
#   worker is early for.  If a scheduler doesn't have a specific time a location needs to be scanned, it
#   should set both to 0.

import logging
import math
import geopy
from queue import Empty
from .transform import get_new_coords
from .models import hex_bounds, Pokemon

log = logging.getLogger(__name__)


# Simple base class that all other schedulers inherit from
# Most of these functions should be overridden in the actual scheduler classes.
# Not all scheduler methods will need to use all of the functions.
class BaseScheduler(object):
    def __init__(self, queues, status, args):
        self.queues = queues
        self.status = status
        self.args = args
        self.scan_location = False

    # schedule function fills the queues with data
    def schedule(self):
        pass

    # location_changed function is called whenever the location being scanned changes
    # scan_location = (lat, lng, alt)
    def location_changed(self, scan_location):
        self.scan_location = scan_location
        self.empty_queues()

    # scanning_pause function is called when scanning is paused from the UI
    # The default function will empty all the queues.
    # Note: This function is called repeatedly while scanning is paused!
    def scanning_paused(self):
        self.empty_queues()

    # scanning_unpause function is called when scanning is unpaused form the UI, before schedule is called
    def scanning_unpause(self):
        pass

    # Function to empty all queues in the queues list
    def empty_queues(self):
        for queue in self.queues:
            if not queue.empty():
                try:
                    while True:
                        queue.get_nowait()
                except Empty:
                    pass


# Hex Search is the classic search method, with the pokepath modification, searching in a hex grid around the center location
class HexSearch(BaseScheduler):

    # Call base initialization, set step_distance
    def __init__(self, queues, status, args):
        BaseScheduler.__init__(self, queues, status, args)

        # If we are only scanning for pokestops/gyms, the scan radius can be 900m.  Otherwise 70m
        if self.args.no_pokemon:
            self.step_distance = 0.900
        else:
            self.step_distance = 0.070

        self.step_limit = args.step_limit

        # This will hold the list of locations to scan so it can be reused, instead of recalculating on each loop
        self.locations = False

    # On location change, empty the current queue and the locations list
    def location_changed(self, scan_location):
        self.scan_location = scan_location
        self.empty_queues()
        self.locations = False

    # Generates the list of locations to scan
    def _generate_locations(self):
        NORTH = 0
        EAST = 90
        SOUTH = 180
        WEST = 270

        xdist = math.sqrt(3) * self.step_distance  # dist between column centers
        ydist = 3 * (self.step_distance / 2)       # dist between row centers

        results = []

        results.append((self.scan_location[0], self.scan_location[1], 0))

        if self.step_limit > 1:
            loc = self.scan_location

            # upper part
            ring = 1
            while ring < self.step_limit:

                loc = get_new_coords(loc, xdist, WEST if ring % 2 == 1 else EAST)
                results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, ydist, NORTH)
                    loc = get_new_coords(loc, xdist / 2, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, xdist, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, ydist, SOUTH)
                    loc = get_new_coords(loc, xdist / 2, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                ring += 1

            # lower part
            ring = self.step_limit - 1

            loc = get_new_coords(loc, ydist, SOUTH)
            loc = get_new_coords(loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
            results.append((loc[0], loc[1], 0))

            while ring > 0:

                if ring == 1:
                    loc = get_new_coords(loc, xdist, WEST)
                    results.append((loc[0], loc[1], 0))

                else:
                    for i in range(ring - 1):
                        loc = get_new_coords(loc, ydist, SOUTH)
                        loc = get_new_coords(loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    for i in range(ring):
                        loc = get_new_coords(loc, xdist, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    for i in range(ring - 1):
                        loc = get_new_coords(loc, ydist, NORTH)
                        loc = get_new_coords(loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    loc = get_new_coords(loc, xdist, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                ring -= 1

        # This will pull the last few steps back to the front of the list
        # so you get a "center nugget" at the beginning of the scan, instead
        # of the entire nothern area before the scan spots 70m to the south.
        if self.step_limit >= 3:
            if self.step_limit == 3:
                results = results[-2:] + results[:-2]
            else:
                results = results[-7:] + results[:-7]

        # Add the required appear and disappear times
        locationsZeroed = []
        for step, location in enumerate(results, 1):
            locationsZeroed.append((step, (location[0], location[1], 0), 0, 0))
        return locationsZeroed

    # Schedule the work to be done
    def schedule(self):
        if not self.scan_location:
            log.warning('Cannot schedule work until scan location has been set')
            return

        # Only generate the list of locations if we don't have it already calculated.
        if not self.locations:
            self.locations = self._generate_locations()

        for location in self.locations:
            # FUTURE IMPROVEMENT - For now, queues is assumed to have a single queue.
            self.queues[0].put(location)
            log.debug("Added location {}".format(location))


# Spawn Only Hex Search works like Hex Search, but skips locations that have no known spawnpoints
class SpawnOnlyHexSearch(HexSearch):

    def _any_spawnpoints_in_range(self, coords, spawnpoints):
        return any(geopy.distance.distance(coords, x).meters <= 70 for x in spawnpoints)

    # Extend the generate_locations function to remove locations with no spawnpoints
    def _generate_locations(self):
        n, e, s, w = hex_bounds(self.scan_location, self.step_limit)
        spawnpoints = set((d['latitude'], d['longitude']) for d in Pokemon.get_spawnpoints(s, w, n, e))

        if len(spawnpoints) == 0:
            log.warning('No spawnpoints found in the specified area!  (Did you forget to run a normal scan in this area first?)')

        # Call the original _generate_locations
        locations = super(SpawnOnlyHexSearch, self)._generate_locations()

        # Remove items with no spawnpoints in range
        locations = [coords for coords in locations if self._any_spawnpoints_in_range(coords[1], spawnpoints)]

        return locations


# Spawn Scan searches known spawnpoints at the specific time they spawn.
class SpawnScan(BaseScheduler):
    def __init__(self, queues, status, args):
        BaseScheduler.__init__(self, queues, status, args)
        # On the first scan, we want to search the last 15 minutes worth of spawns to get existing
        # pokemon onto the map.
        self.firstscan = True
