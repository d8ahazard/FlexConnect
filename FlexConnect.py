from os.path import dirname
import sys
sys.path.append(dirname(__file__))
sys.path.append("./lib")

from scapy.all import *
import datetime
import json
import logging
import os
import pathlib
import sqlite3
import threading
import time
import xml.etree.ElementTree as elTree

import pychromecast
import requests
import xmltodict
from bottle import route, run, request, get, post
from data import Data
from flex_container import FlexContainer
from monitor import Monitor
from pychromecast.controllers.media import MediaController
from pychromecast.controllers.plex import PlexController

__author__ = 'digitalhigh'

Log = logging.getLogger('FlexConnect')
Log.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('./FlexConnect.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to Log
Log.addHandler(ch)
Log.addHandler(fh)
DICT = {}
data = Data()

META_TYPE_IDS = {
    1: "movie",
    2: "show",
    3: "season",
    4: "episode",
    8: "artist",
    9: "album",
    10: "track",
    12: "extra",
    13: "photo",
    15: "playlist",
    18: "collection"
}

TAG_TYPE_ARRAY = {
    1: "genre",
    4: "director",
    5: "writer",
    6: "actor"
}

META_XML_TAGS = {
    "movie": "Video",
    "episode": "Video",
    "track": "Track",
    "photo": "Photo",
    "show": "Directory",
    "season": "Directory",
    "album": "Directory",
    "actor": "Directory",
    "director": "Directory",
    "artist": "Directory",
    "genre": "Directory",
    "collection": "Directory",
    "playlist": "Playlist"
}

META_TYPE_NAMES = dict(map(reversed, META_TYPE_IDS.items()))

DEFAULT_CONTAINER_SIZE = 100000
DEFAULT_CONTAINER_START = 0
DATE_STRUCTURE = "%Y-%m-%d %H:%M:%S"

DICT['version'] = '1.1.107'
NAME = 'Flex TV'
VERSION = '1.1.107'
APP_PREFIX = '/cast'
CAST_PREFIX = '/chromecast'
STAT_PREFIX = '/stats'
ICON = './Resources/flextv.png'
ICON_CAST = './Resources/icon-cast.png'
ICON_CAST_AUDIO = './Resources/icon-cast_audio.png'
ICON_CAST_VIDEO = './Resources/icon-cast_video.png'
ICON_CAST_GROUP = './Resources/icon-cast_group.png'
ICON_CAST_REFRESH = './Resources/icon-cast_refresh.png'
ICON_PLEX_CLIENT = './Resources/icon-plex_client.png'
TEST_CLIP = './Resources/test.mp3'
PLUGIN_IDENTIFIER = "com.plexapp.plugins.FlexTV"


####################################

def cache_timer():
    sec = 600
    while True:
        time.sleep(sec)
        Log.debug("Cache timer started, updatings in 6 minutes, man")
        update_cache()


def update_cache():
    Log.debug("UpdateCache called")
    if data.Exists('last_cache'):
        last_scan = float(data.Load('last_cache'))
        now = float(time.time())
        if now > last_scan:
            time_diff = now - last_scan
            time_mins = time_diff / 60
            if time_mins > 10:
                Log.debug("Scanning devices, it's been %s minutes since our last scan." % time_mins)
                scan_devices()
            else:
                Log.debug("Devices will be re-cached in %s minutes" % round(10 - time_mins))
        else:
            time_diff = last_scan - now
            time_mins = 10 - round(time_diff / 60)
            Log.debug("Device scan set for %s minutes from now." % time_mins)

        Log.debug("Diffs are %s and %s and %s." % (last_scan, now, time_diff))

    else:
        scan_devices()


####################################
# These are our cast endpoints
@route(APP_PREFIX + '/devices')
@route(CAST_PREFIX + '/devices')
def devices():
    """

    Endpoint to scan LAN for cast devices
    """
    Log.debug('Fetchings /devices endpoint.')
    # Grab our response header?
    casts = fetch_devices()
    mc = FlexContainer()
    for cast in casts:
        Log.debug("Cast type is " + cast['type'])
        if (cast['type'] == 'cast') | (cast['type'] == 'audio') | (cast['type'] == 'group'):
            dc = FlexContainer("Device", cast, show_size=False)
            mc.add(dc)

    return mc.content()


@route(APP_PREFIX + '/clients')
@route(CAST_PREFIX + '/clients')
def clients():
    """
    Endpoint to scan LAN for cast devices
    """
    Log.debug('Recieved a call to fetch all devices')
    # Grab our response header?
    casts = fetch_devices()

    mc = FlexContainer()
    for cast in casts:
        dc = FlexContainer("Device", cast, show_size=False)
        mc.add(dc)

    return mc.content()


@route(APP_PREFIX + '/resources')
@route(CAST_PREFIX + '/resources')
def resources():
    """
    Endpoint to scan LAN for cast devices
    """
    Log.debug('Received a call to fetch devices')
    # Grab our response header?
    casts = fetch_devices()

    oc = FlexContainer()

    for cast in casts:
        cast_type = cast['type']
        icon = ICON_CAST
        if cast_type == "audio":
            icon = ICON_CAST_AUDIO
        if cast_type == "cast":
            icon = ICON_CAST_VIDEO
        if cast_type == "group":
            icon = ICON_CAST_GROUP
        if cast['app'] == "Plex Client":
            icon = ICON_PLEX_CLIENT
        dir_dict = {
            "title": cast['name'],
            "duration": cast['status'],
            "tagline": cast['uri'],
            "summary": cast['app'],
            "thumb": icon
        }
        do = FlexContainer("Device", dir_dict)

        oc.add(do)

    return oc.content()


@get('/test')
def test_connection():
    return 'Success'


@get('/')
@get('/config')  # or @route('/config')
def config():
    uri = ''
    token = ''
    path = ''
    if data.Exists('uri'):
        uri = data.Load('uri')
    if data.Exists('path'):
        path = data.Load('path')
        Log.debug("Path is " + path)
    if data.Exists('token'):
        token = data.Load('token')
    return '''
        <form action="/config" method="post">
            Server URI: <input name="uri" type="text" value="%s" />
            Server Token: <input name="token" type="text" value="%s" />
            DB Path: <input name="path" type="text" value="%s" />
            <input value="Save" type="submit" />
        </form>
    ''' % (uri, token, path)


@post('/config')  # or @route('/login', method='POST')
def set_config():
    uri = request.forms.get('uri')
    token = request.forms.get('token')
    path = request.forms.get('path')
    data.Save('uri', uri)
    data.Save('token', token)
    data.Save('path', path)
    return "<p>Config saved.</p>"


@route(APP_PREFIX + '/rescan')
@route(CAST_PREFIX + '/rescan')
def rescan():
    """
    Endpoint to scan LAN for cast devices
    """
    Log.debug('Recieved a call to rescan devices')
    # Grab our response header?
    # UpdateCache()
    return True


@route(CAST_PREFIX + '/play')
def play():
    """
    Endpoint to play media.
    """
    Log.debug('Recieved a call to play media.')
    params = ['Clienturi', 'Contentid', 'Contenttype', 'Serverid', 'Serveruri',
              'Username', 'Transienttoken', 'Queueid', 'Version', 'Primaryserverid',
              'Primaryserveruri', 'Primaryservertoken']
    values = sort_headers(params, False)
    play_status = "Missing required headers and stuff"
    msg = play_status

    if values is not False:
        Log.debug("Holy crap, we have all the headers we need.")
        client_uri = values['Clienturi'].split(':')
        host = client_uri[0]
        port = int(client_uri[1])
        pc = False
        msg = "No message received"
        if 'Serverid' in values:
            servers = fetch_servers()
            for server in servers:
                if server['id'] == values['Serverid']:
                    Log.debug("Found a matching server!")
                    values['Serveruri'] = server['uri']
                    values['Version'] = server['version']

        try:
            cast = pychromecast.Chromecast(host, port)
            cast.wait()
            values['Type'] = cast.cast_type
            pc = PlexController(cast)
            cast.register_handler(pc)
            Log.debug("Sending values to play command: " + json.dumps(values))
            pc.play_media(values, log_data)
        except (pychromecast.LaunchError, pychromecast.PyChromecastError):
            Log.debug('Error connecting to host.')
            play_status = "Error"
        finally:
            if pc is not False:
                play_status = "Success"

    oc = FlexContainer(attributes={
        'Name': 'Playback Status',
        'Status': play_status,
        'Message': msg
    })

    return oc


@route(CAST_PREFIX + '/cmd')
def cast_cmd():
    """
    Media control command(s).

    Plex-specific commands use the format:


    Required params:
    Uri
    Cmd
    Vol(If setting volume, otherwise, ignored)

    Where <COMMAND> is one of:
    PLAY (resume)
    PAUSE
    STOP
    STEPFORWARD
    STEPBACKWARD Need to test, not in PHP cast app)
    PREVIOUS
    NEXT
    MUTE
    UNMUTE
    VOLUME - also requires an int representing level from 0-100

    """
    Log.debug('Recieved a call to control playback')
    params = sort_headers(['Uri', 'Cmd', 'Val'], False)
    response = "Error"

    if params is not False:
        uri = params['Uri'].split(":")
        cast = pychromecast.Chromecast(uri[0], int(uri[1]))
        cast.wait()
        pc = PlexController(cast)
        Log.debug("Handler namespace is %s" % pc.namespace)
        cast.register_handler(pc)

        Log.debug("Handler namespace is %s" % pc.namespace)

        cmd = params['Cmd']
        Log.debug("Command is " + cmd)

        if cmd == "play":
            pc.play()
        if cmd == "pause":
            pc.pause()
        if cmd == "stop":
            pc.stop()
        if cmd == "next":
            pc.next()
        if (cmd == "offset") & ('Val' in params):
            pc.seek(params["Val"])
        if cmd == "previous":
            pc.previous()
        if cmd == "volume.mute":
            pc.mute(True)
        if cmd == "volume.unmute":
            pc.mute(False)
        if (cmd == "volume") & ('Val' in params):
            pc.set_volume(params["Val"])
        if cmd == "volume.down":
            pc.volume_down()
        if cmd == "volume.up":
            pc.volume_up()

        cast.disconnect()
        response = "Command successful"

    oc = FlexContainer()
    oc.set('response', response)
    return oc.content()


@route(CAST_PREFIX + '/audio')
def audio():
    """
    Endpoint to cast audio to a specific device.
    """

    Log.debug('Recieved a call to play an audio clip.')
    params = ['Uri', 'Path']
    values = sort_headers(params, True)
    status = "Missing required headers"
    if values is not False:
        Log.debug("Holy crap, we have all the headers we need.")
        client_uri = values['Uri'].split(":")
        host = client_uri[0]
        port = int(client_uri[1])
        path = values['Path']
        try:
            cast = pychromecast.Chromecast(host, port)
            cast.wait()
            mc = cast.media_controller
            mc.play_media(path, 'audio/mp3', )
        except pychromecast.LaunchError:
            Log.debug('Error connecting to host.')
        finally:
            Log.debug("We have a cast")
            status = "Playback successful"

    oc = FlexContainer()
    oc.set('status', status)

    return oc.content


@route(CAST_PREFIX + '/broadcast/test')
def test():
    values = {'Path': "./Resources/test.mp3"}
    casts = fetch_devices()
    status = "Test successful!"
    try:
        for cast in casts:
            if cast['type'] == "audio":
                mc = MediaController()
                Log.debug("We should be broadcasting to " + cast['name'])
                uri = cast['uri'].split(":")
                cast = pychromecast.Chromecast(uri[0], int(uri[1]))
                cast.wait()
                cast.register_handler(mc)
                mc.play_media(values['Path'], 'audio/mp3')

    except (pychromecast.LaunchError, pychromecast.PyChromecastError):
        Log.debug('Error connecting to host.')
        status = "Test failed!"
    finally:
        Log.debug("We have a cast")

    oc = FlexContainer()
    oc.set('title', status)

    return oc.content()


@route(CAST_PREFIX + '/broadcast')
def broadcast():
    """
    Send audio to *all* cast devices on the network
    """
    Log.debug('Recieved a call to broadcast an audio clip.')
    params = ['Path']
    values = sort_headers(params, True)
    if values is not False:
        do = False
        casts = fetch_devices()
        disconnect = []
        controllers = []
        try:
            for cast in casts:
                if cast['type'] == "audio":
                    mc = MediaController()
                    Log.debug("We should be broadcasting to " + cast['name'])
                    uri = cast['uri'].split(":")
                    cast = pychromecast.Chromecast(uri[0], int(uri[1]))
                    cast.wait()
                    cast.register_handler(mc)
                    controllers.append(mc)
                    disconnect.append(cast)

            for mc in controllers:
                mc.play_media(values['Path'], 'audio/mp3', )

        except (pychromecast.LaunchError, pychromecast.PyChromecastError):
            Log.debug('Error connecting to host.')
        finally:
            for cast in disconnect:
                cast.disconnect()
            Log.debug("We have a cast")

    else:
        do = FlexContainer()
        do.set('title', 'Test Broadcast')

    oc = FlexContainer()
    oc.set("Title", "Foo")

    if do is not False:
        oc.add(do)

    return oc.content()


####################################
# These are our /stat prefixes
@route(STAT_PREFIX + '/tag')
def stat_tag_all():
    mc = build_tag_container("all")
    return mc.content()


@route(STAT_PREFIX + '/tag/actor')
def stat_tag_actor():
    mc = build_tag_container("actor")
    return mc.content()


@route(STAT_PREFIX + '/tag/director')
def stat_tag_director():
    mc = build_tag_container("director")
    return mc.content()


@route(STAT_PREFIX + '/tag/writer')
def stat_tag_writer():
    mc = build_tag_container("writer")
    return mc.content()


@route(STAT_PREFIX + '/tag/genre')
def stat_tag_genre():
    mc = build_tag_container("genre")
    return mc.content()


@route(STAT_PREFIX + '/tag/country')
def stat_tag_country():
    mc = build_tag_container("country")
    return mc.content()


@route(STAT_PREFIX + '/tag/mood')
def stat_tag_mood():
    mc = build_tag_container("mood")
    return mc.content()


@route(STAT_PREFIX + '/tag/autotag')
def stat_tag_autotag():
    mc = build_tag_container("autotag")
    return mc.content()


@route(STAT_PREFIX + '/tag/collection')
def stat_tag_collection():
    mc = build_tag_container("collection")
    return mc.content()


@route(STAT_PREFIX + '/tag/similar')
def stat_tag_similar():
    mc = build_tag_container("similar")
    return mc.content()


@route(STAT_PREFIX + '/tag/year')
def stat_tag_year():
    mc = build_tag_container("year")
    return mc.content()


@route(STAT_PREFIX + '/tag/contentRating')
def stat_tag_content_rating():
    mc = build_tag_container("contentRating")
    return mc.content()


@route(STAT_PREFIX + '/tag/studio')
def stat_tag_studio():
    mc = build_tag_container("studio")
    return mc.content()


# Rating (Reviews)
@route(STAT_PREFIX + '/tag/score')
def stat_tag_score():
    mc = build_tag_container("score")
    return mc.content()


@route(STAT_PREFIX + '/library')
def stat_library():
    mc = FlexContainer()
    Log.debug("Here's where we fetch some library stats.")
    sections = {}
    recs = query_library_stats()
    sizes = query_library_sizes()
    records = recs[0]
    sec_counts = recs[1]
    for record in records:
        section = record["sectionTitle"]
        if section not in sections:
            sections[section] = []
        del (record["sectionTitle"])
        sections[section].append(dict(record))

    for name in sections:
        Log.debug("Looping through section '%s'" % name)
        sec_id = sections[name][0]["section"]
        sec_type = sections[name][0]["sectionType"]
        section_types = {
            1: "movie",
            2: "show",
            3: "music",
            4: "photo",
            8: "music",
            13: "photo"
        }
        if sec_type in section_types:
            sec_type = section_types[sec_type]

        item_count = 0
        play_count = 0
        playable_count = 0
        section_children = []
        for record in sections[name]:
            item_count += record["totalItems"]
            if record['playCount'] is not None:
                play_count += record['playCount']
            if record["type"] in ["episode", "track", "movie"]:
                playable_count = record["totalItems"]

            item_type = str(record["type"]).capitalize()
            record_data = {
                "totalItems": record["totalItems"]
            }
            vc = FlexContainer(item_type, record_data, False)

            if record["lastViewedAt"] is not None:
                last_item = {
                    "title": record['title'],
                    "grandparentTitle": record['grandparentTitle'],
                    "art": record['art'],
                    "thumb": record['thumb'],
                    "ratingKey": record['ratingKey'],
                    "lastViewedAt": record['lastViewedAt'],
                    "username": record['username'],
                    "userId": record['userId']
                }
                li = FlexContainer("lastViewed", last_item, False)
                vc.add(li)

            section_children.append(vc)

            section_data = {
                "title": name,
                "id": sec_id,
                "totalItems": item_count,
                "playableItems": playable_count,
                "playCount": play_count,
                "type": sec_type
            }

            for sec_size in sizes:
                if sec_size['section_id'] == sec_id:
                    Log.debug("Found a matching section size...foo")
                    section_data['mediaSize'] = sec_size['size']

            sec_unique_played = sec_counts.get(str(sec_id)) or None
            if sec_unique_played is not None:
                Log.debug("Hey, we got the unique count")
                section_data["watchedItems"] = sec_unique_played["viewedItems"]
            ac = FlexContainer("Section", section_data, False)
            for child in section_children:
                ac.add(child)
            mc.add(ac)

    return mc.content()


@route(STAT_PREFIX + '/library/growth')
def stat_library_growth():
    headers = sort_headers(["Interval", "Start", "End", "Type"])
    records = query_library_growth(headers)
    total_array = {}
    for record in records:
        dates = str(record["addedAt"])[:-9].split("-")

        year = str(dates[0])
        month = str(dates[1])
        day = str(dates[2])

        year_array = total_array.get(year) or {}
        month_array = year_array.get(month) or {}
        day_array = month_array.get(day) or []
        day_array.append(record)

        month_array[day] = day_array
        year_array[month] = month_array
        total_array[year] = year_array

    mc = FlexContainer()
    grand_total = 0
    types_all = {}
    for y in range(0000, 3000):
        y = str(y)
        year_total = 0
        if y in total_array:
            types_year = {}
            Log.debug("Found a year %s" % y)
            year_container = FlexContainer("Year", {"value": y})
            year_array = total_array[y]
            Log.debug("Year Array: %s" % json.dumps(year_array))
            month_total = 0
            os.environ['TZ'] = 'UTC'
            for m in range(1, 12):
                m = str(m).zfill(2)
                if m in year_array:
                    types_month = {}
                    Log.debug("Found a month %s" % m)
                    month_container = FlexContainer("Month", {"value": m})
                    month_array = year_array[m]
                    for d in range(1, 32):
                        d = str(d).zfill(2)
                        if d in month_array:
                            types_day = {}
                            Log.debug("Found a day %s" % d)
                            day_container = FlexContainer("Day", {"value": d}, False)
                            records = month_array[d]
                            for record in records:
                                record_type = record["type"]
                                record["addedAt"] = int(
                                    time.mktime(time.strptime(record["addedAt"], "%Y-%m-%d %H:%M:%S")))
                                tag_name = META_XML_TAGS.get(record_type) or "Undefined"
                                ac = FlexContainer(tag_name, record, False)
                                temp_day_count = types_day.get(record_type) or 0
                                temp_month_count = types_month.get(record_type) or 0
                                temp_year_count = types_year.get(record_type) or 0
                                temp_all_count = types_all.get(record_type) or 0
                                types_day[record_type] = temp_day_count + 1
                                types_month[record_type] = temp_month_count + 1
                                types_year[record_type] = temp_year_count + 1
                                types_all[record_type] = temp_all_count + 1
                                day_container.add(ac)
                            month_total += day_container.size()
                            day_container.set("totalAdded", day_container.size())
                            for rec_type in types_day:
                                day_container.set("%sCount" % rec_type, types_day.get(rec_type))
                            month_container.add(day_container)
                    year_total += month_total
                    month_container.set("totalAdded", month_total)
                    for rec_type in types_month:
                        month_container.set("%sCount" % rec_type, types_month.get(rec_type))
                    year_container.add(month_container)
            year_container.set("totalAdded", year_total)
            for rec_type in types_year:
                year_container.set("%sCount" % rec_type, types_year.get(rec_type))
            grand_total += year_total
            mc.add(year_container)
    return mc.content()


@route(STAT_PREFIX + '/library/popular')
def stat_library_popular():
    results = query_library_popular()
    mc = FlexContainer()
    for section in results:
        sc = FlexContainer('Hub', limit=True)
        sc.set('hubIdentifier', section)
        sc.set('title', section.capitalize())
        for record in results[section]:
            rec_type = record["type"]
            tag_type = META_XML_TAGS.get(rec_type) or "Undefined"
            rec_users = {}
            if "users" in record:
                rec_users = record["users"]
                del record["users"]
                if "userName" in record:
                    del record["userName"]
                if "userId" in record:
                    del record["userId"]
            me = FlexContainer(tag_type, record, show_size=False)
            usc = FlexContainer("Users", show_size=False)
            view_total = 0
            for userName, userData in rec_users.items():
                vc = FlexContainer("Views")
                views = userData.get("views") or []
                views = sorted(views, key=lambda z: z['dateViewed'], reverse=True)
                if "views" in userData:
                    del userData["views"]
                uc = FlexContainer("User", userData, show_size=False)
                for view in views:
                    vsc = FlexContainer("View", view, show_size=False)
                    vc.add(vsc)
                uc.add(vc)
                uc.set("playCount", vc.size())
                view_total += vc.size()
                usc.add(uc)
            usc.set('userCount', usc.size())
            usc.set('playCount', view_total)
            me.add(usc)
            sc.add(me)
        mc.add(sc)

    return mc.content()


@route(STAT_PREFIX + '/library/quality')
def stat_library_quality():
    results = query_library_quality()
    mc = FlexContainer()
    Log.debug("Record: %s" % json.dumps(results))
    for meta_type, records in results.items():
        me = FlexContainer("Meta")
        me.set("Type", meta_type)
        records = results[meta_type]
        for record in records:
            mi = FlexContainer("Media", record, limit=True)
            me.add(mi)

        mc.add(me)

    return mc.content()


@route(STAT_PREFIX + '/system')
def stat_system():
    Log.debug("Querying system specs")
    headers = sort_headers(["Friendly"])
    friendly = headers.get("Friendly") or False
    mon = Monitor(friendly)
    mem_data = mon.get_memory()
    cpu_data = mon.get_cpu()
    hdd_data = mon.get_disk()
    net_data = mon.get_net()
    mc = FlexContainer(show_size=False)
    mem_container = FlexContainer("Mem", mem_data, show_size=False)
    cpu_container = FlexContainer("Cpu", cpu_data, show_size=False)
    hdd_container = FlexContainer("Hdd", show_size=False)
    for disk_item in hdd_data:
        dc = FlexContainer("Disk", disk_item, show_size=False)
        hdd_container.add(dc)
    net_container = FlexContainer("Net", show_size=False)
    for nic in net_data:
        if_container = FlexContainer("Interface", nic, show_size=False)
        net_container.add(if_container)

    mc.add(mem_container)
    mc.add(cpu_container)
    mc.add(hdd_container)
    mc.add(net_container)
    return mc.content()


@route(STAT_PREFIX + '/user')
def stat_user():
    users = query_user_stats()
    mc = FlexContainer()
    if users is not None:
        for user in users:
            user_meta = user['meta']
            user_devices = user['devices']
            del user['meta']
            del user['devices']
            uc = FlexContainer("User", user, False)
            sc = FlexContainer("Views", show_size=False)
            for meta, items in user_meta.items():
                vc = FlexContainer(meta, limit=True)
                for item in items:
                    tag_name = META_XML_TAGS.get(item['type']) or "Undefined"
                    ic = FlexContainer(tag_name, item, show_size=False)
                    vc.add(ic)

                sc.add(vc)
            uc.add(sc)
            chrome_data = None
            dp = FlexContainer("Devices", None, False, limit=True)

            for device in user_devices:
                if device["deviceName"] != "Chrome":
                    dc = FlexContainer("Device", device, False)
                    dp.add(dc)
                else:
                    chrome_bytes = 0
                    if chrome_data is None:
                        chrome_data = device
                    else:
                        chrome_bytes = device["totalBytes"] + chrome_data.get("totalBytes") or 0
                    chrome_data["totalBytes"] = chrome_bytes

            if chrome_data is not None:
                dc = FlexContainer("Device", chrome_data, False)
                dp.add(dc)
            uc.add(dp)
            mc.add(uc)

        Log.debug("Still alive, returning data")

        return mc.content()


@route("/stats/sessions")
def stat_sessions():
    """
    Fetch player status
    """
    show_all = True
    headers = sort_headers(["Clienturi", "Clientname"])
    uri = headers.get("Clienturi") or False
    name = headers.get("Clientname") or False
    if uri is not False | name is not False:
        show_all = False

    chromecasts = fetch_devices()
    devices_list = []
    cast_devices = []

    for chromecast in chromecasts:
        cast = False
        if show_all is not True:
            if chromecast['name'] == name:
                Log.debug("Found a matching chromecast: " + name)
                cast = chromecast

            if chromecast['uri'] == uri:
                Log.debug("Found a matching uri:" + uri)
                cast = chromecast
        else:
            cast = chromecast

        if cast is not False:
            if cast['type'] in ['cast', 'audio', 'group']:
                cast_devices.append(cast)
            else:
                devices_list.append(cast)

    session_statuses = get_session_status()

    mc = FlexContainer()

    if len(cast_devices):
        for device in cast_devices:
            uris = device['uri'].split(":")
            host = uris[0]
            port = uris[1]
            cast = False
            try:
                cast = pychromecast.Chromecast(host, int(port), timeout=3, tries=1)
            except pychromecast.ChromecastConnectionError:
                Log.error("Unable to connecct to device.")

            if cast:
                Log.debug("Waiting for devices.")
                cast.wait(2)
                app_id = cast.app_id
                meta_dict = False
                if app_id == "9AC194DC":
                    pc = PlexController(cast)
                    cast.register_handler(pc)
                    plex_status = pc.plex_status()
                    raw_status = {
                        'state': plex_status['state'],
                        'volume': plex_status['volume'],
                        'muted': plex_status['muted']
                    }
                    meta_dict = plex_status['meta']
                    if 'title' in meta_dict:
                        delements = []
                        i = 0
                        for session in session_statuses:
                            if (meta_dict['title'] == session['Video']['title']) & (host == session['address']):
                                delements.append(i)
                                meta_dict = session['Video']
                                del session['Video']
                                for key, value in session.items():
                                    raw_status[key] = value
                            i += 1
                        delements.reverse()
                        for rem in delements:
                            del session_statuses[rem]
                else:
                    raw_status = {"state": "idle"}

                del device['status']
                do = FlexContainer("Device", attributes=device, show_size=False)
                for key, value in raw_status.items():
                    do.set(key, value)
                if meta_dict:
                    md = FlexContainer("Meta", meta_dict, show_size=False)
                    do.add(md)
                mc.add(do)

    if len(devices_list):
        for device in devices_list:
            del device['status']
            do = FlexContainer("Device", attributes=device, show_size=False)
            meta_dict = False
            delements = []
            i = 0
            for session in session_statuses:
                if session['machineIdentifier'] == device['id']:
                    delements.append(i)
                    Log.debug("Session Match.")
                    meta_dict = session['Video']
                    del session['Video']
                    for key, value in session.items():
                        do.set(key, value)
                i += 1

            for delement in delements:
                del session_statuses[delement]

            if meta_dict:
                md = FlexContainer("Meta", meta_dict, show_size=False)
                do.add(md)
            else:
                do.set('state', "idle")
            mc.add(do)

    if len(session_statuses):
        for session in session_statuses:
            meta_dict = session['Video']
            del session['Video']
            so = FlexContainer("Device", attributes=session, show_size=False)
            md = FlexContainer("Meta", meta_dict, show_size=False)
            so.add(md)
            mc.add(so)

    return mc.content()


####################################
# These functions are for cast-related stuff
def fetch_devices():
    cast_list = []
    if not data.Exists('device_json'):
        Log.debug("No cached data exists, re-scanning.")
        cast_list = scan_devices()

    else:
        Log.debug("Returning cached data")
        casts_string = data.Load('device_json')
        cast_list = json.loads(casts_string)

    token = False
    for key, value in request.headers.items():
        Log.debug("Header key %s is %s", key, value)
        if key in ("X-Plex-Token", "Token"):
            Log.debug("We have a Token")
            token = value
    if token is False:
        if data.Exists('token'):
            token = data.Load('token')

    if token:
        uri = "http://localhost:32400"
        if data.Exists('uri'):
            uri = data.Load('uri')
        try:
            myurl = uri + "/clients?X-Plex-Token=" + token
        except TypeError:
            myurl = False
            pass

        if myurl:
            Log.debug("Gonna connect to %s" % myurl)
            req = requests.get(myurl)
            client_data = req.text
            root = elTree.fromstring(client_data)
            for device in root.iter('Server'):
                local_item = {
                    "name": device.get('name'),
                    "uri": device.get('host') + ":" + str(device.get('port')),
                    "status": "n/a",
                    "type": device.get('product'),
                    "app": "Plex Client",
                    "id": device.get('machineIdentifier')
                }
                cast_list.append(local_item)

    return cast_list


def fetch_servers():
    servers = []

    token = False
    for key, value in request.headers.items():
        Log.debug("Header key %s is %s", key, value)
        if key in ("X-Plex-Token", "Token"):
            Log.debug("We have a Token")
            token = value
    if token is False:
        if data.Exists('token'):
            token = data.Load('token')

    if token:
        uri = "http://localhost:32400"
        if data.Exists('uri'):
            uri = data.Load('uri')

        myurl = uri + "/clients?X-Plex-Token=" + token
        Log.debug("Gonna connect to %s" % myurl)
        req = requests.get(myurl)
        client_data = req.text
        root = elTree.fromstring(client_data)
        for device in root.iter('Server'):
            version = device.get("version").split("-")[0]
            local_item = {
                "name": device.get('name'),
                "uri": "http://" + device.get('host') + ":" + str(device.get('port')),
                "version": version,
                "id": device.get('machineIdentifier')
            }
            Log.debug("Got me a server: %s" % local_item)
            servers.append(local_item)

    return servers


def scan_devices():
    Log.debug("Re-fetching devices")
    casts = pychromecast.get_chromecasts(1, None, None, True)
    data_array = []
    for cast in casts:
        cast_item = {
            "uri": cast.uri,
            "name": cast.name,
            "status": cast.is_idle,
            "type": cast.cast_type,
            "app": cast.app_display_name,
            'id': cast.uri
        }
        data_array.append(cast_item)

    Log.debug("Cast length is %s", str(len(data_array)))
    Log.debug("Item count is " + str(len(data_array)))
    cast_string = json.dumps(data_array)
    data.Save('device_json', cast_string)
    last_cache = float(time.time())
    data.Save('last_cache', last_cache)
    return data_array


def player_string(values):
    request_id = values['Requestid']
    content_id = values['Contentid'] + '?own=1&window=200'  # key
    content_type = values['Contenttype']
    offset = values['Offset']
    server_id = values['Serverid']
    transcoder_video = values['Transcodervideo']
    server_uri = values['Serveruri'].split("://")
    server_parts = server_uri[1].split(":")
    server_protocol = server_uri[0]
    server_ip = server_parts[0]
    server_port = server_parts[1]
    username = values['Username']
    true = "true"
    false = "false"
    request_array = {
        "type": 'LOAD',
        'requestId': request_id,
        'media': {
            'contentId': content_id,
            'streamType': 'BUFFERED',
            'contentType': content_type,
            'customData': {
                'offset': offset,
                'directPlay': true,
                'directStream': true,
                'subtitleSize': 100,
                'audioBoost': 100,
                'server': {
                    'machineIdentifier': server_id,
                    'transcoderVideo': transcoder_video,
                    'transcoderVideoRemuxOnly': false,
                    'transcoderAudio': true,
                    'version': '1.4.3.3433',
                    'myPlexSubscription': true,
                    'isVerifiedHostname': true,
                    'protocol': server_protocol,
                    'address': server_ip,
                    'port': server_port,
                    'user': {
                        'username': username
                    }
                },
                'containerKey': content_id
            },
            'autoplay': true,
            'currentTime': 0
        }
    }
    Log.debug("Player String: " + json.dumps(request_array))

    return request_array


####################################
# These functions are for stats stuff
def build_tag_container(selection):
    headers = sort_headers(["Type", "Section", "Include-Meta", "Meta-Size"])
    tag_options = ["actor", "director", "writer", "genre", "country", "mood", "similar", "autotag", "collection"]
    meta_options = ["year", "contentRating", "studio", "score"]
    records = []
    if selection in tag_options:
        records = query_tag_stats(selection, headers)
    if selection in meta_options:
        records = query_meta_stats(selection, headers)
    if selection == "all":
        records = query_tag_stats(selection, headers)
        records2 = query_meta_stats(selection, headers)
        records += records2

    Log.debug("We have a total of %s records to process" % len(records))
    media_container = FlexContainer()
    add_meta = False
    if "Include-Meta" in headers:
        add_meta = headers["Include-Meta"].capitalize()
    else:
        if "Meta-Size" in headers:
            add_meta = True
    Log.debug("Add meta is %s" % add_meta)
    for tag_type in records:
        tag_type_container = FlexContainer(tag_type["name"], limit=True)
        tags = tag_type["children"]
        for tag in tags:
            tag_container = FlexContainer(tag["type"], show_size=False)
            tag_container.set("title", tag["name"])
            tag_container.set("totalItems", tag["count"])
            metas = tag["children"]
            for meta in metas:
                meta_type_container = FlexContainer(meta["type"])
                meta_type_container.set("type", meta["name"])
                medias = meta["children"]
                medias = sorted(medias, key=lambda i: i['added'], reverse=True)
                item_count = len(medias)
                if "Meta-Size" in headers:
                    if len(medias) > headers["Meta-Size"]:
                        medias = medias[:headers["Meta-Size"]]
                    for media in medias:
                        media_item_container = FlexContainer("Media", media)
                        meta_type_container.add(media_item_container)
                tag_container.set(meta["name"] + "Count", item_count)
                if add_meta:
                    Log.debug("Adding meta!!")
                    tag_container.add(meta_type_container)

            tag_type_container.add(tag_container)
        media_container.add(tag_type_container)
    return media_container


def query_library_sizes():
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    results = []

    if cursor is not None:
        query = """SELECT sum(size), library_section_id, ls.name FROM media_items 
                    INNER JOIN library_sections AS ls
                    ON ls.id = library_section_id
                    GROUP BY library_section_id;"""

        for size, section_id, section_name in cursor.execute(query):
            dictz = {
                "size": size,
                "section_id": section_id,
                "section_name": section_name
            }
            results.append(dictz)

        close_connection(connection)

    return results


def query_users():
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    users = {}

    if cursor is not None:
        query = """SELECT name, id from accounts;"""

        for name, user_id in cursor.execute(query):
            users[str(user_id)] = name

        close_connection(connection)
    return users


def query_library_quality():
    headers = sort_headers(["Container-Start", "Container-Size", "Type", "Section", "Sort"])
    container_start = headers.get("Container-Start") or 0
    container_size = headers.get("Container-Size") or 1000
    entitlements = get_entitlements()
    query_limit = "LIMIT %s, %s" % (container_start, container_size)

    section = headers.get("Section") or False
    sort = headers.get("Sort") or "DESC"

    query_selector = "AND md.library_section_id in %s" % entitlements
    type_selector = "(1, 4, 10)"
    if "Type" in headers:
        meta_type = headers.get("Type")
        if meta_type in META_TYPE_NAMES:
            meta_type = META_TYPE_NAMES[meta_type]
        if int(meta_type) == meta_type:
            type_selector = "(%s)" % meta_type
    query_selector += " AND md.metadata_type IN %s" % type_selector
    if section:
        query_selector += " AND md.library_section_id == section"

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    results = {}

    if cursor is not None:
        query = """
            select md.title, md3.title as grandparentTitle, 
            md.id as ratingKey, mi.width, mi.height, mi.size as fileSize, mi.duration, mi.bitrate, mi.container, 
            mi.video_codec as videoCodec, mi.audio_codec as audioCodec, mi.display_aspect_ratio as aspectRatio,
            mi.frames_per_second as framesPerSecond, mi.audio_channels as audioChannels,
            md.library_section_id as sectionId, md.metadata_type as type,
            ls.name as sectionName from media_items as mi
            inner join metadata_items as md
            on mi.metadata_item_id = md.id
            left join metadata_items as md2
            on md.parent_id = md2.id
            left join metadata_items as md3
            on md2.parent_id = md3.id
            inner join library_sections as ls
            on md.library_section_id = ls.id
            where md.library_section_id is not null
            %s
            order by mi.width %s, mi.height %s, mi.bitrate %s, mi.audio_channels %s, md.title desc
            %s;

        """ % (query_selector, sort, sort, sort, sort, query_limit)

        Log.debug("Query is %s" % query)
        for row in cursor.execute(query):
            descriptions = cursor.description
            i = 0
            dictz = {}
            meta_type = "unknown"
            for title, foo in descriptions:
                value = row[i]
                if title == "ratingKey":
                    dictz["art"] = "/library/metadata" + str(value) + "/art"
                    dictz["thumb"] = "/library/metadata" + str(value) + "/thumb"

                dictz[title] = row[i]
                if title == "type":
                    meta_type = META_TYPE_IDS.get(value) or value
                    dictz[title] = meta_type

                i += 1
            meta_list = results.get(meta_type) or []
            if meta_type == "episode":
                dictz["banner"] = "/library/metadata/" + str(dictz["ratingKey"]) + "/banner/"
            meta_list.append(dictz)
            results[meta_type] = meta_list

        close_connection(connection)
    Log.debug("No, really, sssss    : %s" % json.dumps(results))
    return results


def query_tag_stats(selection, headers):
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]

    tag_names = {
        "genre": 1,
        "collection": 2,
        "director": 4,
        "writer": 5,
        "actor": 6,
        "country": 8,
        "autotag": 207,
        "mood": 300,
        "similar": 305
    }

    tag_ids = {
        1: "genre",
        2: "collection",
        4: "director",
        5: "writer",
        6: "actor",
        8: "country",
        207: "autotag",
        300: "mood",
        305: "similar"
    }

    if selection == "all":
        stringz = []
        for tag_name in tag_names:
            stringz.append("tags.tag_type = %s" % tag_names[tag_name])
        selector = "WHERE (%s)" % " OR ".join(stringz)
    else:
        if selection not in tag_names:
            return []

        tag_type = tag_names[selection]
        selector = "WHERE tags.tag_type = %s" % tag_type

    section = headers.get("Section") or False
    if section:
        selector += " AND library_section = %s" % section

    entitlements = get_entitlements()
    selector += " AND lib_id IN %s" % entitlements
    meta_type = headers.get("Type") or False

    if meta_type:
        meta_id = False
        if meta_type in META_TYPE_NAMES:
            meta_id = META_TYPE_NAMES[meta_type]

        if meta_id:
            selector += " AND mt.metadata_type = %s" % meta_id

    if cursor is not None:
        query = """SELECT tags.tag, tags.tag_type, mt.id, mt.title, lib.name as library_section, 
                    mt1.title as parent_title, mt2.title as grandparent_title, mt.metadata_type, mt.added_at, mt.year,
                    lib.id as lib_id FROM taggings
                    LEFT JOIN tags ON tags.id = taggings.tag_id
                    INNER JOIN metadata_items AS mt
                    ON taggings.metadata_item_id = mt.id
                    LEFT JOIN metadata_items AS mt1
                    on mt1.id = mt.parent_id
                    LEFT JOIN metadata_items AS mt2
                    on mt2.id = mt1.parent_id
                    INNER JOIN library_sections as lib 
                    on mt.library_section_id = lib.id
                    %s
                    ORDER BY tags.tag_type, mt.metadata_type, library_section, tags.tag;
                    """ % selector

        records = {}
        Log.debug("Query is '%s'" % query)

        for tag, tag_type, ratingkey, title, library_section, parent_title, \
                grandparent_title, meta_type, added_at, year, lib_id in cursor.execute(query):
            tag_title = title
            if tag_type in tag_ids:
                tag_title = tag_ids[tag_type]

            if meta_type in META_TYPE_IDS:
                meta_type = META_TYPE_IDS[meta_type]

            dicts = {
                "title": title,
                "ratingKey": ratingkey,
                "added": added_at,
                "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                "art": "/library/metadata/" + str(ratingkey) + "/art",
                "year": year,
                "section": library_section,
                "sectionId": lib_id
            }

            if parent_title != "":
                dicts["parentTitle"] = parent_title

            if grandparent_title != "":
                dicts["grandparentTitle"] = grandparent_title

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(ratingkey) + "/banner/"

            tag_types = {}
            if tag_title in records:
                tag_types = records[tag_title]

            tags = {}
            if tag in tag_types:
                tags = tag_types[tag]

            meta_types = []
            if meta_type in tags:
                meta_types = tags[meta_type]

            meta_types.append(dicts)
            tags[meta_type] = meta_types
            tag_types[tag] = tags
            records[tag_title] = tag_types

        close_connection(connection)

        results = []

        tag_type_count = 0
        for tag_type in records:
            tags = records[tag_type]
            tag_list = []
            tag_count = 0
            for tag in tags:
                meta_types = tags[tag]
                meta_type_list = []
                meta_count = 0
                for meta_type in meta_types:
                    meta_items = meta_types[meta_type]
                    meta_record = {
                        "name": meta_type,
                        "type": "meta",
                        "count": len(meta_items),
                        "children": meta_items
                    }
                    meta_count += len(meta_items)
                    meta_type_list.append(meta_record)
                meta_type_list = sorted(meta_type_list, key=lambda i: i['count'], reverse=True)
                tag_record = {
                    "name": tag,
                    "type": "tag",
                    "count": meta_count,
                    "children": meta_type_list
                }
                tag_count += meta_count
                tag_list.append(tag_record)
            tag_list = sorted(tag_list, key=lambda i: i['count'], reverse=True)
            tag_type_record = {
                "name": tag_type,
                "type": "type",
                "count": len(tag_list),
                "children": tag_list
            }
            tag_type_count += tag_count
            results.append(tag_type_record)

        return results
    else:
        Log.error("DB Connsection error!")
        return None


def query_meta_stats(selection, headers):
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    Log.debug("Queryings meta stats for %s" % selection)
    entitlements = get_entitlements()
    selector = "WHERE lib.id IN %s" % entitlements

    sort_string = "ORDER BY mi.id"
    if selection != "all":
        selector = "WHERE mi.%s is not NULL AND mi.%s != ''" % (selection, selection)
        sort_string = "ORDER BY mi.%s" % selection

    section = headers.get("Section") or False
    if section:
        selector += " AND library_section = %s" % section

    meta_type = headers.get("Type") or False

    if meta_type:
        meta_id = False
        if meta_type in META_TYPE_NAMES:
            meta_id = META_TYPE_NAMES[meta_type]

        if meta_id:
            selector += " AND mt.metadata_type = %s" % meta_id

    if cursor is not None:
        query = """
            SELECT mi.title, mi.id, mi.year, mt1.title as parent_title, mt2.title as grandparent_title, 
            mi.content_rating, mi.studio, mi.tags_country, mi.rating, mi.added_at,
            lib.name as library_section, mi.library_section_id, mi.metadata_type from metadata_items as mi
            LEFT JOIN metadata_items AS mt1
                on mt1.id = mi.parent_id
            LEFT JOIN metadata_items AS mt2
                on mt2.id = mt1.parent_id
            INNER JOIN library_sections as lib
                on mi.library_section_id = lib.id
            %s
            %s
        """ % (selector, sort_string)

        Log.debug("Query is '%s'" % query)
        records = {}
        record_types = ["year", "contentRating", "studio", "score"]
        for title, rating_key, year, parent_title, grandparent_title, contentRating, studio, country, score, \
                added_at, section, section_id, meta_type in cursor.execute(query):

            if meta_type in META_TYPE_IDS:
                meta_type = META_TYPE_IDS[meta_type]

            dicts = {
                "title": title,
                "ratingKey": rating_key,
                "year": year,
                "contentRating": contentRating,
                "studio": studio,
                "score": score,
                "sectionName": section,
                "sectionId": section_id,
                "added": added_at
            }

            if parent_title != "":
                dicts["parentTitle"] = parent_title

            if grandparent_title != "":
                dicts["grandparentTitle"] = grandparent_title

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(rating_key) + "/banner/"

            if (selection == "tags_country") | (selection == "all"):
                country_data = {}
                if 'country' in records:
                    country_data = records['country']
                countries = country.split("|")
                if ("USA" in countries) | ("United States" in countries):
                    countries = ["USA"]
                if "United Kingdom" in countries:
                    countries = ["United Kingdom"]
                for country_rec in countries:
                    dicts['country'] = country_rec
                    country_list = {}
                    if country_rec in country_data:
                        country_list = country_data[country_rec]
                    meta_list = []
                    if meta_type in country_list:
                        meta_list = country_list[meta_type]
                    meta_list.append(dicts)
                    country_list[meta_type] = meta_list
                    country_data[country_rec] = country_list
                records['country'] = country_data
                if selection == "all":
                    for record_type in record_types:
                        record_value = dicts[record_type]
                        if (record_value is not None) & (len(str(record_value))) > 0:
                            type_data = {}
                            if record_type in records:
                                type_data = records[record_type]
                            type_list = {}
                            if record_value in type_data:
                                type_list = type_data[record_value]
                            meta_list = []
                            if meta_type in type_list:
                                meta_list = type_list[meta_type]
                            meta_list.append(dicts)
                            type_list[meta_type] = meta_list
                            type_data[record_value] = type_list
                            records[record_type] = type_data
            else:
                record_value = dicts[selection]
                if len(str(record_value)) > 0:
                    type_data = {}
                    if selection in records:
                        type_data = records[selection]
                    type_list = {}
                    if record_value in type_data:
                        type_list = type_data[record_value]
                    meta_list = []
                    if meta_type in type_list:
                        meta_list = type_list[meta_type]
                    meta_list.append(dicts)
                    type_list[meta_type] = meta_list
                    type_data[record_value] = type_list
                    records[selection] = type_data

        close_connection(connection)

        results = []
        container_size = int(headers.get("Container-Size") or 25)
        container_start = int(headers.get("Container-Start") or DEFAULT_CONTAINER_START)
        container_max = container_size + container_start
        Log.debug("Container size is set to %s, start to %s" % (container_size, container_start))

        tag_type_count = 0
        # country/rating/etc
        for tag_type in records:
            Log.debug("Tag type is %s" % tag_type)
            tags = records[tag_type]
            tag_list = []
            tag_count = 0
            # 1922/TV-MA
            for tag in tags:

                Log.debug("Tags.tag: %s %s" % (tag, json.dumps(tags[tag])))
                meta_types = tags[tag]
                meta_type_list = []
                meta_count = 0
                for meta_type in meta_types:
                    meta_items = meta_types[meta_type]
                    meta_record = {
                        "name": str(meta_type),
                        "type": "meta",
                        "count": len(meta_items),
                        "children": meta_items
                    }
                    meta_count += len(meta_items)
                    meta_type_list.append(meta_record)
                meta_type_list = sorted(meta_type_list, key=lambda i: i['count'], reverse=True)
                tag_record = {
                    "name": str(tag),
                    "type": "tag",
                    "count": meta_count,
                    "children": meta_type_list
                }
                tag_count += meta_count
                tag_list.append(tag_record)
            tag_list = sorted(tag_list, key=lambda i: i['count'], reverse=True)
            if len(tag_list) >= container_max:
                tag_list = tag_list[container_start:container_size]
            else:
                tag_list = tag_list[container_start:]
            tag_type_record = {
                "name": str(tag_type),
                "type": "type",
                "count": len(tag_list),
                "children": tag_list
            }
            tag_type_count += tag_count
            results.append(tag_type_record)

        return results


def query_user_stats():
    headers = sort_headers(["Type", "Userid", "Username", "Container-Start", "Container-Size", "Devicename",
                            "Deviceid", "Title", "Start", "End", "Interval"])

    entitlements = get_entitlements()
    user_name_selector = headers.get("Username") or False
    user_id_selector = headers.get("Userid") or False
    device_name_selector = headers.get("Devicename") or False
    device_id_selector = headers.get("Deviceid") or False

    type_selector = "(1, 4, 10)"
    if "Type" in headers:
        meta_type = headers.get("Type")
        if meta_type in META_TYPE_NAMES:
            meta_type = META_TYPE_NAMES[meta_type]
        if int(meta_type) == meta_type:
            type_selector = "(%s)" % meta_type

    query_string = "WHERE sm.metadata_type IN %s" % type_selector
    if user_name_selector:
        query_string += " AND user_name='%s'" % user_name_selector

    if user_id_selector:
        query_string += " AND user_id='%s'" % user_name_selector

    if device_name_selector:
        query_string += " AND device_name='%s'" % user_name_selector

    if device_id_selector:
        query_string += " AND device_id='%s'" % user_name_selector

    interval = build_interval()
    start_date = interval[0]
    end_date = interval[1]
    query_string += " AND sm.at between ? AND ?"

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]

    if cursor is not None:

        byte_query = """
                    SELECT accounts.name as user_name, sm.at, sm.metadata_type, accounts.id as user_id,
                    devices.name AS device_name, devices.identifier AS device_id, sb.bytes from statistics_media AS sm
                    INNER JOIN statistics_bandwidth as sb
                     ON sb.at = sm.at AND sb.account_id = sm.account_id AND sb.device_id = sm.device_id
                    INNER JOIN accounts
                     ON accounts.id = sm.account_id
                    INNER JOIN devices
                     ON devices.id = sm.device_id
                    %s
                    ORDER BY sm.at DESC;
                    """ % query_string
        params = (start_date, end_date)
        Log.debug("Media stats/device query is '%s', params are %s" % (byte_query, params))
        user_results = {}
        user_dates = {}
        meta_count = 0
        for user_name, viewed_at, meta_type, user_id, device_name, device_id, data_bytes in cursor.execute(
                byte_query, params):
            if user_name not in user_results:
                user_results[user_name] = {}
            user_dict = user_results.get(user_name)

            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            item_list = user_dict.get(meta_type) or []
            last_active = user_dates.get(user_name) or 0

            last_viewed = int(time.mktime(datetime.datetime.strptime(viewed_at, "%Y-%m-%d %H:%M:%S").timetuple()))

            if last_viewed > last_active:
                last_active = last_viewed

            user_dates[user_name] = last_active

            dicts = {
                "userId": user_id,
                "userName": user_name,
                "lastViewedAt": last_viewed,
                "type": meta_type,
                "deviceName": device_name,
                "deviceId": device_id,
                "bytes": data_bytes
            }

            meta_count += 1
            item_list.append(dicts)
            user_results[user_name][meta_type] = item_list
            user_results[user_name]['lastSeen'] = last_active

        Log.debug("Query results sorted, found %s records." % meta_count)

        query_string = "WHERE sm.metadata_type IN %s" % type_selector
        if user_name_selector:
            query_string += " AND user_name='%s'" % user_name_selector

        if user_id_selector:
            query_string += " AND user_id='%s'" % user_name_selector

        if device_name_selector:
            query_string += " AND device_name='%s'" % user_name_selector

        if device_id_selector:
            query_string += " AND device_id='%s'" % user_name_selector

        query_string += " AND sm.library_section_id in %s" % entitlements
        query_string += " AND sm.viewed_at BETWEEN ? AND ?"

        query = """
            SELECT sm.account_id as user_id, sm.library_section_id, sm.grandparent_title,
            sm.parent_title, sm.title, mi.id as rating_key, mi.tags_genre as genre, mi.tags_country as country, mi.year,
            sm.viewed_at, sm.metadata_type, accounts.name as user_name
            FROM metadata_item_views as sm
            JOIN accounts
            ON 
            sm.account_id = accounts.id
            LEFT JOIN metadata_items as mi
            ON 
            sm.title = mi.title 
            AND mi.library_section_id = sm.library_section_id
            AND mi.metadata_type = sm.metadata_type
            %s                      
            ORDER BY sm.viewed_at desc;
            """ % query_string

        params = (start_date, end_date)
        Log.debug("Meta query is '%s', params are %s" % (query, params))

        view_results = {}
        meta_count = 0
        for user_id, library_section, grandparent_title, parent_title, title, rating_key, genre, country, year, \
                viewed_at, meta_type, user_name in cursor.execute(query, params):
            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            last_viewed = int(time.mktime(datetime.datetime.strptime(viewed_at, "%Y-%m-%d %H:%M:%S").timetuple()))

            user_dict = view_results.get(user_name) or {}
            view_meta_list = user_dict.get(meta_type) or []

            dicts = {
                "userId": user_id,
                "userName": user_name,
                "title": title,
                "parentTitle": parent_title,
                "grandparentTitle": grandparent_title,
                "librarySectionID": library_section,
                "lastViewedAt": last_viewed,
                "type": meta_type,
                "ratingKey": rating_key,
                "thumb": "/library/metadata/" + str(rating_key) + "/thumb",
                "art": "/library/metadata/" + str(rating_key) + "/art",
                "year": year,
                "genre": genre,
                "country": country
            }

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(rating_key) + "/banner/"

            meta_count += 1
            view_meta_list.append(dicts)
            user_dict[meta_type] = view_meta_list
            view_results[user_name] = user_dict

        Log.debug("Meta query completed, %s records retrieved." % meta_count)

        query3 = """
                    SELECT SUM(sb.bytes), sb.account_id AS user_id, devices.identifier, accounts.name AS user_name,
                    devices.name AS device_name, devices.identifier AS machine_identifier
                    FROM statistics_bandwidth AS sb
                    INNER JOIN accounts
                    ON accounts.id = sb.account_id
                    INNER JOIN devices
                    ON devices.id = sb.device_id
                    GROUP BY account_id, device_id;
                    """

        Log.debug("Device query is '%s'" % query3)

        device_results = {}
        for total_bytes, user_id, device_id, user_name, device_name, machine_identifier in cursor.execute(query3):
            user_list = device_results.get(user_name) or []

            device_dict = {
                "userId": user_id,
                "userName": user_name,
                "deviceId": device_id,
                "deviceName": device_name,
                "machineIdentifier": machine_identifier,
                "totalBytes": total_bytes
            }
            user_list.append(device_dict)
            device_results[user_name] = user_list
        close_connection(connection)
        Log.debug("Connection closed.")

        output = []
        container_start = headers.get("Container-Start") or DEFAULT_CONTAINER_START
        container_size = headers.get("Container-Size") or DEFAULT_CONTAINER_SIZE
        Log.debug("Container starts size are %s and %s" % (container_start, container_size))
        for record_user, type_dict in view_results.items():
            user_id = False
            user_meta_results = {}
            user_dict = user_results.get(record_user) or {}
            device_list = device_results.get(record_user) or []
            last_seen = user_dict.get('lastSeen') or "NEVER"
            for meta_type, meta in type_dict.items():
                meta_list = user_meta_results.get(meta_type) or []
                for meta_record in meta:
                    user_meta_list = user_dict.get(meta_type) or []
                    record_date = str(meta_record["lastViewedAt"])[:6]
                    for check in user_meta_list:
                        check_date = str(check["lastViewedAt"])[:6]
                        if check_date == record_date:
                            for value in ["deviceName", "deviceId", "bytes"]:
                                meta_record[value] = check[value]
                    user_id = meta_record['userId']
                    del meta_record['userName']
                    del meta_record['userId']
                    meta_record['lastViewedAt'] = datetime.datetime.fromtimestamp(meta_record['lastViewedAt']).strftime(
                        DATE_STRUCTURE)
                    meta_list.append(meta_record)
                meta_list = sorted(meta_list, key=lambda i: i['lastViewedAt'], reverse=True)
                user_meta_results[meta_type] = meta_list
            device_list = sorted(device_list, key=lambda i: i['totalBytes'], reverse=True)

            device_list = device_list[container_start:container_size]
            for meta_type, truncate in user_meta_results.items():
                truncate = truncate[container_start:container_size]
                user_meta_results[meta_type] = truncate
            last_seen = datetime.datetime.fromtimestamp(last_seen).strftime("%Y-%m-%d")
            user_record = {
                "meta": user_meta_results,
                "devices": device_list,
                "userName": record_user,
                "userId": user_id,
                "lastSeen": last_seen
            }

            output.append(user_record)

        output = sorted(output, key=lambda i: i['lastSeen'], reverse=True)
        return output
    else:
        Log.error("DB Connection error!")
        return None


def query_library_stats():
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    if cursor is not None:
        entitlements = get_entitlements()
        query = """SELECT
            FirstSet.library_section_id,
            FirstSet.metadata_type,    
            FirstSet.item_count,
            SecondSet.play_count,
            SecondSet.rating_key,
            SecondSet.title,
            SecondSet.grandparent_title,
            SecondSet.last_viewed,
            SecondSet.user_name,
            SecondSet.user_id,
            FirstSet.section_name,
            FirstSet.section_type
        FROM 
            (
                SELECT
                    mi.library_section_id,
                    mi.metadata_type,
                    ls.name AS section_name, ls.section_type,
                    count(mi.metadata_type) AS item_count
                FROM metadata_items AS mi
                INNER JOIN library_sections AS ls
                    ON mi.library_section_id = ls.id
                WHERE library_section_id IS NOT NULL
                GROUP BY library_section_id, metadata_type
            ) AS FirstSet
        LEFT JOIN
            (
                SELECT 
                    mi.id AS rating_key,
                    miv.title AS title,
                    miv.library_section_id,
                    miv.viewed_at AS last_viewed,
                    miv.metadata_type,
                    miv.grandparent_title AS grandparent_title,
                    count(miv.metadata_type) AS play_count,
                    accounts.name AS user_name, accounts.id AS user_id,
                    ls.name AS section_name, ls.section_type AS section_type,
                    max(viewed_at) AS last_viewed 
                FROM metadata_item_views AS miv
                INNER JOIN library_sections AS ls
                    ON miv.library_section_id = ls.id
                INNER JOIN metadata_items AS mi
                    ON mi.title = miv.title
                INNER JOIN accounts
                    ON miv.account_id = accounts.id
                AND
                    mi.metadata_type = miv.metadata_type             
                WHERE mi.library_section_id IS NOT NULL
                AND mi.library_section_id in %s
                GROUP BY miv.metadata_type
            ) AS SecondSet
        ON FirstSet.library_section_id = SecondSet.library_section_id
        AND FirstSet.metadata_type = SecondSet.metadata_type
        WHERE FirstSet.library_section_id in %s
        GROUP BY FirstSet.library_section_id, FirstSet.metadata_type
        ORDER BY FirstSet.library_section_id;""" % (entitlements, entitlements)

        Log.debug("Querys is '%s'" % query)
        results = []
        for section, meta_type, item_count, play_count, ratingkey, title, \
                grandparent_title, last_viewed, user_name, user_id, sec_name, sec_type in cursor.execute(query):

            meta_type = META_TYPE_IDS.get(meta_type) or meta_type

            if last_viewed is not None:
                last_viewed = int(time.mktime(time.strptime(last_viewed, '%Y-%m-%d %H:%M:%S')))

            dicts = {
                "section": section,
                "totalItems": item_count,
                "playCount": play_count,
                "title": title,
                "grandparentTitle": grandparent_title,
                "lastViewedAt": last_viewed,
                "type": meta_type,
                "username": user_name,
                "userId": user_id,
                "sectionType": sec_type,
                "sectionTitle": sec_name,
                "ratingKey": ratingkey,
                "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                "art": "/library/metadata/" + str(ratingkey) + "/art"
            }

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(ratingkey) + "/banner/"

            results.append(dicts)
        count_query = """
                        SELECT mi.total_items, miv.viewed_count, mi.metadata_type, mi.library_section_id
                        FROM (
                            SELECT count(metadata_type) AS total_items, metadata_type, library_section_id
                            FROM metadata_items
                            GROUP BY metadata_type, library_section_id
                        ) AS mi
                        INNER JOIN (
                            SELECT count(metadata_type) AS viewed_count, metadata_type, library_section_id FROM (
                                SELECT DISTINCT metadata_type, library_section_id, title, thumb_url
                                FROM metadata_item_views
                            ) AS umiv
                            GROUP BY metadata_type, library_section_id
                        ) AS miv
                        ON miv.library_section_id = mi.library_section_id AND miv.metadata_type = mi.metadata_type;
                        """
        sec_counts = {}
        for total_items, viewed_count, meta_type, section_id in cursor.execute(count_query):
            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            dicts = {
                "sectionId": section_id,
                "type": meta_type,
                "totalItems": total_items,
                "viewedItems": viewed_count
            }
            sec_counts[str(section_id)] = dicts
        close_connection(connection)
        return [results, sec_counts]
    else:
        Log.error("Error connecting to DB!")


def query_library_growth(headers):
    container_size = int(headers.get("Container-Size") or DEFAULT_CONTAINER_SIZE)
    container_start = int(headers.get("Container-Start") or DEFAULT_CONTAINER_START)
    results = []
    interval = build_interval()
    start_date = interval[0]
    end_date = interval[1]

    Log.debug("Okay, we should have start and end dates of %s and %s" % (start_date, end_date))
    entitlements = get_entitlements()
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    if cursor is not None:
        Log.debug("Ready to query!")
        query = """
            SELECT mi1.id, mi1.title, mi1.year, mi1.metadata_type, mi1.created_at, mi1.tags_genre AS genre, 
            mi1.tags_country AS country, mi1.parent_id as parentRatingKey, mi2.title AS parent_title,
            mi2.parent_id AS grandparentRatingKey, mi3.title AS grandparent_title, mi1.library_section_id as section
            FROM metadata_items AS mi1
            LEFT JOIN metadata_items AS mi2
            ON mi1.parent_id = mi2.id
            LEFT JOIN metadata_items AS mi3
            ON mi2.parent_id = mi3.id
            WHERE mi1.created_at BETWEEN ? AND ?
            AND section in %s
            AND mi1.metadata_type IN (1, 4, 10)
            ORDER BY mi1.created_at DESC;
        """ % entitlements
        params = (start_date, end_date)
        Log.debug("Query is '%s', params are '%s'" % (query, params))
        i = 0
        container_max = container_start + container_size
        for rating_key, title, year, meta_type, created_at, genres, country, \
            parentRatingKey, parent_title, \
            grandparentRatingKey, grandparent_title, section \
                in cursor.execute(query, params):
            if i >= container_max:
                break

            if i >= container_start:
                meta_type = META_TYPE_IDS.get(meta_type) or meta_type
                dicts = {
                    "ratingKey": rating_key,
                    "title": title,
                    "parentTitle": parent_title,
                    "parentRatingKey": parentRatingKey,
                    "grandparentTitle": grandparent_title,
                    "grandparentRatingKey": grandparentRatingKey,
                    "year": year,
                    "thumb": "/library/metadata/" + str(rating_key) + "/thumb",
                    "art": "/library/metadata/" + str(rating_key) + "/art",
                    "type": meta_type,
                    "genres": genres,
                    "country": country,
                    "addedAt": created_at
                }
                results.append(dicts)
            i += 1
        close_connection(connection)
    return results


def log_data(data_in):
    Log.debug("Is there data?? " + json.dumps(data_in))


def query_library_popular():
    headers = sort_headers(["Type", "Section", "Start", "End", "Interval", "Sort"])

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    Log.debug("Querying most popular media")
    entitlements = get_entitlements()
    selector = "AND mi.library_section_id IN %s AND sm.title != ''" % entitlements
    sort = headers.get("Sort") or "Total"
    section = headers.get("Section") or False
    if section:
        selector += " AND mi.library_section_id = %s" % section

    meta_type = headers.get("Type") or False

    if meta_type:
        meta_id = False
        if meta_type in META_TYPE_NAMES:
            meta_id = META_TYPE_NAMES[meta_type]

        if meta_id:
            selector += " AND mi.metadata_type = %s" % meta_id

    interval = build_interval()
    start_date = interval[0]
    end_date = interval[1]

    results = {}

    if cursor is not None:
        query = """
            SELECT
                sm.library_section_id as sectionId, sm.[index] as number, mi2.title as parentTitle, mi.rating,
                sm.title, sm.viewed_at as lastViewed, mi.id as ratingKey, mi.tags_genre as genre,
                sm.account_id as userId, accounts.name as userName, mi.metadata_type as type, mi2.id as parentRatingKey,
                mi2.metadata_type as parentType, mi2.[index] as parentIndex, mi3.title as grandparentTitle,
                mi3.id as grandparentRatingKey, mi3.metadata_type as grandparentType
            FROM metadata_item_views as sm
            INNER JOIN metadata_items as mi
                ON 
                mi.guid = sm.guid
                AND mi.title = sm.title
                and mi.library_section_id = sm.library_section_id
            LEFT JOIN metadata_items as mi2
                ON
                mi.parent_id = mi2.id
            LEFT JOIN metadata_items as mi3
                ON
                mi2.parent_id = mi3.id
            INNER JOIN accounts
                ON accounts.id = sm.account_id
                WHERE sm.viewed_at BETWEEN '%s' AND '%s'
            %s
            order by ratingKey, lastViewed;
        """ % (start_date, end_date, selector)

        Log.debug("Query is '%s'" % query)
        results = {}
        for row in cursor.execute(query):
            descriptions = cursor.description
            count = 0
            record = {"playCount": 0, "users": {}}
            for key, foo in descriptions:
                value = row[count]
                record[key] = value

                if key == "type":
                    meta_type = META_TYPE_IDS.get(value) or value
                    record[key] = meta_type

                count += 1

            # Done building "item", now do stuff
            rating_key = record['ratingKey']
            user_name = record['userName']
            user_id = record['userId']
            viewed_at = record['lastViewed']

            record = results.get(rating_key) or record
            record_last = record['lastViewed']
            if record_last > viewed_at:
                last_viewed = record_last
            else:
                last_viewed = viewed_at
            view_count = record.get("playCount") or 0
            view_count += 1
            record["playCount"] = view_count

            users_dict = record.get('users') or {}
            user_record = users_dict.get(user_name) or {
                "userName": user_name,
                "userId": user_id
            }
            user_views = user_record.get("views") or []
            user_views.append({"dateViewed": viewed_at})
            user_record["views"] = user_views
            users_dict[user_name] = user_record
            record['users'] = users_dict
            record['lastViewed'] = last_viewed

            subkeys = ["parent", "grandparent"]

            for key in subkeys:
                # Build/check parent, grandparents
                if record[key + "RatingKey"] is not None:
                    sub_id = record[key + "RatingKey"]
                    sub_record = results.get(sub_id) or {
                        "ratingKey": record[key + 'RatingKey'],
                        "title": record[key + 'Title'],
                        "lastViewed": last_viewed,
                        "genre": record['genre']
                    }
                    sub_last = sub_record['lastViewed']
                    if sub_last > last_viewed:
                        last_viewed = sub_last
                    sub_record['lastViewed'] = last_viewed
                    sub_count = sub_record.get('playCount') or 0
                    sub_users_dict = sub_record.get('users') or {}
                    sub_user_record = sub_users_dict.get(user_name) or {
                        "userName": user_name,
                        "userId": user_id
                    }
                    sub_user_views = sub_user_record.get("views") or []
                    sub_user_views.append({"dateViewed": viewed_at})
                    sub_user_record["views"] = sub_user_views
                    sub_users_dict[user_name] = sub_user_record
                    sub_record['users'] = sub_users_dict
                    sub_view_count = sub_user_record.get("playCount") or 0
                    sub_view_count += 1
                    sub_user_record["playCount"] = sub_view_count
                    sub_users_dict[user_name] = sub_user_record
                    sub_count += 1
                    sub_meta_type = META_TYPE_IDS.get(record[key + 'Type']) or "unknown"
                    sub_record["playCount"] = sub_count
                    sub_record["type"] = sub_meta_type
                    sub_record["parentTitle"] = record[key + 'Title']
                    sub_record["parentRatingKey"] = record[key + 'RatingKey']
                    if sub_meta_type == "season":
                        sub_record["index"] = record[key + 'Index']

                    results[sub_id] = sub_record

            results[rating_key] = record

        close_connection(connection)

    # Now sort by meta type
    sorted_media = {}
    for rating_key, media in results.items():

        remove_items = ["grandparentType", "parentType", "number", "parentIndex"]
        for remove in remove_items:
            if remove in media:
                del media[remove]

        meta_type = media["type"]
        meta_list = sorted_media.get(meta_type) or []
        media['userCount'] = len(media['users'])
        media["art"] = "/library/metadata/" + str(media['ratingKey']) + "/art"
        media["key"] = "/library/metadata/" + str(media['ratingKey']) + "/thumb"

        if meta_type == "episode":
            media["banner"] = "/library/metadata/" + str(media['ratingKey']) + "/banner/"

        play_count = 0
        for user, user_data in media["users"].items():
            play_count += len(user_data["views"])
        media['playCount'] = play_count
        meta_list.append(media)
        sorted_media[meta_type] = meta_list

    results = {}
    for meta_type, list_item in sorted_media.items():
        sort_keys = ["userCount", "playCount", "title"]
        if sort in sort_keys:
            param = sort
        else:
            param = "userCount"
        Log.debug("Sorting stuff by %s" % param)

        sort_reverse = param != "title"
        list_item = sorted(list_item, key=lambda z: z[param], reverse=sort_reverse)
        sort_param_count = {}
        resort = True
        for item in list_item:
            sort_param_count[str(item[param])] = 1
            if len(sort_param_count) > 1:
                resort = False
                break

        if resort:
            if param == "userCount":
                param = "playCount"
            else:
                param = "title"
            Log.debug("All %s items have same sort param, sorting by %s." % (meta_type, param))
            sort_reverse = param != "title"
            list_item = sorted(list_item, key=lambda i: i[param], reverse=sort_reverse)
        results[meta_type] = list_item

    return results


def fetch_cursor():
    if data.Exists('path'):
        path = data.Load('path')
        path = pathlib.Path(path)
    else:
        Log.debug("NO PATH FOR DB")
        return False

    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    return [cursor, connection]


def close_connection(connection):
    Log.debug("No. I don't wanna.")
    connection.close()


def get_entitlements():
    token = False
    allowed_keys = []
    inputs = merge_dict(request.headers, request.query)
    for key, value in inputs.items():
        Log.debug("Header key %s is %s", key, value)
        if key in ("X-Plex-Token", "Token"):
            Log.debug("We have a Token")
            token = value

    if token is False:
        if data.Exists('token'):
            token = data.Load('token')

    if token:
        uri = "http://localhost:32400"
        if data.Exists('uri'):
            uri = data.Load('uri')
        try:
            my_url = "%s/library/sections?X-Plex-Token=%s" % (uri, token)
        except TypeError:
            my_url = False
            pass

        if my_url:
            Log.debug("Gonna touch myself at '%s'" % my_url)
            req = requests.get(my_url)
            client_data = req.text
            root = elTree.fromstring(client_data)
            for section in root.iter('Directory'):
                Log.debug("Section?")
                allowed_keys.append(section.get("key"))

    if len(allowed_keys) != 0:
        allowed_keys = "(" + ", ".join(allowed_keys) + ")"
        Log.debug("Hey, we got the keys: %s" % allowed_keys)
    else:
        allowed_keys = "()"
        Log.debug("No keys, try again.")

    return allowed_keys


def get_session_status():
    sessions = []
    token = False
    include_extra = False
    for key, value in request.headers.items():
        if key in ("X-Plex-Token", "Token"):
            Log.debug("We have a Token")
            token = value
        if key in ("X-Plex-IncludeExtra", "IncludeExtra"):
            check = value
            if (check is True) | (check == "true"):
                include_extra = True

    if token is False:
        if data.Exists('token'):
            token = data.Load('token')

    if token:
        uri = 'http://localhost:32400'
        if data.Exists('uri'):
            uri = data.Load('uri')
        try:
            my_url = "%s/status/sessions?X-Plex-Token=%s" % (uri, token)
        except TypeError:
            my_url = False
            pass

        if my_url:
            Log.debug("Fetching status froms '%s'" % my_url)
            req = requests.get(my_url)
            client_data = req.text
            parsed_dict = xmltodict.parse(client_data)
            reply_dict = un_attribute(parsed_dict)
            mc = reply_dict['MediaContainer']
            sections = mc.get('Video') or []
            if type(sections) == dict:
                sections = [sections]
            for session in sections:
                client_dict = session.get('Player') or {}
                del session['Player']
                keep = ["Video', ""Media", "Genre", "User", "Country", "Session", "TranscodeSession"]
                delete = []
                if include_extra is False:
                    Log.debug("Filtering")
                    for key in session:
                        if (key not in keep) & (type(session[key]) is not str):
                            delete.append(key)
                for key in delete:
                    Log.debug("Should be deleting key %s" % key)
                    del session[key]

                client_dict['Video'] = session
                sessions.append(client_dict)
    return sessions


def un_attribute(xml_dict):
    dict_out = {}
    for key, value in xml_dict.items():
        fixed = key.replace("@", "")
        if type(value) is str:
            value = value
        elif type(value) is list:
            new_list = []
            for item in value:
                new_list.append(un_attribute(item))
            value = new_list
        else:
            value = un_attribute(value)
        dict_out[fixed] = value
    return dict_out


####################################
# These functions are for utility stuff
def get_time_difference(time_start, time_end):
    time_diff = time_end - time_start
    return time_diff.total_seconds() / 60


def sort_headers(header_list, strict=False):
    returns = {}
    items = merge_dict(request.headers, request.query)
    for key, value in items.items():

        for item in header_list:
            if key in ("X-Plex-" + item, item):
                value = str(value)
                try:
                    test_value = int(value)
                except ValueError:
                    Log.debug("Value is not a string.")
                    pass
                else:
                    value = test_value
                Log.debug("Value for %s is '%s'" % (item, value))

                returns[item] = value
                header_list.remove(item)

    if strict:
        len2 = len(header_list)
        if len2 == 0:
            Log.debug("We have all of our values: " + json.dumps(returns))
            return returns

        else:
            Log.error("Sorry, parameters are missing.")
            for item in header_list:
                Log.error("Missing " + item)
            return False
    else:
        return returns


def build_interval():
    headers = sort_headers(["Start", "End", "Interval"])
    start_date = False
    end_date = False
    interval = False

    if "End" in headers:
        end_check = headers.get("End")
        valid = validate_date(end_check)
        if valid:
            end_date = valid

    if "Start" in headers:
        start_check = headers.get("Start")
        valid = validate_date(start_check)
        if valid:
            Log.debug("We have a vv start date, we'll use that.")
            start_date = valid

    if "Interval" in headers:
        int_check = headers.get("Interval")
        if int(int_check) == int_check:
            interval = int_check

    if start_date & end_date:
        return [start_date, end_date]

    if start_date & interval:
        start_date = datetime.datetime.strftime(datetime.datetime.strptime(
            start_date, DATE_STRUCTURE) + datetime.timedelta(days=interval), DATE_STRUCTURE)
        return [start_date, end_date]

    if end_date & interval:
        start_date = datetime.datetime.strftime(datetime.datetime.strptime(
            end_date, DATE_STRUCTURE) - datetime.timedelta(days=interval), DATE_STRUCTURE)
        return [start_date, end_date]

    # Default behavior is to return 365 (or specified interval) days worth of records from today.
    Log.debug("Returning default interval")
    if interval is False:
        interval = 365
    end_int = datetime.datetime.now()
    start_int = end_int - datetime.timedelta(days=interval)
    start_date = datetime.datetime.strftime(start_int, DATE_STRUCTURE)
    end_date = datetime.datetime.strftime(end_int, DATE_STRUCTURE)
    return [start_date, end_date]


def merge_dict(dict1, dict2):
    out = {}
    for key, value in dict1.items():
        out[key] = value
    for key, value in dict2.items():
        out[key] = value

    return out


def validate_date(date_text):
    valid = False

    full_date = str(date_text).split(" ")
    date_list = full_date[0].split("-")
    if len(date_list) == 3:
        Log.debug("Date has YMD params, we're good")
    if len(date_list) == 2:
        Log.debug("Date missing day param, adding")
        date_list.append("01")
    if len(date_list) == 1:
        Log.debug("Date missing month and day, adding")
        date_list.append("01")
        date_list.append("01")
    date_param = "-".join(date_list)

    time_list = ["00", "00", "00"]
    if len(full_date) == 2:
        Log.debug("Date appears to have a time param")
        time_list = full_date[1].split(":")
        if len(time_list) == 3:
            Log.debug("Date has full time")
        if len(time_list) == 2:
            Log.debug("Date is missing hours")
            time_list.append("00")
        if len(time_list) == 1:
            Log.debug("Date has an hour param only")
            time_list.append("00")
            time_list.append("00")
    time_param = ":".join(time_list)

    date_check = "%s %s" % (date_param, time_param)
    Log.debug("Date string built to %s" % date_check)

    try:
        datetime.datetime.strptime(date_check, DATE_STRUCTURE)
        valid = date_check
    except ValueError:
        pass

    if valid is False:
        Log.error("Could not determine date structure for '%s" % date_text)

    return valid


def runner():
    """ Method that runs forever """
    print("Executing")
    run(host="0.0.0.0", port=5667, debug=True)


def custom_action(los_packet):
    src_uri = los_packet[0][1].src
    dest_uri = los_packet[0][1].dst
    if data.Exists('device_json'):
        match = False
        for cast_dev in json.loads(data.Load('device_json')):
            cast_ip = cast_dev['uri'].split(":")[0]
            if (cast_ip == dest_uri) | (cast_ip == src_uri):
                if cast_ip == dest_uri:
                    dest_uri = cast_dev['name']
                if cast_ip == src_uri:
                    src_uri = cast_dev['name']
                match = True
        if src_uri == "192.168.1.120":
            match = False
        if dest_uri == "192.168.1.120":
            match = False
        if match:
            Log.debug('Packet Match: {} ==> {}'.format(src_uri, dest_uri))


def socket_spy():
    sniff(filter='tcp or udp', prn=custom_action)


thread = threading.Thread(target=runner)
thread2 = threading.Thread(target=cache_timer)
thread3 = threading.Thread(target=socket_spy)
thread.daemon = True
thread.start()
update_cache()
thread2.start()
thread3.start()
