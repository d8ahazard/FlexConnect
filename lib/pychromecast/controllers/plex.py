"""
Controller to interface with the Plex-app.
"""
import json
from time import sleep
from urllib.parse import urlparse

from pychromecast.controllers import BaseController
from pychromecast.controllers.media import MediaController

STREAM_TYPE_UNKNOWN = "UNKNOWN"
STREAM_TYPE_BUFFERED = "BUFFERED"
STREAM_TYPE_LIVE = "LIVE"
MESSAGE_TYPE = 'type'

TYPE_PLAY = "PLAY"
TYPE_PAUSE = "PAUSE"
TYPE_STOP = "STOP"
TYPE_STEPFORWARD = "STEPFORWARD"
TYPE_STEPBACKWARD = "STEPBACK"
TYPE_PREVIOUS = "PREVIOUS"
TYPE_NEXT = "NEXT"
TYPE_LOAD = "LOAD"
TYPE_SEEK = "SEEK"
TYPE_MEDIA_STATUS = 'MEDIA_STATUS'
TYPE_GET_STATUS = "GET_STATUS"


class PlexController(BaseController):
    """ Controller to interact with Plex namespace. """

    def __init__(self,cast):
        super(PlexController, self).__init__(
            "urn:x-cast:plex", "9AC194DC")
        self.app_id = "9AC194DC"
        self.namespace = "urn:x-cast:plex"
        self.request_id = 0
        self.media_session_id = 0
        self.receiver = None
        self.last_message = "No messages sent"
        self.media_meta = {}
        self.volume = cast.status.volume_level
        self.muted = False
        self.stream_type = ""
        self.state = "Idle"
        self._status_listeners = []
        self.cast = cast

    def set_volume(self, percent):
        percent = float(percent) / 100
        self.cast.set_volume(percent)

    def volume_up(self):
        self.cast.volume_up()

    def volume_down(self):
        self.cast.volume_down()

    def mute(self, status):
        self.cast.set_volume_muted(status)

    def stop(self):
        self.namespace = "urn:x-cast:plex"
        """ Send stop command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_STOP})

    def pause(self):
        self.namespace = "urn:x-cast:plex"
        """ Send pause command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_PAUSE})

    def play(self):
        self.namespace = "urn:x-cast:plex"
        """ Send play command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_PLAY})

    def previous(self):
        self.namespace = "urn:x-cast:plex"
        """ Send previous command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_PREVIOUS})

    def next(self):
        self.namespace = "urn:x-cast:plex"
        """ Send next command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_NEXT})

    def get_last_message(self):
        return self.last_message

    def play_media(self, params, callback_function=None, **kwargs):
        """
        Launch the Plex chromecast app and initiate playback

        :param params:
        :param callback_function:
        :dict params: Requires the following keys for successful playback-
            'Contentid' - The media key from PMS
            'Contenttype' - 'audio' or 'video'
            'Serverid' - ID of the PMS Server where the media is located
            'Serveruri' - URI of the PMS Server (http(s)://server.ip.or.hostname:PORT)
            'Transienttoken' - A transient token for the server hosting the media
            'Username' - Name of the PMS user
            'Queueid' - The Playqueue ID containing the media
            'Serverversion' (Optional) - Version of the server hosting the media
            'Offset' (Optional) - Interval, in milliseconds, to start playback from

        :callable|None callback_function:
        :param kwargs: additional arguments
        """
        self.namespace = "urn:x-cast:plex"

        def app_launched_callback():
            self.logger.debug("Application is launched")
            self.set_load(params, callback_function)

        receiver_ctrl = self._socket_client.receiver_controller
        receiver_ctrl.launch_app(self.app_id, callback_function=app_launched_callback)

    def set_load(self, params, callback_function):
        self.logger.debug("Reached the load phase")
        self.namespace = "urn:x-cast:com.google.cast.media"
        play_queue_id = params['Queueid']
        content_id = params['Contentid']
        self.request_id += 1  # Update
        server_uri = urlparse(params['Serveruri'])
        protocol = server_uri.scheme
        address = server_uri.hostname
        port = server_uri.port
        if protocol == 'https':
            verified = True
        else:
            verified = False

        if 'Version' in params:
            server_version = params['Version']
        else:
            server_version = "1.10.1.4602"

        if 'Offset' in params:
            offset = int(params['offset'])
        else:
            offset = 0

        if 'providerIdentifier' in params:
            provider_identifier = params['providerIdentifier']
        else:
            provider_identifier = "com.plexapp.plugins.library"

        if "provider.plex.tv" in content_id:
            self.logger.debug("This is a pc, we need to format")
            container_key = "{}?own=1".format(content_id)
            content_key = "/library/metadata/{}".format(play_queue_id)
        else:
            container_key = "/playQueues/{}?own=1".format(play_queue_id)
            content_key = content_id

        self.logger.debug("Protocol, address, port and verified are %s %s %s and %s", protocol, address, port, verified)

        server = {
                    "machineIdentifier": params["Serverid"],
                    "transcoderVideo": True,
                    "transcoderVideoRemuxOnly": False,
                    "transcoderAudio": True,
                    "isVerifiedHostname": verified,
                    "protocol": protocol,
                    "address": address,
                    "accessToken": params["Transienttoken"]
                }

        if port is not None:
            server['port'] = str(port)

        if 'Primaryserverid' in params:
            primary_id = params['Primaryserverid']
        else:
            primary_id = params['Serverid']

        if 'Primaryservertoken' in params:
            primary_token = params['Primaryservertoken']
        else:
            primary_token = params['Transienttoken']

        if 'Primaryserveruri' in params:
            primary_uri = urlparse(params['Primaryserveruri'])
            primary_protocol = primary_uri.scheme
            primary_address = primary_uri.hostname
            primary_port = primary_uri.port
        else:
            primary_protocol = protocol
            primary_address = address
            primary_port = port

        if primary_protocol == 'https':
            primary_verified = True
        else:
            primary_verified = False

        primary_server = {
                        "machineIdentifier": primary_id,
                        "transcoderVideo": True,
                        "transcoderVideoRemuxOnly": False,
                        "transcoderAudio": True,
                        "version": server_version,
                        "myPlexSubscription": True,
                        "isVerifiedHostname": primary_verified,
                        "protocol": primary_protocol,
                        "address": primary_address,
                        "accessToken": primary_token
                    }

        if primary_port is not None:
            primary_server['port'] = str(primary_port),

        media = {
                "contentId": content_key,
                "streamType": "BUFFERED",
                "metadata": None,
                "duration": None,
                "tracks": None,
                "textTrackStyle": None,
                "customData": {
                    "playQueueType": params['Contenttype'],
                    "providerIdentifier": provider_identifier,
                    "containerKey": container_key,
                    "offset": offset,
                    "directPlay": True,
                    "directStream": True,
                    "audioBoost": 100,
                    "audioForceMultiChannel": False,
                    "autoPlay": True,
                    "mediaIndex": None,
                    "subtitleSize": 100,
                    "server": server,
                    "primaryServer": primary_server,
                    "user": {
                        "username": params["Username"]
                    }
                }
            }

        self.logger.debug("Sending media message: " + json.dumps(media))

        msg = {
            "type": "LOAD",
            "requestId": 0,
            "sessionId": None,  # Does this need to be static?
            "media": media,
            "activeTrackIds": None,
            "autoplay": True,
            "currentTime": offset,
            "customData": None
        }

        self.logger.debug("Sending playback message: " + json.dumps(msg))

        def parse_status(data):
            self.update_plex_status(data)

        self.send_message(msg, inc_session_id=True, callback_function=parse_status)

    def update_plex_status(self, data):
        self.logger.debug("Got a request to update plex status: %s", json.dumps(data))
        self.media_meta = data['status'][0]['media']['metadata']
        self.volume = self.cast.status.volume_level
        self.muted = self.cast.status.volume_muted
        self.stream_type = data['status'][0]['customData']['type']
        self.state = data['status'][0]['playerState']

    def plex_status(self):
        self.namespace = "urn:x-cast:com.google.cast.media"
        self.logger.debug("Plex status requested.")

        def parseada_status(data):
            self.logger.debug("Callback fired?")
            self.update_plex_status(data)

        self.send_message({MESSAGE_TYPE: TYPE_GET_STATUS}, callback_function=parseada_status)

        sleep(1.0)

        return {
            "meta": self.media_meta,
            "volume": self.volume,
            "muted": self.muted,
            "type": self.stream_type,
            "state": self.state
        }

    def receive_message(self, message, data):
        """ Called when a media message is received. """
        self.logger.debug("Plex media receive function called.")
        if data[MESSAGE_TYPE] == TYPE_MEDIA_STATUS:
            self.logger.debug("(DH) MESSAGE RECEIVED: " + data)
            return True

        return False

    def register_status_listener(self, listener):
        """ Register a listener for new media statusses. A new status will
            call listener.new_media_status(status) """
        self._status_listeners.append(listener)
        return False

    def _process_media_status(self, data):
        """ Processes a STATUS message. """


    def _fire_status_changed(self):
        """ Tells listeners of a changed status. """
        for listener in self._status_listeners:
            try:
                self.logger.debug("Doing a thing with a listener...")
            except Exception:  # pylint: disable=broad-except
                pass
    def tear_down(self):
        """ Called when controller is destroyed. """
        super(PlexController, self).tear_down()

        self._status_listeners[:] = []
