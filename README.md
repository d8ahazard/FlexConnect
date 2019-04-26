# FlexConnect
It's like the FlexTV plugin...only not a plugin.

## Howto
1. Install Python3.
2. Clone this zip somewhere, extract it somewhere.
3. Open a command prompt.
4. CD to where you extracted/cloned this repo.
5. run "python FlexConnect.py"
6. Forward port 5667 to the server Flexconnect is on, open any firewalls for 5667 to python.
7. Profit.

## Daemon?
Make a script that does the above, put it in your system's startup thinger.

## Docker?
Yes. digitalhigh/flexconnect

## Configuration
Use a web browser to navigate to localhost:5667, or the IP of the server flexconnect is on.
Enter the IP address of your Plex server, Plex Token, and path to the PMS database file.
(Optional) Set a password and enable authentication. If authentication is enabled, the plex token specified MUST be included in all requests using the param X-Plex-Token=<TOKEN>
