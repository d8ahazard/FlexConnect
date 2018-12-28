import sys

def get_os_name():
    platforms = {
        'linux1': 'Linux',
        'linux2': 'Linux',
        'darwin': 'MacOSX',
        'win32': 'Windows'
    }
    if sys.platform not in platforms:
        return sys.platform

    return platforms[sys.platform]