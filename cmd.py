import json
import subprocess
import logging
from time import sleep

log = logging.getLogger('FlexHelper')


def run_command(command, single=True):
    results = []
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    line = p.stdout.readline().decode('utf-8')
    while line:
        if line.strip() is not "":
            results.append(str(line.strip()))
        line = p.stdout.readline().decode('utf-8')
    while p.poll() is None:
        sleep(.1)
    # Empty STDERR buffer
    err = p.stderr.read()
    if p.returncode != 0:
        results = ["Error: " + str(err)]
    log.debug("Returning %s" % json.dumps(results))
    return results
