import os
import shutil
import sys
import time
import tempfile

__test__ = False


ROOT = os.path.dirname(
    os.path.dirname(
    os.path.dirname(
    os.path.dirname(__file__))))
DYNOMITE = os.path.join(ROOT, 'bin', 'dynomite')
TMP_DIR = None


def setup_module():
    cmd = "%s start -o dpct1 -p 11222 --data '%s' --storage dict_storage " \
          "-n 1 -r 1 -w 1 --detached" % (DYNOMITE, tmp_dir())
    os.system(cmd)
    time.sleep(2)


def teardown_module():
    os.system("%s stop -o dpct1" % DYNOMITE)
    if os.path.isdir(tmp_dir()):
        shutil.rmtree(tmp_dir())


def tmp_dir():
    # Don't want to create dir at import time, only on demand
    global TMP_DIR
    if TMP_DIR:
        return TMP_DIR
    TMP_DIR = tempfile.mkdtemp()
    return TMP_DIR
