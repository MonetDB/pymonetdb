#!/usr/bin/env python3

import os
import platform
import subprocess
import sys
import time

import pytest


def start_mserver(monetdbdir, farmdir, dbname, port, logfile):  # noqa: C901
    exe = os.path.join(monetdbdir, 'bin', 'mserver5')
    if platform.system() == 'Windows':
        exe += '.exe'
    dbpath = os.path.join(farmdir, dbname)
    try:
        os.mkdir(dbpath)
    except FileExistsError:
        pass
    #
    env = dict((k, v) for k, v in os.environ.items())
    path_components = [
        os.path.join(monetdbdir, "bin"),
        os.path.join(monetdbdir, "lib", "monetdb5"),
        env['PATH'],
    ]
    env['PATH'] = os.pathsep.join(path_components)
    sets = dict(
        prefix=monetdbdir,
        exec_prefix=monetdbdir,
        mapi_port=port,
    )
    cmdline = [
        exe,
        f'--dbpath={dbpath}',
    ]
    for k, v in sets.items():
        cmdline.append('--set')
        cmdline.append(f'{k}={v}')
    print()
    print('-- Starting mserver')
    print(f'-- PATH={env["PATH"]}')
    print(f'-- cmdline: {cmdline!r}')
    t0 = time.time()
    awkward_silence = t0 + 2
    proc = subprocess.Popen(cmdline, env=env, stderr=open(logfile, 'wb'))
    #
    while True:
        try:
            code = proc.wait(timeout=0.1)
            exit(f'mserver unexpectedly exited with code {code}')
        except subprocess.TimeoutExpired:
            if os.path.exists(os.path.join(dbpath, '.started')):
                break
            t = time.time()
            if t >= awkward_silence:
                print(f"-- Waited for {t - t0:.1f}s")
                awkward_silence = t + 1
            if t > t0 + 30.1:
                print("Starting mserver took too long, giving up")
                proc.kill()
                exit("given up")
    print('-- mserver has started')
    return proc


if len(sys.argv) != 5:
    exit(f"Usage: {sys.argv[0]} MONETDIR FARMDIR DBNAME PORT")
monet_dir = sys.argv[1]
farm_dir = sys.argv[2]
db_name = sys.argv[3]
db_port = int(sys.argv[4])

proc = start_mserver(monet_dir, farm_dir, db_name, db_port, os.path.join(farm_dir, "errlog"))
try:
    print('The reported default encoding is', sys.getdefaultencoding())
    with open(os.path.join(farm_dir, 'w.txt'), 'w') as f:
        print("The encoding for 'w' files is", f.encoding)
    with open(os.path.join(farm_dir, 'w.txt'), 'wt') as f:
        print("The encoding for 'wt files is", f.encoding)
    ret = pytest.main(args=['-k', 'not test_control'])
    exit(ret)
finally:
    if proc.returncode is None:
        print('-- Killing the server')
        proc.kill()
    else:
        print('-- Server has already terminated')
