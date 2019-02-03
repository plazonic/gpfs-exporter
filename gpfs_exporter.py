#!/usr/bin/python3.6
import os
import sys
import time
import socket
import http.server
from subprocess import (PIPE, STDOUT, Popen, CalledProcessError)

stat_mapping = {
  '_br_': {
    'name': 'gpfs_bytes_read',
    'description': 'GPFS bytes read',
    'type': 'counter'
  },
  '_bw_': {
    'name': 'gpfs_bytes_write',
    'description': 'GPFS bytes written',
    'type': 'counter'
  },
  '_oc_': {
    'name': 'gpfs_requests_open',
    'description': 'GPFS open call requests including create',
    'type': 'counter'
  },
  '_cc_': {
    'name': 'gpfs_requests_close',
    'description': 'GPFS close call requests',
    'type': 'counter'
  },
  '_rdc_': {
    'name': 'gpfs_requests_read',
    'description': 'GPFS number of read requests',
    'type': 'counter'
  },
  '_wc_': {
    'name': 'gpfs_requests_write',
    'description': 'GPFS number of write requests',
    'type': 'counter'
  },
  '_dir_': {
    'name': 'gpfs_requests_readdir',
    'description': 'GPFS number of readdit requests',
    'type': 'counter'
  },
  '_iu_': {
    'name': 'gpfs_inode_updates',
    'description': 'GPFS number of inode updates to disk',
    'type': 'counter'
  },
}
def get_stats():
    # results will go into a hash
    per_host = {}
    # mmpmon needs input file, for simplicity we give it on stdin
    inputscript = b"""
       once nlist add *
       fs_io_s
    """
    process = Popen(["/usr/lpp/mmfs/bin/mmpmon", "-p"], stdout=PIPE, stdin = PIPE, stderr = STDOUT)
    try:
        all_stats, unused_err = process.communicate(inputscript)
    except:
        process.kill()
        process.wait()
        raise
    retcode = process.poll()
    if retcode:
        raise CalledProcessError(retcode, '/usr/lpp/mmfs/bin/mmpmon', output=output)
    for l in all_stats.decode().split('\n'):
        if l.startswith('_fs_io_s_'):
            # _fs_io_s_ _n_ 172.29.22.78 _nn_ tiger-i23g14-op0 _rc_ 0 _t_ 1536005985 _tu_ 350611 _cl_ tiger2.gpfs _fs_ tiger2.gpfs _d_ 32 _br_ 3401130516933 _bw_ 525742920053 _oc_ 14848149 _cc_ 10894911 _rdc_ 1776360 _wc_ 5527815 _dir_ 37573 _iu_ 11739978
            d = dict(zip(*[iter(l.split(' ')[1:])]*2))
            if '_t_' in d and '_tu_' in d:
              d['t_microseconds'] = '%s%06d' %(d['_t_'], int(d['_tu_']))
              d['t_miliseconds'] = d['t_microseconds'][0:-3]
            n = '_nn_' if '_nn_' in d else '_n_'
            n = d[n]
            n = n.replace('-ib0', '')
            n = n.replace('-op0', '')
            if n not in per_host:
                per_host[n] = {}
            per_host[n][d['_fs_']] = d
    return per_host

def get_prom_stats(stats):
    all = []
    for s in stat_mapping.keys():
        ss = stat_mapping[s]
        all.append("# HELP %s %s" %(ss['name'], ss['description']))
        all.append("# TYPE %s counter" % ss['name'])
        for h in sorted(stats.keys()):
            for f in sorted(stats[h].keys()):
                one = stats[h][f]
                if s in one:
                    all.append('%s{fs="%s", host="%s"} %s %s' % (ss['name'], f, h, one[s], one['t_miliseconds']))
    return all

def print_prom_stats(stats):
    for i in get_prom_stats(stats):
        print(i)

def get_systemd_socket():
    SYSTEMD_FIRST_SOCKET_FD = 3
    socket_type = http.server.HTTPServer.socket_type
    address_family = http.server.HTTPServer.address_family
    return socket.fromfd(SYSTEMD_FIRST_SOCKET_FD, address_family, socket_type)

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        self._set_headers()
        for l in get_prom_stats(get_stats()):
            self.wfile.write(str.encode(l + '\n'))
        return
    do_POST = do_GET

class SockInheritHTTPServer(http.server.HTTPServer):
    def __init__(self, address_info, handler, bind_and_activate=True):
        # Note that we call it with bind_and_activate = False.
        http.server.HTTPServer.__init__(self,
                                        address_info,
                                        handler,
                                        bind_and_activate=False)

        # The socket from systemd needs to be set AFTER calling the parent's
        # class's constructor, otherwise HTTPServer.__init__() will re-set
        # self.socket() and the handover won't work.
        self.socket = get_systemd_socket()

def wait_loop(serve=12):
    # The connection/port/host doesn't really matter as we don't allocate the
    # socket ourselves.
    httpserv = SockInheritHTTPServer(('127.0.0.1', 8123), RequestHandler)
    httpserv.timeout = 10
    start = 0
    while start < serve:
        httpserv.handle_request()
        start += 1
    httpserv.server_close()

if __name__ == "__main__":
    fds = os.environ.get("LISTEN_FDS", None)
    if fds != None:
        wait_loop()
        sys.exit()
    else:
        raise SystemExit("This server should only run from systemd")
