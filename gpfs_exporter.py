#!/usr/bin/python2.7
import pwd
import getpass
import os
import re
import sys
import time
import socket
import logging
from subprocess import (PIPE, STDOUT, Popen, CalledProcessError)
from optparse import OptionParser
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer

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
    inputscript = """
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
    for l in all_stats.split('\n'):
        if l.startswith('_fs_io_s_'):
            # _fs_io_s_ _n_ 172.29.22.78 _nn_ tiger-i23g14-op0 _rc_ 0 _t_ 1536005985 _tu_ 350611 _cl_ tiger2.gpfs _fs_ tiger2.gpfs _d_ 32 _br_ 3401130516933 _bw_ 525742920053 _oc_ 14848149 _cc_ 10894911 _rdc_ 1776360 _wc_ 5527815 _dir_ 37573 _iu_ 11739978
            d = dict(map(None, *[iter(l.split(' ')[1:])]*2))
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
        print i

class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        for i in get_prom_stats(get_stats()):
            self.wfile.write(i+'\n')

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        # Doesn't do anything with posted data
        self._set_headers()
        for i in get_prom_stats(get_stats()):
            self.wfile.write(i+'\n')

def run(server_class=HTTPServer, handler_class=S, port=9001, address=''):
    server_address = (address, port)
    httpd = server_class(server_address, handler_class)
    print 'Starting httpd...'
    httpd.serve_forever()


parser = OptionParser()
parser.add_option("-p", "--prometheus", action="store_true", dest="prometheus_out",
                  default=False, help="output on stdout format compatible with prometheus")
parser.add_option("-P", "--port", dest="port", default=9001, help="TCP Port, defaults to 9001")
parser.add_option("-A", "--address", dest="address", default="0.0.0.0", help="TCP IP Address, defaults to 0.0.0.0")
(options, args) = parser.parse_args()

if options.prometheus_out:
    all_hosts = get_stats()
    print_prom_stats(all_hosts)
else:
    run(port=int(options.port), address=options.address)
