#!/usr/bin/python3.6
import os
import sys
import time
import socket
import http.server
import re
import csv
from subprocess import (PIPE, STDOUT, Popen, CalledProcessError, check_output)

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

# {'name': 'system', 'id': '0', 'blksize': '4 MB', 'data': 'yes', 'meta': 'yes', 'total_data': '129030701056', 'free_data': '33869172736', 'free_data_percent': '26', 'total_meta': '129030701056', 'free_meta': '38223740928'}
pool_mapping = {
  'total_data': {
    'name': 'gpfs_pool_data_size_bytes',
    'description': 'GPFS Pool Data Total Size',
    'type': 'gauge'
  },
  'free_data': {
    'name': 'gpfs_pool_data_free_bytes',
    'description': 'GPFS Pool Data Free Size',
    'type': 'gauge'
  },
  'total_meta': {
    'name': 'gpfs_pool_meta_size_bytes',
    'description': 'GPFS Pool Meta Total Size',
    'type': 'gauge'
  },
  'free_meta': {
    'name': 'gpfs_pool_meta_free_bytes',
    'description': 'GPFS Pool Meta Free Size',
    'type': 'gauge'
  },
}
# used to map data,meta in that order
pool_type = {
  'yes': {
    'yes': 'data,meta',
    'no': 'data',
  },
  'no': {
    'yes': 'meta',
    'no': '',
  }
}

# ('filesystemName', 'tiger2.gpfs'), ('quotaType', 'FILESET'), ('id', '5'), ('name', '5'), ('blockUsage', '1745619584'), ('blockQuota', '10737418240'), ('blockLimit', '10737418240'), ('blockInDoubt', '0'), ('blockGrace', 'none'), ('filesUsage', '3476'), ('filesQuota', '0'), ('filesLimit', '0'), ('filesInDoubt', '0'), ('filesGrace', 'none'), ('remarks', 'e'), ('quota', 'on'), ('defQuota', 'off'), ('fid', ''), ('filesetname', '')
quota_mapping = {
  'blockUsage': {
    'name': 'gpfs_quota_block_usage_bytes',
    'description': 'GPFS Block Quota Usage',
    'type': 'gauge',
    'multiply': 1024,
  },
  'blockQuota': {
    'name': 'gpfs_quota_block_limit_soft_bytes',
    'description': 'GPFS Block Quota Soft Limit',
    'type': 'gauge',
    'multiply': 1024,
  },
  'blockLimit': {
    'name': 'gpfs_quota_block_limit_hard_bytes',
    'description': 'GPFS Block Quota Hard Limit',
    'type': 'gauge',
    'multiply': 1024,
  },
  'blockInDoubt': {
    'name': 'gpfs_quota_block_usage_in_doubt_bytes',
    'description': 'GPFS Block Quota Usage In Doubt',
    'type': 'gauge',
    'multiply': 1024,
  },
  'blockGrace': {
    'name': 'gpfs_quota_block_grace_in_seconds',
    'description': 'GPFS Block Quota Grace In Seconds with 0=ok, 1=expired or seconds+1',
    'type': 'gauge',
    'multiply': 'grace',
  },
  'filesUsage': {
    'name': 'gpfs_quota_files_usage',
    'description': 'GPFS Number Of Files Quota Usage',
    'type': 'gauge',
    'multiply': 1,
  },
  'filesQuota': {
    'name': 'gpfs_quota_files_limit_soft',
    'description': 'GPFS Number Of Files Quota Soft Limit',
    'type': 'gauge',
    'multiply': 1,
  },
  'filesLimit': {
    'name': 'gpfs_quota_files_limit_hard',
    'description': 'GPFS Number Of Files Quota Hard Limit',
    'type': 'gauge',
    'multiply': 1,
  },
  'filesInDoubt': {
    'name': 'gpfs_quota_files_usage_in_doubt',
    'description': 'GPFS Number Of Files Quota Usage In Doubt',
    'type': 'gauge',
    'multiply': 1,
  },
  'filesGrace': {
    'name': 'gpfs_quota_files_grace_in_seconds',
    'description': 'GPFS Number Of Files Grace In Seconds with 0=ok, 1=expired or seconds=1',
    'type': 'gauge',
    'multiply': 'grace',
  },
}
quota_type = {
  'USR': 'uid',
  'GRP': 'gid',
  'FILESET': 'fileset_id',
}

DEVNULL = open(os.devnull, 'w')

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
        raise CalledProcessError(retcode, '/usr/lpp/mmfs/bin/mmpmon', output=all_stats)
    local_fs = []
    pool_stats = {}
    # double deep dict with filesets['tiger2.gpfs'][1] = { 'name': 'CRYOEM', 'status': 'Linked', 'parent': 0, 'path'.... }
    filesets = {}
    # raw quotas
    quotas = []
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
            fs = d['_fs_']
            per_host[n][fs] = d
            if '_tigress' not in fs and '_projects' not in fs and fs not in local_fs:
                local_fs += [fs]
    if local_fs:
        # example data line
        # Name                    Id   BlkSize Data Meta Total Data in (KB)   Free Data in (KB)   Total Meta in (KB)    Free Meta in (KB)
        # system                   0      4 MB  yes  yes   129030701056    33869172736 ( 26%)   129030701056    38223740928 ( 30%)
        pat = re.compile('(?P<name>\S+)\s+(?P<id>\d+)+\s+(?P<blksize>\d+\s*\S+)\s+(?P<data>\S+)\s+(?P<meta>\S+)\s+(?P<total_data>\d+)\s+(?P<free_data>\d+)\s+\\(\s*(?P<free_data_percent>\d+)%\\)\s+(?P<total_meta>\d+)\s+(?P<free_meta>\d+)\s+')
        for one_fs in local_fs:
            try:
                # Collect pool usage
                for l in check_output(["/usr/lpp/mmfs/bin/mmlspool", one_fs],stderr=DEVNULL).decode().split('\n'):
                    m = pat.match(l)
                    if m:
                        n = one_fs + " " + m['name']
                        pool_stats[n] = m.groupdict()
                        pool_stats[n]['type'] = pool_type[m['data']][m['meta']]
                        pool_stats[n]['fs'] = one_fs
                # Next, get basic info on all filesets in this one_fs
                filesets[one_fs] = {}
                for l in csv.DictReader(check_output(["/usr/lpp/mmfs/bin/mmlsfileset", one_fs, '-Y'],stderr=DEVNULL).decode().split('\n'), delimiter=':'):
                    filesets[one_fs][l['id']] = {
                        'fs': l['filesystemName'],
                        'name': l['filesetName'],
                        'status': l['status'],
                        'path': l['path'],
                        'parentId': l['parentId'],
                    }
                # Finally, collect all quotas on this filesystem
                for l in csv.DictReader(check_output(["/usr/lpp/mmfs/bin/mmrepquota", '-Y', '-n', one_fs],stderr=DEVNULL).decode().split('\n'), delimiter=':'):
                    quotas.append(l)
            except:
                pass
    return (per_host, pool_stats, filesets, quotas)

def append_descriptions(all, ss):
    all.append("# HELP %s %s" %(ss['name'], ss['description']))
    all.append("# TYPE %s %s" % (ss['name'], ss['type']))

def real_value(val, multiply):
    if multiply == 'grace':
        if val == 'none':
            return 0
        elif val == 'expired':
            return 1
        elif 'day' in val:
            return 86400 * int(val.split(' ')[0]) + 1
        elif 'hour' in val:
            return 3600 * int(val.split(' ')[0]) + 1
        elif 'minute' in val:
            return 60 * int(val.split(' ')[0]) + 1
        elif 'second' in val:
            return int(val.split(' ')[0]) + 1
        else:
            print("ERROR: Got unknown grace=%s" % val, file=sys.stderr)
            return 1
    else:
        return int(val)*multiply

def get_prom_stats(all_stats):
    stats, pools, filesets, quotas = all_stats
    all = []
    sorted_hosts = sorted(stats.keys())
    for s,ss in stat_mapping.items():
        append_descriptions(all, ss)
        for h in sorted_hosts:
            for f in sorted(stats[h].keys()):
                one = stats[h][f]
                if s in one:
                    all.append('%s{fs="%s", host="%s"} %s %s' % (ss['name'], f, h, one[s], one['t_miliseconds']))
    for s,ss in pool_mapping.items():
        ss = pool_mapping[s]
        append_descriptions(all, ss)
        for pn, p in pools.items():
            if s in p:
                all.append('%s{fs="%s", pool_name="%s", pool_id="%s", pool_type="%s", block_size="%s"} %d' % (ss['name'], p['fs'], p['name'], p['id'], p['type'], p['blksize'], int(p[s])*1024))
    for s, ss in quota_mapping.items():
        append_descriptions(all, ss)
        for q in quotas:
            # OrderedDict([('mmrepquota', 'mmrepquota'), ('', ''), ('HEADER', '0'), ('version', '1'), ('reserved', ''), ('filesystemName', 'tiger2.gpfs'), ('quotaType', 'USR'), ('id', '94970'), ('name', '94970'), ('blockUsage', '298887936'), ('blockQuota', '524288000'), ('blockLimit', '536870912'), ('blockInDoubt', '0'), ('blockGrace', 'none'), ('filesUsage', '149912'), ('filesQuota', '1990000'), ('filesLimit', '2000000'), ('filesInDoubt', '0'), ('filesGrace', 'none'), ('remarks', 'd_fset'), ('quota', 'on'), ('defQuota', 'on'), ('fid', '0'), ('filesetname', '0')])
            # OrderedDict([('mmrepquota', 'mmrepquota'), ('', ''), ('HEADER', '0'), ('version', '1'), ('reserved', ''), ('filesystemName', 'tiger2.gpfs'), ('quotaType', 'GRP'), ('id', '30051'), ('name', '30051'), ('blockUsage', '418134272'), ('blockQuota', '0'), ('blockLimit', '0'), ('blockInDoubt', '0'), ('blockGrace', 'none'), ('filesUsage', '393'), ('filesQuota', '0'), ('filesLimit', '0'), ('filesInDoubt', '0'), ('filesGrace', 'none'), ('remarks', 'i'), ('quota', 'on'), ('defQuota', 'off'), ('fid', '0'), ('filesetname', '0')])
            # OrderedDict([('mmrepquota', 'mmrepquota'), ('', ''), ('HEADER', '0'), ('version', '1'), ('reserved', ''), ('filesystemName', 'tiger2.gpfs'), ('quotaType', 'FILESET'), ('id', '5'), ('name', '5'), ('blockUsage', '1745619584'), ('blockQuota', '10737418240'), ('blockLimit', '10737418240'), ('blockInDoubt', '0'), ('blockGrace', 'none'), ('filesUsage', '3476'), ('filesQuota', '0'), ('filesLimit', '0'), ('filesInDoubt', '0'), ('filesGrace', 'none'), ('remarks', 'e'), ('quota', 'on'), ('defQuota', 'off'), ('fid', ''), ('filesetname', '')])
            fs = q['filesystemName']
            fid = q['fid']
            if fid != '' and fs in filesets and fid in filesets[fs]:
                filesetname = filesets[fs][fid]['name']
            elif fid == '' and q['quotaType'] == 'FILESET' and fs in filesets and q['id'] in filesets[fs]:
                filesetname = filesets[fs][q['id']]['name']
            else:
                filesetname = ''
            all.append('%s{fs="%s", quota_type="%s", %s="%s", fid="%s", filesetname="%s", quota="%s", def_quota="%s", remarks="%s"} %d' % (ss['name'], fs, q['quotaType'], quota_type[q['quotaType']], q['id'], fid, filesetname, q['quota'], q['defQuota'], q['remarks'], real_value(q[s],ss['multiply'])))
    return all

def print_prom_stats():
    for i in get_prom_stats(get_stats()):
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
        print("Running outside systemd - will output one data dump and exit.")
        print_prom_stats()
