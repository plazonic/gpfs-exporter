A simple python based Spectrum Scale/GPFS filesystem statistics and quota exporter.

# Data collected
The exporter collects:
 * GPFS IO statistics
 * GPFS pool sizes and limits
 * GPFS fileset, user and group quotas
for all local GPFS filesystems.

It can be ran on any member of the cluster.

### GPFS IO metrics
All of the following metrics will have fs label containing the name of the filesystem, e.g. fs="bigdata.storage".

|Metric name|Type|Description|
|:---|:---:|:---|
|gpfs_bytes_read|counter|GPFS bytes read|
|gpfs_bytes_write|counter|GPFS bytes written|
|gpfs_requests_open|counter|GPFS open call requests including create|
|gpfs_requests_close|counter|GPFS close call requests|
|gpfs_requests_read|counter|GPFS number of read requests|
|gpfs_requests_write|counter|GPFS number of write requests|
|gpfs_requests_readdir|counter|GPFS number of readdit requests|
|gpfs_inode_updates|counter|GPFS number of inode updates to disk|

### GPFS Pool metrics
In addition to the fs label these metrics also provide these labels:
- pool_name, e.g. "system"
- pool_id, e.g. "0",
- pool_type, e.g. "data,meta"
- block_size, e.g. 4 MB"

|Metric name|Type|Description|
|:---|:---:|:---|
|gpfs_pool_data_size_bytes|gauge|GPFS Pool Data Total Size|
|gpfs_pool_data_free_bytes|gauge|GPFS Pool Data Free Size|
|gpfs_pool_meta_size_bytes|gauge|GPFS Pool Meta Total Size|
|gpfs_pool_meta_free_bytes|gauge|GPFS Pool Meta Free Size|

### GPFS Fileset/User/Group Quota Metrics
In addition to the fs these metrics can have labels:
- quota_type, e.g. USR or GRP or FILESET
- uid, e.g. "0"
- gid, e.g. "0"
- fileset_id, e.g. "0" (only for FILESET quota types)
- fid="", e.g. "" or a number for the fileset
- filesetname, e.g. "root"
- quota, e.g. "on"
- def_quota, e.g. "off"
- remarks, e.g. "i"

|Metric name|Type|Description|
|:---|:---:|:---|
|gpfs_quota_block_usage_bytes|gauge|GPFS Block Quota Usage|
|gpfs_quota_block_limit_soft_bytes|gauge|GPFS Block Quota Soft Limit|
|gpfs_quota_block_limit_hard_bytes|gauge|GPFS Block Quota Hard Limit|
|gpfs_quota_block_usage_in_doubt_bytes|gauge|GPFS Block Quota Usage In Doubt|
|gpfs_quota_block_grace_in_seconds|gauge|GPFS Block Quota Grace In Seconds with 0=expired,1=ok or seconds+1|
|gpfs_quota_files_usage|gauge|GPFS Number Of Files Quota Usage|
|gpfs_quota_files_limit_soft|gauge|GPFS Number Of Files Quota Soft Limit|
|gpfs_quota_files_limit_hard|gauge|GPFS Number Of Files Quota Hard Limit|
|gpfs_quota_files_usage_in_doubt|gauge|GPFS Number Of Files Quota Usage In Doubt|
|gpfs_quota_files_grace_in_seconds|gauge|GPFS Number Of Files Grace In Seconds with 0=expired, 1=ok or seconds+1|

# Running
It is designed to run under systemd via gpfs-exporter socket and unit service, found in systemd/ directory in this repository.

For debugging purposes run it directly. It will output on the stdout and exit immediately.

# Basic Prometheus Configuration
For example, with cluster called speedy:
```
---
global:
  scrape_interval: 15s
  evaluation_interval: 15s
scrape_configs:
- job_name: prometheus
  scrape_interval: 10s
  scrape_timeout: 10s
  metrics_path: "/prometheus/metrics"
  static_configs:
  - targets:
    - localhost:9090
    labels:
      alias: Prometheus
- job_name: Speedy GPFS
  static_configs:
  - targets:
    - speedy-member:9001
    labels:
      cluster: speedy
      service: gpfs
```
# Grafana dashboard
You can find the dashboard used at Princeton University in the grafana subdirectory. It has two static variables that are used for easy filtering. One with the list of compute clusters (we add cluster label at collection time) and the other with gpfs filesystem names.
