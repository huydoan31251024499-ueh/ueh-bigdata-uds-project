# Spark + HDFS on Docker - Troubleshooting Guide

## Common Errors & Fixes

### 1. **BlockMissingException: No live nodes contain current block**
**Cause:** Datanode crashed or became unresponsive  
**Fix:**
```bash
make clean-volumes  # Remove corrupt HDFS state
make full-run       # Fresh restart with data upload
```

### 2. **java.io.IOException: Cannot connect to namenode**
**Cause:** Container networking issue or namenode not ready  
**Fix:**
```bash
make restart       # Restart all containers
make health-check  # Verify services are ready
```

### 3. **File not found in HDFS**
**Cause:** Data upload failed silently  
**Fix:**
```bash
docker exec namenode hdfs dfs -ls /user/doanquochuy/uds-project/data/raw/
# If empty, run:
make upload-data
```

### 4. **Datanode marked as dead after a few minutes**
**Cause:** 
- Datanode process crashed (memory/CPU issue)
- Heartbeat timeout (network issue)
- Block corruption

**Fix:**
```bash
# Check datanode status
docker exec namenode hdfs dfsadmin -report

# Restart just the datanode
docker-compose restart datanode

# If that fails, do full clean restart
make clean-volumes && make up
```

### 5. **Replication factor mismatch**
**Cause:** Default replication = 3, but only 1 datanode available  
**Status:** Already fixed in `hadoop.env`:
```
HDFS_CONF_dfs_replication=1
```

## Best Practices

### ✅ Recommended Workflow
```bash
# First time setup
make full-run

# Subsequent runs (if containers still running)
make upload-data
make spark-submit

# If any errors occur
make restart
make full-run
```

### ✅ Avoid These Mistakes
❌ Don't: Run spark-submit without uploading data first  
✅ Do: Use `make full-run` or `make upload-data` before jobs

❌ Don't: Keep containers running for weeks  
✅ Do: Restart periodically with `make restart`

❌ Don't: Ignore datanode health  
✅ Do: Check status with `make health-check` before running jobs

❌ Don't: Reuse old HDFS volumes if having issues  
✅ Do: Use `make clean-volumes` to start fresh

## Performance Tips

### Increase HDFS Heartbeat Timeout
If datanodes keep dying, edit `docker-compose.yml`:
```yaml
environment:
  - HDFS_CONF_dfs_heartbeat_interval=1
  - HDFS_CONF_dfs_client_socket_timeout=180000
```

### Increase Memory for Containers
For larger datasets, edit `docker-compose.yml`:
```yaml
spark-master:
  environment:
    SPARK_DRIVER_MEMORY: "2g"
    SPARK_EXECUTOR_MEMORY: "2g"
```

### Use Smaller Blocks for Small Files
Add to `hadoop.env`:
```
HDFS_CONF_dfs_blocksize=67108864  # 64MB (default is 128MB)
```

## Debugging Commands

### Check HDFS Filesystem Report
```bash
docker exec namenode hdfs dfsadmin -report
```

### View Datanode Logs
```bash
docker logs datanode | tail -50
```

### Check File Blocks
```bash
docker exec namenode hdfs fsck /user/doanquochuy/uds-project/data/raw/ -files -blocks
```

### Force Datanode Registration
```bash
docker exec namenode hdfs dfsadmin -safemode leave
docker exec datanode hdfs datanode -initializeDatanode
```

### Monitor HDFS Web UI
```
http://localhost:9870/  # NameNode
http://localhost:9864/  # DataNode (if accessible)
```

## Why the Fresh Start Worked

The successful run used:
1. **Clean shutdown** (`docker-compose down`)
2. **Volume removal** (implicit when containers removed)
3. **Fresh datanode initialization** (new volumes)
4. **Proper sequencing** (data uploaded AFTER HDFS is healthy)
5. **Health check** (ensured services ready before job)

This sequence eliminates:
- Stale metadata
- Dead datanode processes
- Block corruption
- State inconsistencies
