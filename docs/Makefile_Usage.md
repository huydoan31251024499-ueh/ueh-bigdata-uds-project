# Makefile Usage Guide - Hadoop/Spark Cluster Management

## Overview

Two complementary Makefiles manage your Docker-based Hadoop/Spark cluster:

- **`Makefile`** - General Docker lifecycle and Spark job submission
- **`Makefile.hdfs`** - HDFS diagnostics and data recovery

## File 1: Makefile (General Tasks)

### Startup & Shutdown

```bash
# Start all containers
make up

# Check status
make status

# Stop containers
make down

# Graceful restart
make restart

# Follow logs
make logs
```

### Shell Access

Access any container directly for manual debugging:

```bash
# Access namenode
make shell node=namenode

# Access spark-master
make shell node=spark-master

# Access datanode
make shell node=datanode

# Access resource manager
make shell node=resourcemanager
```

### Spark Job Submission

Submit Python scripts to Spark with automatic copying and execution:

```bash
# Submit with default script (weather_spark_process.py)
make submit

# Submit with custom script
make submit SCRIPT_NAME=my_analysis.py

# Verify script exists before submitting
make submit SCRIPT_NAME=nonexistent.py  # ❌ Will show error
```

**What happens:**
1. Verifies script exists locally in `./src/`
2. Copies script to Spark master container
3. Runs `spark-submit` with cluster configuration
4. Shows output in terminal

---

## File 2: Makefile.hdfs (HDFS Troubleshooting)

### Diagnostics

```bash
# View detailed filesystem report
make -f Makefile.hdfs report

# Check filesystem integrity and find corrupt blocks
make -f Makefile.hdfs check-fs
```

**Report output shows:**
- Configured capacity
- DFS usage
- DataNode status
- Safe mode status

**Check-fs output shows:**
- Corrupt blocks (if any)
- Missing blocks
- Replication status

### Data Management

#### Reset entire project directory

When experiencing persistent HDFS errors:

```bash
# Remove /user/doanquochuy/uds-project and recreate empty structure
make -f Makefile.hdfs reset-hdfs
```

**Use cases:**
- After `BlockMissingException` errors
- When datanode marked as dead
- To start fresh without Docker volume cleanup

#### Reload specific files

Replace a single file without resetting entire project:

```bash
# Reload weather data
make -f Makefile.hdfs reload FILE=hcmc_weather_raw.csv

# Reload orders data
make -f Makefile.hdfs reload FILE=uds_orders.csv

# Reload custom CSV
make -f Makefile.hdfs reload FILE=my_dataset.csv
```

**Steps performed:**
1. Removes old file from HDFS
2. Copies fresh copy from local machine to container
3. Uploads to HDFS
4. Verifies file exists

### Recovery Operations

```bash
# Force exit from safe mode (if stuck)
make -f Makefile.hdfs safe-mode

# Clear namenode cache
make -f Makefile.hdfs clean-cache
```

---

## Typical Workflows

### Workflow 1: Fresh Start with Data Upload

```bash
# 1. Start cluster
make up

# 2. Reset HDFS structure
make -f Makefile.hdfs reset-hdfs

# 3. Upload all data files
make -f Makefile.hdfs reload FILE=hcmc_weather_raw.csv
make -f Makefile.hdfs reload FILE=uds_orders.csv

# 4. Run spark job
make submit

# 5. Check results
make logs
```

### Workflow 2: Fixing Data Issues

```bash
# 1. Check what's wrong
make -f Makefile.hdfs check-fs
make -f Makefile.hdfs report

# 2. If corrupt blocks found, reset
make -f Makefile.hdfs reset-hdfs

# 3. Reload affected files
make -f Makefile.hdfs reload FILE=hcmc_weather_raw.csv

# 4. Re-run job
make submit
```

### Workflow 3: Quick File Update

```bash
# Only need to update one file? No need to reset:
make -f Makefile.hdfs reload FILE=uds_orders.csv

# Then re-run job
make submit
```

### Workflow 4: Container Maintenance

```bash
# Check if containers are healthy
make status

# Look at logs for errors
make logs

# If containers seem stuck, restart
make restart

# Or access shell for manual inspection
make shell node=namenode
```

---

## Variable Configuration

### Makefile Variables

Edit these in the `Makefile` to customize:

```makefile
DOCKER_COMPOSE := docker-compose    # Docker Compose binary
NAMENODE := namenode                # NameNode container name
SPARK_MASTER := spark-master        # Spark Master container name
LOCAL_SCRIPTS := ./src              # Local script directory
REMOTE_SCRIPTS := /app/src          # Remote script directory in container
SCRIPT_NAME ?= weather_spark_process.py  # Default script for submission
```

Override on command line:

```bash
make submit SCRIPT_NAME=other_script.py
make shell node=datanode
```

### Makefile.hdfs Variables

```makefile
NAMENODE := namenode                # NameNode container name
HDFS_PROJECT_DIR := /user/doanquochuy/uds-project  # HDFS project path
HDFS_DATA_DIR := $(HDFS_PROJECT_DIR)/data/raw      # HDFS data directory
LOCAL_DATA_DIR := ./data/raw        # Local data directory
```

---

## Common Error Scenarios

### Error: BlockMissingException

```bash
# 1. Check status
make -f Makefile.hdfs report

# 2. If datanode is dead, reset HDFS
make -f Makefile.hdfs reset-hdfs

# 3. Reload all files
make -f Makefile.hdfs reload FILE=hcmc_weather_raw.csv
make -f Makefile.hdfs reload FILE=uds_orders.csv

# 4. Re-submit job
make submit
```

### Error: File Not Found in HDFS

```bash
# Check if file was uploaded
make -f Makefile.hdfs check-fs

# If missing, reload it
make -f Makefile.hdfs reload FILE=hcmc_weather_raw.csv
```

### Error: Script Not Found

```bash
# ❌ Fails - script doesn't exist
make submit SCRIPT_NAME=missing.py

# ✓ Works - verify script exists first
ls ./src/weather_spark_process.py
make submit
```

### Error: HDFS Safe Mode Stuck

```bash
# Force leave safe mode
make -f Makefile.hdfs safe-mode

# Check status
make -f Makefile.hdfs report
```

---

## Help Commands

```bash
# Show all Makefile targets
make help

# Show all Makefile.hdfs targets
make -f Makefile.hdfs help

# List available Python scripts
ls ./src/

# List available CSV files
ls ./data/raw/
```

---

## Tips & Best Practices

### ✅ DO:

- Run `make status` before submitting jobs
- Use `make -f Makefile.hdfs report` to diagnose issues
- Run `make restart` if containers become unresponsive
- Use `make -f Makefile.hdfs reload FILE=...` for quick updates
- Check logs with `make logs` to understand failures

### ❌ DON'T:

- Mix manual `docker exec` commands with Make targets
- Skip `make status` checks before important runs
- Delete files directly - use `make -f Makefile.hdfs reload`
- Ignore warnings in the `make -f Makefile.hdfs report` output
- Run `make submit` without verifying data is loaded

---

## Integration with CI/CD

These Makefiles can be integrated into deployment pipelines:

```bash
#!/bin/bash
# deploy.sh - CI/CD deployment script

make down               # Clean shutdown
make up                # Fresh start
make -f Makefile.hdfs reset-hdfs  # Clean HDFS
make -f Makefile.hdfs reload FILE=hcmc_weather_raw.csv
make -f Makefile.hdfs reload FILE=uds_orders.csv
make submit            # Run job
if [ $? -eq 0 ]; then
    echo "✓ Deployment successful"
else
    echo "✗ Deployment failed"
    exit 1
fi
```

---

## Troubleshooting the Makefiles

### "command not found: docker"

Ensure Docker is installed and in PATH:
```bash
which docker
docker --version
```

### "permission denied" errors

Ensure your user is in the docker group:
```bash
sudo usermod -aG docker $USER
newgrp docker
```

### Tab indentation errors

Makefiles require tabs (not spaces). If you see:
```
makefile:XX: recipe for target '...' failed
```

Check for spaces instead of tabs. Most editors can convert them.

### Variable substitution not working

Ensure you're using Make variable syntax:

```bash
# ✓ Correct - uses Make variables
make submit SCRIPT_NAME=script.py

# ❌ Wrong - shell variables don't work in Make
SCRIPT=script.py make submit
```

