# Makefile - General Docker Lifecycle & Spark Submission
# Manages Hadoop/Spark cluster startup, shutdown, and job submission

# ============================================================================
# VARIABLES
# ============================================================================
DOCKER_COMPOSE := docker-compose
NAMENODE := namenode
SPARK_MASTER := spark-master
SPARK_WORKER := spark-worker
DATANODE := datanode
RESOURCEMANAGER := resourcemanager

APP_PATH := /app
LOCAL_SCRIPTS := ./src
REMOTE_SCRIPTS := $(APP_PATH)/src

SCRIPT_NAME ?= weather_spark_process.py
SPARK_MASTER_URL := spark://$(SPARK_MASTER):7077

# ============================================================================
# PHONY TARGETS
# ============================================================================
.PHONY: help up down restart status logs shell submit clean

# ============================================================================
# HELP TARGET
# ============================================================================
help:
	@echo "=========================================="
	@echo "Spark + Hadoop Docker Cluster Manager"
	@echo "=========================================="
	@echo ""
	@echo "LIFECYCLE COMMANDS:"
	@echo "  make up              - Start all Docker containers"
	@echo "  make down            - Stop all Docker containers"
	@echo "  make restart         - Restart all containers (graceful)"
	@echo "  make status          - Show running containers status"
	@echo "  make logs            - Follow Docker Compose logs"
	@echo ""
	@echo "SHELL ACCESS:"
	@echo "  make shell node=namenode    - Open shell in namenode container"
	@echo "  make shell node=datanode    - Open shell in datanode container"
	@echo "  make shell node=spark-master - Open shell in spark-master container"
	@echo ""
	@echo "SPARK SUBMISSION:"
	@echo "  make submit SCRIPT_NAME=script.py - Copy script and run spark-submit"
	@echo "  Default script: weather_spark_process.py"
	@echo ""
	@echo "MAINTENANCE:"
	@echo "  make clean           - Stop containers and clean temporary files"
	@echo ""
	@echo "VARIABLES (override on command line):"
	@echo "  SCRIPT_NAME          - Python script to submit (default: $(SCRIPT_NAME))"
	@echo "  SPARK_MASTER_URL     - Spark master URL (default: $(SPARK_MASTER_URL))"
	@echo ""

# ============================================================================
# LIFECYCLE TARGETS
# ============================================================================

# Start all containers
up:
	@echo "⬆️  Starting Docker containers..."
	$(DOCKER_COMPOSE) up -d
	@echo "✓ Containers started"
	@sleep 3
	@make status

# Stop all containers
down:
	@echo "⬇️  Stopping Docker containers..."
	$(DOCKER_COMPOSE) down
	@echo "✓ Containers stopped"

# Restart containers (graceful restart)
restart: down
	@echo "🔄 Restarting containers..."
	@sleep 2
	make up

# Show container status
status:
	@echo "Container Status:"
	@echo "================="
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "Container Details:"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "namenode|spark|datanode|resourcemanager" || true

# Follow Docker Compose logs
logs:
	@echo "Following Docker Compose logs (Ctrl+C to stop)..."
	$(DOCKER_COMPOSE) logs -f

# ============================================================================
# SHELL ACCESS TARGET
# ============================================================================

# Generic shell target: make shell node=container_name
shell:
	@if [ -z "$(node)" ]; then \
		echo "❌ Error: node parameter required"; \
		echo "Usage: make shell node=namenode"; \
		echo "       make shell node=spark-master"; \
		exit 1; \
	fi
	@echo "🔌 Connecting to $(node) container..."
	docker exec -it $(node) /bin/bash

# ============================================================================
# SPARK SUBMISSION TARGET
# ============================================================================

# Copy script to spark-master and run spark-submit
submit:
	@if [ ! -f "$(LOCAL_SCRIPTS)/$(SCRIPT_NAME)" ]; then \
		echo "❌ Error: Script not found: $(LOCAL_SCRIPTS)/$(SCRIPT_NAME)"; \
		exit 1; \
	fi
	@echo "📤 Copying $(SCRIPT_NAME) to $(SPARK_MASTER)..."
	docker cp $(LOCAL_SCRIPTS)/$(SCRIPT_NAME) $(SPARK_MASTER):$(REMOTE_SCRIPTS)/
	@echo "▶️  Running spark-submit on $(SPARK_MASTER)..."
	docker exec $(SPARK_MASTER) /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_URL) \
		--deploy-mode client \
		$(REMOTE_SCRIPTS)/$(SCRIPT_NAME)
	@echo "✓ Spark job completed"

# ============================================================================
# MAINTENANCE TARGETS
# ============================================================================

# Clean up containers and temporary files
clean: down
	@echo "🧹 Cleaning up..."
	@echo "✓ Containers stopped and removed"
