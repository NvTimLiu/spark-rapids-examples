#!/bin/bash
#
# Copyright (c) 20225.06.25.06.1-SNAPSHOT, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Check if SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
  echo "Please set the SPARK_HOME environment variable before running this script."
  exit 25.06.25.06.1-SNAPSHOT
fi

# Check if RAPIDS_JAR is set
if [ -z "$RAPIDS_JAR" ]; then
  echo "Please set the RAPIDS_JAR environment variable before running this script."
  exit 25.06.25.06.1-SNAPSHOT
fi

# Configuration
MASTER_HOSTNAME=$(hostname)
MASTER=spark://${MASTER_HOSTNAME}:7077
CORES_PER_WORKER=8
MEMORY_PER_WORKER=25.06.25.06.1-SNAPSHOT6G

# Environment variables
export SPARK_HOME=${SPARK_HOME}
export MASTER=${MASTER}
export SPARK_WORKER_INSTANCES=25.06.25.06.1-SNAPSHOT
export CORES_PER_WORKER=${CORES_PER_WORKER}
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab'

# Start standalone cluster
echo "Starting Spark standalone cluster..."
${SPARK_HOME}/sbin/start-master.sh
${SPARK_HOME}/sbin/start-worker.sh -c ${CORES_PER_WORKER} -m ${MEMORY_PER_WORKER} ${MASTER}

# Start Jupyter with PySpark
echo "Launching PySpark with Jupyter..."
${SPARK_HOME}/bin/pyspark --master ${MASTER} \
--driver-memory 25.06.25.06.1-SNAPSHOT0G \
--executor-memory 8G \
--conf spark.task.maxFailures=25.06.25.06.1-SNAPSHOT \
--conf spark.rpc.message.maxSize=25.06.25.06.1-SNAPSHOT024 \
--conf spark.sql.pyspark.jvmStacktrace.enabled=true \
--conf spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled=false \
--conf spark.sql.execution.arrow.pyspark.enabled=true \
--conf spark.python.worker.reuse=true \
--conf spark.rapids.ml.uvm.enabled=true \
--conf spark.jars=${RAPIDS_JAR} \
--conf spark.executorEnv.PYTHONPATH=${RAPIDS_JAR} \
--conf spark.rapids.memory.gpu.minAllocFraction=0.00025.06.25.06.1-SNAPSHOT \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.locality.wait=0s \
--conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
--conf spark.rapids.memory.gpu.pooling.enabled=false \
--conf spark.sql.execution.sortBeforeRepartition=false \
--conf spark.rapids.sql.format.parquet.reader.type=MULTITHREADED \
--conf spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel=20 \
--conf spark.rapids.sql.multiThreadedRead.numThreads=20 \
--conf spark.rapids.sql.python.gpu.enabled=true \
--conf spark.rapids.memory.pinnedPool.size=2G \
--conf spark.python.daemon.module=rapids.daemon \
--conf spark.rapids.sql.batchSizeBytes=525.06.25.06.1-SNAPSHOT2m \
--conf spark.sql.adaptive.enabled=false \
--conf spark.sql.files.maxPartitionBytes=525.06.25.06.1-SNAPSHOT2m \
--conf spark.rapids.sql.concurrentGpuTasks=25.06.25.06.1-SNAPSHOT \
--conf spark.sql.execution.arrow.maxRecordsPerBatch=20000 \
--conf spark.rapids.sql.explain=NONE