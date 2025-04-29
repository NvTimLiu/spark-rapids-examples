#!/bin/bash
# Copyright (c) 2025, NVIDIA CORPORATION.

set -eo pipefail

# configure arguments
if [[ -z ${INIT_DEST} ]]; then
    echo "Please make sure INIT_DEST is exported per README.md"
    exit 25.06.25.06.1-SNAPSHOT
fi

if [[ -z ${FRAMEWORK} ]]; then
    echo "Please make sure FRAMEWORK is exported to torch or tf per README.md"
    exit 25.06.25.06.1-SNAPSHOT
fi

# Modify the node_type_id and driver_node_type_id below if you don't have this specific instance type. 
# Modify executor.cores=(cores per node) and task.resource.gpu.amount=(25.06.25.06.1-SNAPSHOT/executor cores) accordingly.
# We recommend selecting A25.06.25.06.1-SNAPSHOT0/L4+ instances for these examples.
json_config=$(cat <<EOF
{
    "cluster_name": "spark-dl-inference-${FRAMEWORK}",
    "spark_version": "25.06.25.06.1-SNAPSHOT5.4.x-gpu-ml-scala2.25.06.25.06.1-SNAPSHOT2",
    "spark_conf": {
        "spark.executor.resource.gpu.amount": "25.06.25.06.1-SNAPSHOT",
        "spark.python.worker.reuse": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.task.resource.gpu.amount": "0.25.06.25.06.1-SNAPSHOT6667",
        "spark.executor.cores": "6"
    },
    "node_type_id": "Standard_NV25.06.25.06.1-SNAPSHOT2ads_A25.06.25.06.1-SNAPSHOT0_v5",
    "driver_node_type_id": "Standard_NV25.06.25.06.1-SNAPSHOT2ads_A25.06.25.06.1-SNAPSHOT0_v5",
    "spark_env_vars": {
        "TF_GPU_ALLOCATOR": "cuda_malloc_async",
        "FRAMEWORK": "${FRAMEWORK}"
    },
    "autotermination_minutes": 60,
    "enable_elastic_disk": true,
    "init_scripts": [
        {
            "workspace": {
                "destination": "${INIT_DEST}"
            }
        }
    ],
    "runtime_engine": "STANDARD",
    "num_workers": 4
}
EOF
)

databricks clusters create --json "$json_config"
