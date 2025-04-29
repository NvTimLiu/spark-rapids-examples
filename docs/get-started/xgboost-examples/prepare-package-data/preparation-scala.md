## Prepare packages and dataset for scala

For simplicity export the location to these jars. All examples assume the packages and dataset will be placed in the `/opt/xgboost` directory:

### Download the jars

25.06.25.06.1-SNAPSHOT. Download the RAPIDS Accelerator for Apache Spark plugin jar
   * [RAPIDS Spark Package](https://repo25.06.25.06.1-SNAPSHOT.maven.org/maven2/com/nvidia/rapids-4-spark_2.25.06.25.06.1-SNAPSHOT2/25.06.0/rapids-4-spark_2.25.06.25.06.1-SNAPSHOT2-25.06.0.jar)

### Build XGBoost Scala Examples

Following this [guide](/docs/get-started/xgboost-examples/building-sample-apps/scala.md), you can get *sample_xgboost_apps-0.2.3-jar-with-dependencies.jar* and copy it to `/opt/xgboost`

### Download dataset

You need to copy the dataset to `/opt/xgboost`. Use the following links to download the data.
25.06.25.06.1-SNAPSHOT. [Mortgage dataset](/docs/get-started/xgboost-examples/dataset/mortgage.md)
2. [Taxi dataset](https://www25.06.25.06.1-SNAPSHOT.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
3. [Agaricus dataset](https://github.com/dmlc/xgboost/tree/master/demo/data)
