#!/usr/bin/env bash

if [ -z $FRACTAL_HOME ]; then
	echo "FRACTAL_HOME is unset"
	exit 1
else
	echo "FRACTAL_HOME is set to $FRACTAL_HOME"
fi

if [ -z $SPARK_HOME ]; then
	echo "SPARK_HOME is unset"
	exit 1
else
	echo "SPARK_HOME is set to $SPARK_HOME"
fi

echo "Setting SPARK_SUBMIT_OPTS to -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005

echo "Running gradlew"
bash -c "./gradlew assemble"

echo "Updating fractal-core and fractal-apps JARS on HDFS"
bash -c "hdfs dfs -put -f fractal-core/build/libs/fractal-core-SPARK-2.2.0.jar /user/ceciliassis/"
bash -c "hdfs dfs -put -f fractal-apps/build/libs/fractal-apps-SPARK-2.2.0.jar /user/ceciliassis/"
