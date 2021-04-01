#!/bin/bash
(cd tmp; unzip -uo /home/aljoscha/remoteEnvJars/gradoop-flink-0.5.2.jar)
(cd tmp; unzip -uo /home/aljoscha/remoteEnvJars/gradoop-common-0.5.2.jar)
(cd tmp; unzip -uo /home/aljoscha/remoteEnvJars/flink-gelly_2.11-1.7.2.jar)
(cd tmp; unzip -uo /home/aljoscha/remoteEnvJars/vDriveGraphVisualization.jar)
jar -cvf combined.jar -C tmp .
rm -r /home/aljoscha/remoteEnvJars/tmp/*
