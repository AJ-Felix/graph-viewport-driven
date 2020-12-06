#!/bin/bash
(cd tmp; unzip -uo /home/aljoscha/remoteEnvJars/multiJar.jar)
(cd tmp; unzip -uo /home/aljoscha/remoteEnvJars/viewportDrivenGraphStreaming.jar)
jar -cvf combined.jar -C tmp .
rm -r /home/aljoscha/remoteEnvJars/tmp/*
