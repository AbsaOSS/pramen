#!/bin/bash

#sbt -DSPARK_VERSION="2.4.8" ++2.11.12 assembly

scp ~/sources/dist/* svc-dedce@binks1:/bigdata/common/pramen/jars/
scp ~/sources/dist/* svc-dedce@binks2:/bigdata/common/pramen/jars/

exit 0

VERSION=$(grep "ThisBuild / version" version.sbt | cut -d\" -f2)
scp runner/target/scala-2.11/pramen-runner_2.11_2.4.8-${VERSION}.jar svc-dedce@binks1:/bigdata/common/pramen/jars/
scp extras/target/scala-2.11/pramen-extras_2.11_2.4.8-${VERSION}.jar svc-dedce@binks1:/bigdata/common/pramen/jars/

scp runner/target/scala-2.11/pramen-runner_2.11_2.4.8-${VERSION}.jar svc-dedce@binks2:/bigdata/common/pramen/jars/
scp extras/target/scala-2.11/pramen-extras_2.11_2.4.8-${VERSION}.jar svc-dedce@binks2:/bigdata/common/pramen/jars/

