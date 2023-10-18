#!/bin/bash

scp ~/sources/dist/* svc-dedcet@chew1:/bigdata/common/pramen/jars/
scp ~/sources/dist/* svc-dedcet@chew2:/bigdata/common/pramen/jars/

exit 0

sbt -DSPARK_VERSION="2.4.8" ++2.11.12 assembly

VERSION=$(grep "ThisBuild / version" version.sbt | cut -d\" -f2)
scp runner/target/scala-2.11/pramen-runner_2.11_2.4.8-${VERSION}.jar svc-dedcet@chew1:/bigdata/common/pramen/jars/
scp extras/target/scala-2.11/pramen-extras_2.11_2.4.8-${VERSION}.jar svc-dedcet@chew1:/bigdata/common/pramen/jars/

scp runner/target/scala-2.11/pramen-runner_2.11_2.4.8-${VERSION}.jar svc-dedcet@chew2:/bigdata/common/pramen/jars/
scp extras/target/scala-2.11/pramen-extras_2.11_2.4.8-${VERSION}.jar svc-dedcet@chew2:/bigdata/common/pramen/jars/

