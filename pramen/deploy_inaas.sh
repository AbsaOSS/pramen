#!/bin/bash

sbt -DSPARK_VERSION="3.3.2" ++2.12.18 assembly

VERSION=$(grep "ThisBuild / version" version.sbt | cut -d\" -f2)

aws s3 cp runner/target/scala-2.12/pramen-runner_2.12_3.3.2-${VERSION}.jar s3://bdtools-ingestionaas-dev-data/pramen/jars/
aws s3 cp extras/target/scala-2.12/pramen-extras_2.12_3.3.2-${VERSION}.jar s3://bdtools-ingestionaas-dev-data/pramen/jars/
