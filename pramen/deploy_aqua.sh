#!/bin/bash

sbt -DSPARK_VERSION="3.3.2" ++2.12.18 assembly

VERSION=$(grep "ThisBuild / version" version.sbt | cut -d\" -f2)

aws s3 cp runner/target/scala-2.12/pramen-runner_2.12_3.3-${VERSION}.jar s3://aqueduct-pipelines-dev-application-data/pramen/jars/
aws s3 cp extras/target/scala-2.12/pramen-extras_2.12_3.3-${VERSION}.jar s3://aqueduct-pipelines-dev-application-data/pramen/jars/
aws s3 cp runner/target/scala-2.12/pramen-runner_2.12_3.3-${VERSION}.jar s3://aqueduct-pipelines-uat-application-data/pramen/jars/
aws s3 cp extras/target/scala-2.12/pramen-extras_2.12_3.3-${VERSION}.jar s3://aqueduct-pipelines-uat-application-data/pramen/jars/


# aqueduct-pipelines-dev-application-data
# npintdeaqueduct-afs1-dev-ruslan-glue-testl
