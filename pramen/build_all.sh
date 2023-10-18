sbt -DSPARK_VERSION="3.3.3" ++2.12.18 assembly
sbt -DSPARK_VERSION="3.2.4" ++2.12.18 assembly
sbt -DSPARK_VERSION="3.4.1" ++2.12.18 assembly

sbt -DSPARK_VERSION="2.4.8" ++2.11.12 assembly
sbt -DSPARK_VERSION="3.2.4" ++2.13.11 assembly
sbt -DSPARK_VERSION="3.4.1" ++2.13.11 assembly

