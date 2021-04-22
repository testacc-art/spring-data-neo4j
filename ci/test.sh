#!/bin/bash -x

set -euo pipefail

mkdir -p /tmp/jenkins-home
chown -R 1001:1001 .
MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" \
  ./mvnw -s settings.xml -P${PROFILE} clean dependency:list verify -Dsort -U -B -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-neo4j
