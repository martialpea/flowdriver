#!/bin/sh
#
# Gradle startup script for POSIX systems
#

APP_NAME="Gradle"
APP_BASE_NAME=$(basename "$0")

PRG="$0"
while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done
APP_HOME=$(cd "$(dirname "$PRG")" && pwd)

CLASSPATH="$APP_HOME/gradle/wrapper/gradle-wrapper.jar"

if [ -n "$JAVA_HOME" ]; then
  JAVACMD="$JAVA_HOME/bin/java"
  if [ ! -x "$JAVACMD" ]; then
    echo "ERROR: JAVA_HOME is invalid: $JAVA_HOME" >&2
    exit 1
  fi
else
  JAVACMD="java"
  command -v java > /dev/null 2>&1 || {
    echo "ERROR: java not found." >&2
    exit 1
  }
fi

exec "$JAVACMD" \
  -Xmx64m \
  -Xms64m \
  $JAVA_OPTS \
  $GRADLE_OPTS \
  "-Dorg.gradle.appname=$APP_BASE_NAME" \
  -classpath "$CLASSPATH" \
  org.gradle.wrapper.GradleWrapperMain \
  "$@"
