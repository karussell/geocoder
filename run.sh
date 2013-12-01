JAVA=$JAVA_HOME/bin/java
if [ "x$JAVA_HOME" = "x" ]; then
 JAVA=java
fi

if [ "x$MAVEN_HOME" = "x" ]; then
 MAVEN_HOME=`mvn -v | grep "Maven home" | cut -d' ' -f3`
fi

ACTION=$1
JAVA_OPTS="-XX:PermSize=60m -XX:MaxPermSize=60m -Xmx200m -Xms200m -server"
VERSION=`grep  "<name>" -B 1 pom.xml | grep version | cut -d'>' -f2 | cut -d'<' -f1`
JAR=target/geocoder-$VERSION-jar-with-dependencies.jar

if [ ! -f "$JAR" ]; then
  echo "## now building jar: $JAR"
  echo "## using maven at $MAVEN_HOME"
  #mvn clean
  "$MAVEN_HOME/bin/mvn" -DskipTests=true install assembly:single > /tmp/geocoder-compile.log
  returncode=$?
  if [[ $returncode != 0 ]] ; then
      echo "## compilation of core failed"
      cat /tmp/geocoder-compile.log
      exit $returncode
  fi      
else
  echo "## existing jar found $JAR"
fi

if [ "x$ACTION" = "xfeed" ]; then
  "$JAVA" $JAVA_OPTS -cp "$JAR" com.graphhopper.geocoder.JsonFeeder
elif [ "x$ACTION" = "xserver" ]; then
  "$JAVA" $JAVA_OPTS -cp "$JAR" com.graphhopper.geocoder.http.HttpServerMain
elif [ "x$ACTION" = "xfixer" ]; then
  "$JAVA" $JAVA_OPTS -cp "$JAR" com.graphhopper.geocoder.RelationShipFixer
else    
  echo unknown action $ACTION
fi
