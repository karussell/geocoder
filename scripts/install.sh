MVN=mvn

# efficientstring 1.10
DIR=efficientstring
if [ ! -d "$DIR" ]; then
  git clone https://github.com/jillesvangurp/$DIR
  cd $DIR
  git checkout 2712624061
  $MVN clean install
  cd ..
fi

# jsonj 1.29
DIR=jsonj
if [ ! -d "$DIR" ]; then
  git clone https://github.com/jillesvangurp/$DIR
  cd $DIR
  git checkout 2a1b775b8a
  $MVN clean install 
  cd ..
fi

# iterables-support 1.6
DIR=iterables-support
if [ ! -d "$DIR" ]; then
  git clone https://github.com/jillesvangurp/$DIR
  cd $DIR
  git checkout c3570e108e2
  sed -i 's/1\.28/1\.29/g' pom.xml
  $MVN clean install 
  cd ..
fi

# osm2geojson 1.0-SNAPSHOT
DIR=osm2geojson
if [ ! -d "$DIR" ]; then
  git clone https://github.com/karussell/$DIR
  cd $DIR
  git checkout 80098c61d50
  $MVN clean install 
  cd ..
fi
