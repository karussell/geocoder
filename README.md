# Geocoder

Currently not useful, but shows that a geocoder based on only OSM data and ElasticSearch can work. No Nominatim etc required.

# Installation

 1. Install Java and maven
 2. run scripts/install.sh
 3. Create GeoJson with OsmJoin from osm2geojson project
 4. Feed the produced files into ElasticSearch via JsonFeeder
 4. Finally offer the search as API via starting HttpServerMain 

# License

Apache License 2.0
