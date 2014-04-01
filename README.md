# Geocoder

Currently not useful, but shows that a geocoder based on only OSM data and ElasticSearch can work. No Nominatim etc required. 
The full import process of world wide data takes less than 20 hours, but there are several bugs and unresolved issues 
like getting the parent name for streets where the parent has no boundaries

# Installation

 1. Install Java and maven
 2. run scripts/install.sh
 3. Create GeoJson with OsmJoin from osm2geojson project
 4. Feed the produced files into ElasticSearch via JsonFeeder: `./run.sh feeder`
 5. 'Optional' step to fix the boundaries: `./run.sh fixer`
 6. Finally offer the search as API via starting HttpServerMain: `./run.sh server`

# License

Apache License 2.0
