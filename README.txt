TomP2P
======

TomP2P is a distributed hash sorted table (DHST). A DHST is an extended DHT, 
which stores values for a location key in a sorted table. Each peer has such 
a sorted table, and its values are accessed with a content key. A DHST can 
store multiple values for a location key.

Release
=======
To make a release, use the maven plugin:
mvn release:prepare -Dusername=xxx -Dpassword=yyy
mvn release:perform