TomP2P - http://tomp2p.net
==========================
TomP2P is an extended DHT, which stores multiple values for a key. Each peer
has a table (either disk-based or memory-based) to store its values. A single
value can be queried / updated with a secondary key. The underlying
communication framework uses Java NIO to handle many concurrent connections.

Release Management
==================
To make a release, use the maven plugin:
mvn release:prepare -Dusername=xxx -Dpassword=yyy
mvn release:perform
