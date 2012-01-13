TomP2P - http://tomp2p.net
==========================
TomP2P is an extended DHT, which stores multiple values for a key. Each peer
has a table (either disk-based or memory-based) to store its values. A single
value can be queried / updated with a secondary key. The underlying
communication framework uses Java NIO to handle many concurrent connections.

Feature List of TomP2P
======================

* Java5 DHT implementation with non-blocking IO using Netty.
* XOR-based iterative routing similar to Kademlia.
* Standard DHT operations: put, get
* Extended DHT operations and support for custom operations: putIfAbsent, add, send, digest
* Direct and indirect replication.
* Mesh-based distributed tracker.
* Data protection based on signatures.
* Port forwarding detection and configuration via UPNP and NAT-PMP.
* Runs with IPv6 and IPv4.
* Network operations support the listenable future objects concept.