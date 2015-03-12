# TomP2P [![Build Status](https://travis-ci.org/tomp2p/TomP2P.svg?branch=master)](https://travis-ci.org/tomp2p/TomP2P)

[TomP2P](http://tomp2p.net) is a P2P library and a *distributed hash table* (DHT) implementation
which provides a decentralized key-value infrastructure for distributed 
applications. Each peer has a table that can be configured either to be 
disk-based or memory-based to store its values.

TomP2P stores key-value pairs in a distributed manner. To find the 
peers to store the data in the distributed hash table, TomP2P uses an 
iterative routing to find the closest peers. Since TomP2P uses non-blocking 
communication, a future object is required to keep track of future results. 
This key concept is used for all the communication (iterative routing and 
DHT operations, such as storing a value on multiple peers) in TomP2P and 
it is also exposed in the API. Thus, an operation such as *get* or 
*put* will return immediately and the user can either block the 
operation to wait for the completion or add a listener that gets notified 
when the operation completes.

## Features

* Java6 DHT implementation with non-blocking IO using Netty.
* XOR-based iterative routing similar to Kademlia.
* Standard DHT operations: put(), get()
* Extended DHT operations and support for custom operations: putIfAbsent(), add(), send(), digest() 
* Selective get() using min-max or Bloom filters
* Direct and indirect replication.
* Mesh-based distributed tracker.
* Data protection based on signatures.
* Port forwarding detection and configuration via UPNP and NAT-PMP.
* Runs with IPv6 and IPv4.
* Network operations support the listenable future objects concept.

## Code Examples (API v4.1)

```java
//create a peer
Peer peer = new PeerMaker(new Number160(rnd)).setPorts(port).buildAndListen();

//store object
FutureDHT f =peer.put(Number160.createHash(“key”)).setObject(“hello world”).build();

//get object
FutureDHT f = peer.get(Number160.createHash(“key”)).build();

//to get the result, either add listener
f.addListener(...)
//or block
f.await()

//send direct messages to a particular peer
peer.sendDirect().setPeerAddress(peer1).setObject(“test”).build();
```
