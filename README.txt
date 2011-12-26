TomP2P - http://tomp2p.net
==========================
TomP2P is an extended DHT, which stores multiple values for a key. Each peer
has a table (either disk-based or memory-based) to store its values. A single
value can be queried / updated with a secondary key. The underlying
communication framework uses Java NIO to handle many concurrent connections.

Release Management
==================
To make a release, use the maven plugin:

  mvn release:prepare -Dusername=##username## -Dpassword=##password##
  mvn release:perform
  
if using a netbook or similar, use:

  mvn release:prepare -Dusername=##username## -Dpassword=##password## -Darguments='-Dmaven.test.skip=true'
  mvn release:perform -Darguments='-Dmaven.test.skip=true'
  

Adding 3rd party libraries to repository
========================================

E.g. upload snapshot of Netty using scp 

mvn deploy:deploy-file \
 -DgroupId=org.jboss.netty \
 -DartifactId=netty \
 -Dversion=3.2.7.Final-SNAPSHOT \
 -Dpackaging=jar \
 -Dfile=netty-3.2.7.Final-SNAPSHOT.jar \ 
 -DrepositoryId=ssh-tomp2p \
 -Durl=scp://tomp2p.net/home/##username##/maven \ 
 -Dusername=##username## \
 -Dpassword=##password##