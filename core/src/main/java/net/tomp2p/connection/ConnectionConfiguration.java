package net.tomp2p.connection;

import java.security.KeyPair;

public interface ConnectionConfiguration {
	boolean sign();
	boolean sctp();
	KeyPair keyPair();
}