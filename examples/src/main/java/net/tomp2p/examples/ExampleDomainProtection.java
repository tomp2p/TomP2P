package net.tomp2p.examples;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.StorageLayer.ProtectionEnable;
import net.tomp2p.dht.StorageLayer.ProtectionMode;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class ExampleDomainProtection
{
    final Random rnd = new Random( 42L );

    public static void main( String[] args )
        throws NoSuchAlgorithmException, IOException, ClassNotFoundException
    {
        exampleAllMaster();
        exampleNoneMaster();
    }

    public static void exampleAllMaster()
        throws NoSuchAlgorithmException, IOException, ClassNotFoundException
    {
        KeyPairGenerator gen = KeyPairGenerator.getInstance( "DSA" );
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        KeyPair pair3 = gen.generateKeyPair();
        final Number160 peer2Owner = Utils.makeSHAHash( pair2.getPublic().getEncoded() );
        PeerDHT peer1 = new PeerDHT(new PeerBuilder( pair1 ).ports( 4001 ).start());
        PeerDHT peer2 = new PeerDHT(new PeerBuilder( pair2 ).ports( 4002 ).start());
        PeerDHT peer3 = new PeerDHT(new PeerBuilder( pair3 ).ports( 4003 ).start());
        PeerDHT[] peers = new PeerDHT[] { peer1, peer2, peer3 };
        ExampleUtils.bootstrap( peers );
        setProtection( peers, ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY );
        // peer 1 stores "test" in the domain key of owner peer 2
        FuturePut futurePut =
            peer1.put( Number160.ONE ).data( new Data( "test" ) ).domainKey( peer2Owner ).protectDomain().start();
        futurePut.awaitUninterruptibly();
        // peer 2 did not claim this domain, so we stored it
        System.out.println( "stored: " + futurePut.isSuccess() + " -> because no one claimed this domain" );
        // peer 3 want to store something
        futurePut =
            peer3.put( Number160.ONE ).data( new Data( "hello" ) ).domainKey( peer2Owner ).protectDomain().start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> becaues peer1 already claimed this domain" );
        // peer 2 claims this domain
        futurePut =
            peer2.put( Number160.ONE ).data( new Data( "MINE!" ) ).domainKey( peer2Owner ).protectDomain().start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> becaues peer2 is the owner" );
        // get the data!
        FutureGet futureGet = peer1.get( Number160.ONE ).domainKey( peer2Owner ).start();
        futureGet.awaitUninterruptibly();
        System.out.println( "we got " + futureGet.data().object() );
        shutdown( peers );
    }

    public static void exampleNoneMaster()
        throws NoSuchAlgorithmException, IOException, ClassNotFoundException
    {
        KeyPairGenerator gen = KeyPairGenerator.getInstance( "DSA" );
        KeyPair pair1 = gen.generateKeyPair();
        KeyPair pair2 = gen.generateKeyPair();
        KeyPair pair3 = gen.generateKeyPair();
        final Number160 peer2Owner = Utils.makeSHAHash( pair2.getPublic().getEncoded() );
        PeerDHT peer1 = new PeerDHT(new PeerBuilder( pair1 ).ports( 4001 ).start());
        PeerDHT peer2 = new PeerDHT(new PeerBuilder( pair2 ).ports( 4002 ).start());
        PeerDHT peer3 = new PeerDHT(new PeerBuilder( pair3 ).ports( 4003 ).start());
        PeerDHT[] peers = new PeerDHT[] { peer1, peer2, peer3 };
        ExampleUtils.bootstrap( peers );
        setProtection( peers, ProtectionEnable.NONE, ProtectionMode.MASTER_PUBLIC_KEY );
        // peer 1 stores "test" in the domain key of owner peer 2
        FuturePut futurePut =
            peer1.put( Number160.ONE ).data( new Data( "test" ) ).protectDomain().domainKey( peer2Owner ).start();
        futurePut.awaitUninterruptibly();
        // peer 2 did not claim this domain, so we stored it
        System.out.println( "stored: " + futurePut.isSuccess()
            + " -> because no one can claim domains except the owner, storage ok but no protection" );
        // peer 3 want to store something
        futurePut =
            peer3.put( Number160.ONE ).data( new Data( "hello" ) ).protectDomain().domainKey( peer2Owner ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess()
            + " -> because no one can claim domains except the owner, storage ok but no protection" );
        // peer 2 claims this domain
        futurePut =
            peer2.put( Number160.ONE ).data( new Data( "MINE!" ) ).protectDomain().domainKey( peer2Owner ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> becaues peer2 is the owner" );
        // get the data!
        FutureGet futureGet = peer1.get( Number160.ONE ).domainKey( peer2Owner ).start();
        futureGet.awaitUninterruptibly();
        System.out.println( "we got " + futureGet.data().object() );
        futurePut = peer3.put( Number160.ONE ).domainKey( peer2Owner ).data( new Data( "hello" ) ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> because this domain is claimed by peer2" );
        shutdown( peers );
    }

    private static void shutdown( PeerDHT[] peers )
    {
        for ( PeerDHT peer : peers )
        {
            peer.shutdown();
        }
    }

    private static void setProtection( PeerDHT[] peers, ProtectionEnable protectionEnable, ProtectionMode protectionMode )
    {
        for ( PeerDHT peer : peers )
        {
            peer.storageLayer().protection( protectionEnable, protectionMode, protectionEnable,
                                                           protectionMode );
        }
    }
}
