package net.tomp2p.examples;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import net.tomp2p.futures.FutureGet;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageLayer.ProtectionEnable;
import net.tomp2p.storage.StorageLayer.ProtectionMode;
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
        Peer peer1 = new PeerMaker( pair1 ).ports( 4001 ).makeAndListen();
        Peer peer2 = new PeerMaker( pair2 ).ports( 4002 ).makeAndListen();
        Peer peer3 = new PeerMaker( pair3 ).ports( 4003 ).makeAndListen();
        Peer[] peers = new Peer[] { peer1, peer2, peer3 };
        ExampleUtils.bootstrap( peers );
        setProtection( peers, ProtectionEnable.ALL, ProtectionMode.MASTER_PUBLIC_KEY );
        // peer 1 stores "test" in the domain key of owner peer 2
        FuturePut futurePut =
            peer1.put( Number160.ONE ).setData( new Data( "test" ) ).setDomainKey( peer2Owner ).setProtectDomain().start();
        futurePut.awaitUninterruptibly();
        // peer 2 did not claim this domain, so we stored it
        System.out.println( "stored: " + futurePut.isSuccess() + " -> because no one claimed this domain" );
        // peer 3 want to store something
        futurePut =
            peer3.put( Number160.ONE ).setData( new Data( "hello" ) ).setDomainKey( peer2Owner ).setProtectDomain().start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> becaues peer1 already claimed this domain" );
        // peer 2 claims this domain
        futurePut =
            peer2.put( Number160.ONE ).setData( new Data( "MINE!" ) ).setDomainKey( peer2Owner ).setProtectDomain().start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> becaues peer2 is the owner" );
        // get the data!
        FutureGet futureGet = peer1.get( Number160.ONE ).setDomainKey( peer2Owner ).start();
        futureGet.awaitUninterruptibly();
        System.out.println( "we got " + futureGet.getData().object() );
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
        Peer peer1 = new PeerMaker( pair1 ).ports( 4001 ).makeAndListen();
        Peer peer2 = new PeerMaker( pair2 ).ports( 4002 ).makeAndListen();
        Peer peer3 = new PeerMaker( pair3 ).ports( 4003 ).makeAndListen();
        Peer[] peers = new Peer[] { peer1, peer2, peer3 };
        ExampleUtils.bootstrap( peers );
        setProtection( peers, ProtectionEnable.NONE, ProtectionMode.MASTER_PUBLIC_KEY );
        // peer 1 stores "test" in the domain key of owner peer 2
        FuturePut futurePut =
            peer1.put( Number160.ONE ).setData( new Data( "test" ) ).setProtectDomain().setDomainKey( peer2Owner ).start();
        futurePut.awaitUninterruptibly();
        // peer 2 did not claim this domain, so we stored it
        System.out.println( "stored: " + futurePut.isSuccess()
            + " -> because no one can claim domains except the owner, storage ok but no protection" );
        // peer 3 want to store something
        futurePut =
            peer3.put( Number160.ONE ).setData( new Data( "hello" ) ).setProtectDomain().setDomainKey( peer2Owner ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess()
            + " -> because no one can claim domains except the owner, storage ok but no protection" );
        // peer 2 claims this domain
        futurePut =
            peer2.put( Number160.ONE ).setData( new Data( "MINE!" ) ).setProtectDomain().setDomainKey( peer2Owner ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> becaues peer2 is the owner" );
        // get the data!
        FutureGet futureGet = peer1.get( Number160.ONE ).setDomainKey( peer2Owner ).start();
        futureGet.awaitUninterruptibly();
        System.out.println( "we got " + futureGet.getData().object() );
        futurePut = peer3.put( Number160.ONE ).setDomainKey( peer2Owner ).setData( new Data( "hello" ) ).start();
        futurePut.awaitUninterruptibly();
        System.out.println( "stored: " + futurePut.isSuccess() + " -> because this domain is claimed by peer2" );
        shutdown( peers );
    }

    private static void shutdown( Peer[] peers )
    {
        for ( Peer peer : peers )
        {
            peer.shutdown();
        }
    }

    private static void setProtection( Peer[] peers, ProtectionEnable protectionEnable, ProtectionMode protectionMode )
    {
        for ( Peer peer : peers )
        {
            peer.getPeerBean().storage().setProtection( protectionEnable, protectionMode, protectionEnable,
                                                           protectionMode );
        }
    }
}
