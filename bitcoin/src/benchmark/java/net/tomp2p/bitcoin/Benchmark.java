package net.tomp2p.bitcoin;

import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.message.MessageFilter;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.params.TestNet3Params;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
public class Benchmark {

    private static final Random RND = new Random(42L);
    private static NetworkParameters params = TestNet3Params.get();
//    private static NetworkParameters params = MainNetParams.get();
    final int nrPeers = 100;
    private Number160 nr;
    private PeerDHT master;
    private PeerDHT masterRegistered;
    private PeerDHT[] peers;
    private PeerDHT[] peersRegistered;
    protected List<Registration> registrations;
    private RegistrationBitcoinService registrationService;


    @Setup
    public void setup() throws URISyntaxException, IOException {
        final int port = 3001;

        String mode = params.getId().toString();
        URL resource = Benchmark.class.getResource("/registrations/org.bitcoin.test");
        File dir = new File(resource.toURI());
        registrations = Utils.readRegistrations(dir, "validRegistrations.array");
        RegistrationStorage registrationStorage = new RegistrationStorage() {
            @Override
            public void start() {

            }

            @Override
            public void stop() {

            }

            @Override
            public boolean lookup(Registration registration) {
                return false;
            }

            @Override
            public void store(Registration registration) {

            }
        };
        registrationService = new RegistrationBitcoinService(registrationStorage, params, new java.io.File("."), "tomP2P-bitcoin-testnet").start();
        nr = new Number160(RND);
        peers = createAndAttachPeersDHT(nrPeers, port);
        peersRegistered = createAndAttachPeersDHTRegistered(nrPeers, port+1);
        bootstrap(peers);
        bootstrap(peersRegistered);
        master = peers[0];
        masterRegistered = peersRegistered[0];
    }


    public PeerDHT[] createAndAttachPeersDHT( int nr, int port ) throws IOException {
        PeerDHT[] peers = new PeerDHT[nr];
        for ( int i = 0; i < nr; i++ ) {
            Registration registration = registrations.get(i);
            if ( i == 0 ) {
                peers[0] = new PeerBuilderDHT(new PeerBuilder( registration.getKeyPair() ).ports( port ).start()).start();
            } else {
                peers[i] = new PeerBuilderDHT(new PeerBuilder( registration.getKeyPair() ).masterPeer( peers[0].peer() ).start()).start();
            }
        }
        return peers;
    }

    public PeerDHT[] createAndAttachPeersDHTRegistered( int nr, int port ) throws IOException {
        PeerDHT[] peers = new PeerDHT[nr];
        MessageFilter messageFilter = new MessageFilterRegistered(registrationService);
        for ( int i = 0; i < nr; i++ ) {
            Registration registration = registrations.get(i);
            if ( i == 0 ) {
                peers[0] = new PeerBuilderDHT(new PeerBuilder( registration ).messageFilter(messageFilter).ports( port ).start()).start();
            } else {
                peers[i] = new PeerBuilderDHT(new PeerBuilder( registration ).messageFilter(messageFilter).masterPeer( peers[0].peer() ).start()).start();
            }
        }
        return peers;

    }

    public static void bootstrap( PeerDHT[] peers ) {
        //make perfect bootstrap, the regular can take a while
        for(int i=0;i<peers.length;i++) {
            for(int j=0;j<peers.length;j++) {
                peers[i].peerBean().peerMap().peerFound(peers[j].peerAddress(), null, null, null);
            }
        }
    }

    @TearDown
    public void tearDown() {
        if (master != null) {
            master.shutdown();
        }
        if (masterRegistered != null) {
            masterRegistered.shutdown();
        }
        registrationService.stop();
    }

    /*
     * Mode.AverageTime measures the average execution time, and it does it
     * in the way similar to Mode.Throughput.
     *
     * Some might say it is the reciprocal throughput, and it really is.
     * There are workloads where measuring times is more convenient though.
     */
    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testPutNormal() throws InterruptedException, IOException, ClassNotFoundException {
        int number = RND.nextInt(nrPeers);
        FuturePut futurePut = peers[number].put(nr).data(new Data("hallo")).start();
        futurePut.await();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testPutRegistered() throws InterruptedException, IOException, ClassNotFoundException {
        int number = RND.nextInt(nrPeers);
        FuturePut futurePut = peersRegistered[number].put(nr).data(new Data("hallo")).start();
        futurePut.await();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testVerify() throws ExecutionException, InterruptedException {
        int number = RND.nextInt(nrPeers);
        RegistrationBitcoin registration = (RegistrationBitcoin) registrations.get(number);
        registrationService.verify(registration);
//        System.out.println(registration.getBlockId());
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void testBlockDownload() throws ExecutionException, InterruptedException {
        int number = RND.nextInt(nrPeers);
        RegistrationBitcoin registration = (RegistrationBitcoin) registrations.get(number);
        Sha256Hash blockHash = registration.getBlockId();
        registrationService.getBlockAndVerify(blockHash);
//        System.out.println(blockHash);
    }

}
