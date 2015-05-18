package net.tomp2p.bitcoin;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.bitcoinj.core.Coin;
import org.bitcoinj.core.Wallet;
import org.bitcoinj.params.RegTestParams;
import org.bitcoinj.wallet.KeyChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * This TestClass requires bitcoind v0.10.0 to be installed and available in the path.
 */
public class RegistrationServiceRegTestTest {

    private final static Logger LOG = LoggerFactory.getLogger(RegistrationServiceRegTestTest.class);
    private final static Logger LOGBTC = LoggerFactory.getLogger("bitcoin-cli");
    private static Process bitcoind;
    private static Path datadirPath;

    private static RegistrationService rs;
    private static KeyPair keyPair;

    @org.junit.BeforeClass
    static public void setUp() throws Exception {
        //TODO: check if bitcoind and bitcoin-cli are available. Skip tests if not
        URL resource = RegistrationServiceRegTestTest.class.getResource("/bitcoin-regtest");
        datadirPath = Paths.get(resource.getPath());
        ProcessBuilder pb = new ProcessBuilder("bitcoind", "-regtest", "-debug", "-datadir="+datadirPath.toString());
        bitcoind = pb.start();
        LOG.info("bitcoind started");
        bitcoind.waitFor(60, TimeUnit.SECONDS); // wait for one minute to make sure bitcoind is ready for commands
        LOG.info("bitcoind is running");
        File dir = new File(resource.toURI());
        rs = new RegistrationService(RegTestParams.get(), dir, "tomp2p-bitcoin");
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        keyPair = gen.generateKeyPair();
    }

    @org.junit.Test
    public void testStart() throws Exception {
        Coin value = Coin.COIN;
        generateBlocks(101);
        getBitcoinNodeInfo();


        rs.start();
        assertTrue(rs.kit.peerGroup().getConnectedPeers().size() >= 1);
        LOG.info("Wallet balance: " + rs.kit.wallet().getBalance());
        assertTrue(rs.kit.wallet().getBalance().equals(Coin.ZERO));
        String address = rs.kit.wallet().currentAddress(KeyChain.KeyPurpose.RECEIVE_FUNDS).toString();
        LOG.info(address);
        sendBitcoinsToAddress(1, address);

        // Bitcoinj allows you to define a BalanceFuture to execute a callback once your wallet has a certain balance.
        // Here we wait until the we have enough balance and display a notice.
        ListenableFuture<Coin> balanceFuture = rs.kit.wallet().getBalanceFuture(value, Wallet.BalanceType.AVAILABLE);
        FutureCallback<Coin> callback = new FutureCallback<Coin>() {
            public void onSuccess(Coin balance) {
                LOG.info("coins arrived and the wallet now has enough balance");
                LOG.info("Wallet balance: " + rs.kit.wallet().getBalance());
            }

            public void onFailure(Throwable t) {
                LOG.info("something went wrong");
            }
        };
        Futures.addCallback(balanceFuture, callback);

        generateBlocks(1);
        balanceFuture.get();
        assertTrue(rs.kit.wallet().getBalance().equals(Coin.valueOf(100000000)));
    }

    private static void generateBlocks(int amount) throws Exception {
        LOGBTC.info("generate {} blocks", amount);
        ProcessBuilder pb = new ProcessBuilder("bitcoin-cli", "-regtest", "-datadir="+datadirPath.toString(), "setgenerate", "true", Integer.toString(amount));
        Process p = pb.start();
        logProcessOutput(p);
        p.waitFor();
    }

    private void getBitcoinNodeInfo() throws Exception {
        LOGBTC.info("bitcoind info:");
        ProcessBuilder pb = new ProcessBuilder("bitcoin-cli", "-regtest", "-datadir="+datadirPath.toString(), "getinfo");
        Process p = pb.start();
        logProcessOutput(p);
        p.waitFor();
    }

    private void sendBitcoinsToAddress(int amount, String address) throws Exception {
        LOGBTC.info("Send {} bitcoins to {}", amount, address);
        ProcessBuilder pb = new ProcessBuilder("bitcoin-cli", "-regtest", "-datadir="+datadirPath.toString(), "sendtoaddress", address, Integer.toString(amount));
        Process p = pb.start();
        logProcessOutput(p);
        p.waitFor();
    }

    private static void logProcessOutput(Process p) throws IOException {
        InputStreamReader isr = new InputStreamReader(p.getInputStream());
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null)
            LOGBTC.info(line);
    }

    @org.junit.AfterClass
    static public void tearDown() throws Exception {
        bitcoind.destroy();
        LOG.info("bitcoind stopped");
    }

}
