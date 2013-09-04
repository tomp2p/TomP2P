package net.tomp2p.p2p;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.PeerConnection;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RawDataReply;

/**
 * Testing with
 * -Djava.util.logging.config.file=src/main/config/tomp2plog.properties -server
 * -Xms2048m -Xmx2048m -XX:+UseParallelGC -XX:+AggressiveOpts
 * -XX:+UseFastAccessorMethods
 * 
 * @author draft
 */
public class TestPerformance {
    final private static Random rnd = new Random(42L);

    public static void main(String[] args) throws Exception {
        test1();
        test2();
        test3();
        test4();
    }

    private static void test1() throws Exception {
        
    }

    private static void test2() throws Exception {
        
    }

    private static void test3() throws Exception {
        
    }

    private static void test4() throws Exception {
        
    }
}
