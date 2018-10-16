package net.tomp2p.rpc;

import net.tomp2p.connection.ChannelTransceiver;
import net.tomp2p.connection.DataSend;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.network.KCP;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class TestDirect {
    @Test
    public void testDirectSmallData() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelTransceiver.resetCounters();
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).enableMaintenance(false).port(2424).start();

            byte[] testArray = new byte[222];
            for(int i=0;i<testArray.length;i++) {
                testArray[i] = (byte) i;
            }
            System.out.println("Send ARR: "+ Arrays.toString(testArray));

            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).enableMaintenance(false).port(8088).start();

            final CountDownLatch done = new CountDownLatch(1);

            recv1.dataReply(100, new DataStream() {
                @Override
                public ByteBuffer receiveSend(ByteBuffer buffer, DataSend dataSend) {
                    System.out.println("gotR: "+buffer.remaining());
                    return buffer;
                }
            });

            sender.sendDirect(100, recv1.peerAddress(), new DataStream() {
                @Override
                public ByteBuffer receiveSend(ByteBuffer buffer, DataSend dataSend) {
                    System.out.println("gotS: "+buffer.remaining());
                    if(buffer.get(1) == testArray[1] && buffer.get(testArray.length-1) == testArray[testArray.length - 1]) {
                        done.countDown();
                    }
                    return ByteBuffer.wrap(new byte[0]);

                }
            }).send(ByteBuffer.wrap(testArray));


            done.await();


        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }

    @Test
    public void testDirectLargeData() throws Exception {
        Peer sender = null;
        Peer recv1 = null;
        ChannelTransceiver.resetCounters();
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).enableMaintenance(false).port(2424).start();

            byte[] testArray = new byte[9000]; //TODO: figure out why 6000 is not working
            for(int i=0;i<testArray.length;i++) {
                testArray[i] = (byte) i;
            }
            System.out.println("Send ARR: "+ Arrays.toString(testArray));

            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).enableMaintenance(false).port(8088).start();

            final CountDownLatch done = new CountDownLatch(1);

            recv1.dataReply(100, new DataStream() {
                @Override
                public ByteBuffer receiveSend(ByteBuffer buffer, DataSend dataSend) {
                    System.out.println("gotR: "+buffer.remaining());
                    return buffer;
                }
            });

            sender.sendDirect(100, recv1.peerAddress(), new DataStream() {
                @Override
                public ByteBuffer receiveSend(ByteBuffer buffer, DataSend dataSend) {
                    System.out.println("gotS: "+buffer.remaining());
                    if(buffer.get(1) == testArray[1] && buffer.get(testArray.length-1) == testArray[testArray.length - 1]) {
                        done.countDown();
                    }
                    return ByteBuffer.wrap(new byte[0]);

                }
            }).send(ByteBuffer.wrap(testArray));


            done.await();


        } finally {
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
        }
    }
}
