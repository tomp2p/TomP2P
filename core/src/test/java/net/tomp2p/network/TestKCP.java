package net.tomp2p.network;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestKCP {

    @Test
    public void testSendReceiveSmall() throws InterruptedException {

        final Queue<byte[]> q = new ArrayBlockingQueue<>(2);

        KCP sender = create(q, "snd");
        KCP recv = create(q, "rcv");

        sender.send(ByteBuffer.wrap(new byte[1000]));
        sender.update(100);
        //now we have one packet we should send
        Assert.assertEquals(1, q.size());
        sender.update(200);
        //after 100ms, we now have 2 packets in the queue
        Assert.assertEquals(2, q.size());

        recv.input(q.poll());
        recv.update(100);

        sender.input(q.poll());
        sender.update(200);

        sender.update(300);
        //now we have the first packet twice in the queue
        Assert.assertEquals(2, q.size());
    }

    @Test
    public void testSendReceiveLarge() throws InterruptedException {

        final Queue<byte[]> q = new ArrayBlockingQueue<>(1000);

        KCP sender = create(q, "snd");
        //sender.noDelay(0, 20, 1, 0);
        KCP recv = create(q, "rcv");

        sender.send(ByteBuffer.wrap(new byte[20000]));
        sender.update(100);
        //now we have one packet we should have sent out, needs ack to send more
        Assert.assertEquals(1, q.size());

        recv.input(q.poll());
        recv.update(100);
        sender.input(q.poll());

        sender.update(200);
        //now we have two packet we should have sent out, needs ack to send more
        Assert.assertEquals(2, q.size());

        recv.input(q.poll());
        recv.input(q.poll());
        recv.update(200);
        sender.input(q.poll());

        sender.update(300);
        //now we have two packet we should have sent out, needs ack to send more
        Assert.assertEquals(4, q.size());

        recv.input(q.poll());
        recv.input(q.poll());
        recv.input(q.poll());
        recv.input(q.poll());
        recv.update(300);
        sender.input(q.poll());

        sender.update(400);
        //now we have two packet we should have sent out, needs ack to send more
        Assert.assertEquals(8, q.size());
    }

    @Test
    public void testLossyChannel() throws InterruptedException {

        final BlockingQueue<byte[]> q1 = new ArrayBlockingQueue<>(1000);
        final BlockingQueue<byte[]> q2 = new ArrayBlockingQueue<>(1000);

        final KCP snd = create(q1, "snd");
        final KCP rcv = create(q2, "rcv");

        final Random rnd1 = new Random(42);
        final Random rnd2 = new Random(42);

        new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 200;
                while(true) {

                        byte[] tmp = q2.poll();
                        if(tmp == null) {
                            snd.update(counter);
                            counter += 100;
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            continue;
                        }
                        if(rnd1.nextInt() % 10 == 0) {
                            //drop it!
                            System.out.println("drop it");
                        } else {
                            snd.input(tmp);
                        }

                }
            }

        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 200;
                while(true) {
                        byte[] tmp = q1.poll();
                        if(tmp == null) {
                            rcv.update(counter);
                            counter += 100;
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            continue;
                        }
                        if(rnd2.nextInt() % 10 == 0) {
                            //drop it!
                            System.out.println("drop it");
                        } else {
                            rcv.input(tmp);
                        }

                }
            }
        }).start();

        int nr = 44032; //this is the limit: 32 * (1400-24)
        byte[] sn = new byte[nr];
        sn[nr-1] = 1;
        snd.send(ByteBuffer.wrap(sn));
        snd.update(100);

        Thread.sleep(1000);

        byte[] rc = new byte[nr];
        int nr2 = rcv.recv(rc);
        Assert.assertEquals(44032, nr2);
        //exceeded rcv size
        Assert.assertArrayEquals(sn, rc);

    }

    private static KCP create(final Queue<byte[]> q, String tag) {
        KCP kcp = new KCP(10, new KCPListener() {
            @Override
            public void output(byte[] buffer, int offset, int length) {
                System.out.println(tag +"|len: " + length);
                byte[] b = new byte[length];
                System.arraycopy(buffer, offset, b, 0, length);
                q.add(b);
            }
        });
        kcp.update(0);
        return kcp;
    }
}
