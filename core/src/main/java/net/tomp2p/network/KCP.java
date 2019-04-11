package net.tomp2p.network;

//=====================================================================
//
// KCP - A Better ARQ Protocol Implementation
// skywind3000 (at) gmail.com, 2010-2011
//
// Features:
// + Average RTT reduce 30% - 40% vs traditional ARQ like tcp.
// + Maximum RTT reduce three times vs tcp.
// + Lightweight, distributed as a single source file.
//
//=====================================================================

//LICENSE: MIT: https://github.com/skywind3000/kcp/blob/master/LICENSE

/*
* KCP is a fast and reliable protocol that can achieve the transmission effect of a reduction of the average
* latency by 30% to 40% and reduction of the maximum delay by a factor of three, at the cost of 10% to 20%
* more bandwidth wasted than TCP. It is implemented by using the pure algorithm, and is not responsible for
* the sending and receiving of the underlying protocol (such as UDP), requiring the users to define their
* own transmission mode for the underlying data packet, and provide it to KCP in the way of callback. Even
* the clock needs to be passed in from the outside, without any internal system calls.
*
* You may have implement a P2P, or a UDP-based protocol, but are lack of a set of perfect ARQ reliable protocol
* implementation, then by simply copying the file to the existing project, and writing a couple of lines of code,
* you can use it.
*/

import net.tomp2p.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;


/*

KCP has only one kind of segment: both the data and control messages are
encoded into the same structure and share the same header.

The KCP packet (aka. segment) structure is as following:

0               4   5   6       8 (BYTE)
+---------------+---+---+-------+
|     conv      |cmd|frg|  wnd  |
+---------------+---+---+-------+   8
|     ts        |     sn        |
+---------------+---------------+  16
|     una       |     len       |
+---------------+---------------+  24
|                               |
|        DATA (optional)        |
|                               |
+-------------------------------+

*/

public class KCP {

    public interface KCPListener {
        void output(byte[] buffer, int offset, int length);
    }

    private static final Logger LOG = LoggerFactory.getLogger(KCP.class);

    //=====================================================================
    // KCP BASIC
    //=====================================================================
    private static final int IKCP_RTO_NDL = 30;   // no delay min rto
    private static final int IKCP_RTO_MIN = 100;  // normal min rto
    private static final int IKCP_RTO_DEF = 200;
    private static final int IKCP_RTO_MAX = 60000;
    private static final int IKCP_CMD_PUSH = 81;  // cmd: push data
    private static final int IKCP_CMD_ACK = 82;   // cmd: ack
    private static final int IKCP_CMD_WASK = 83;  // cmd: window probe (ask)
    private static final int IKCP_CMD_WINS = 84;  // cmd: window size (tell)
    private static final int IKCP_ASK_SEND = 1;   // need to send IKCP_CMD_WASK
    private static final int IKCP_ASK_TELL = 2;   // need to send IKCP_CMD_WINS
    private static final int IKCP_WND_SND = 32;
    private static final int IKCP_WND_RCV = 32;
    private static final int IKCP_MTU_DEF = 1400;
    private static final int IKCP_INTERVAL = 100;
    private static final int IKCP_OVERHEAD = 24;
    private static final int IKCP_DEADLINK = 10;
    private static final int IKCP_THRESH_INIT = IKCP_WND_SND / 2;
    private static final int IKCP_THRESH_MIN = 2;
    private static final int IKCP_PROBE_INIT = 7000;    // 7 secs to probe window size
    private static final int IKCP_PROBE_LIMIT = 120000; // up to 120 secs to probe window

    private int conversationId = 0;
    private long snd_una = 0;
    private long snd_nxt = 0;
    private long rcv_nxt = 0;
    private long ts_probe = 0;
    private long probe_wait = 0;
    private int snd_wnd = IKCP_WND_SND;
    private int rcv_wnd = IKCP_WND_RCV;
    private int rmt_wnd = IKCP_WND_RCV;
    private long cwnd = 0; //congestion window size
    private long probe = 0;
    private int mtu = IKCP_MTU_DEF;
    private int mss = this.mtu - IKCP_OVERHEAD;
    private byte[] buffer = new byte[(int) (mtu + IKCP_OVERHEAD) * 3];

    //private List<Segment> nrcv_buf = new ArrayList<>();
    //private List<Segment> nsnd_buf = new ArrayList<>();
    //private List<Segment> nrcv_que = new ArrayList<>();
    //private List<Segment> nsnd_que = new ArrayList<>();

    private SortedMap<Long, Segment> nrcv_buf = new ConcurrentSkipListMap<>();
    private SortedMap<Long, Segment> nsnd_buf = new ConcurrentSkipListMap<>();
    private Queue<Segment> nrcv_que = new ConcurrentLinkedQueue<>();
    private Queue<Segment> nsnd_que = new ConcurrentLinkedQueue<>();

    private long state = 0;
    private ArrayList<Long> acklist = new ArrayList<>(128);
    private long rx_srtt = 0;
    private long rx_rttval = 0;
    private int rx_rto = IKCP_RTO_DEF;
    private int rx_minrto = IKCP_RTO_MIN;
    private long currentTs = 0;

    private int interval = IKCP_INTERVAL;

    private long ts_flush = IKCP_INTERVAL;

    private boolean nodelayEnabled = false;
    //private long updated = 0;
    private long ssthresh = IKCP_THRESH_INIT; // slow-start threshold
    private int fastresend = 0;
    private long xmit = 0;
    private boolean congestionControlEnabled = true;
    private long dead_link = IKCP_DEADLINK;

    private final KCPListener kcpListener;

    private class Segment {
        protected int conversationId = 0;

        //TODO: everything was long, check if necessary
        protected byte cmd = 0; //command
        protected int frg = 0; //fragment count
        protected int wnd = 0; //window size

        protected long ts = 0; //timestamp
        protected long sn = 0; //serial number
        protected long una = 0; //un-acknowledged serial number
        protected long resendts = 0;
        // Regardless of whether TCP or KCP calculates RTO, there is a minimum RTO limit. Even if the RTO is
        // calculated to be 40ms, since the default RTO is 100ms, the protocol can detect packet loss only
        // after 100ms. In fast mode, it is 30ms. You can change this manually.
        protected long rto = 0;
        protected long fastack = 0;
        protected long xmit = 0;

        protected byte[] data;

        protected Segment(int size) {
            this.data = new byte[size];
        }

        //---------------------------------------------------------------------
        // ikcp_encode_seg
        //---------------------------------------------------------------------
        // encode a segment into buffer
        protected int encode(byte[] array, int offset) {
            int currentOffset = offset;

            offset = Utils.intToByteArray(conversationId, array, offset);  //4
            offset = Utils.byteToByteArray(cmd, array, offset);  //1 / 5
            offset = Utils.byteToByteArray(frg, array, offset);  //1 / 6
            offset = Utils.shortToByteArray(wnd, array, offset); //2 / 8

            offset = Utils.intToByteArray(ts, array, offset);    //4 / 12
            offset = Utils.intToByteArray(sn, array, offset);    //4 / 16
            offset = Utils.intToByteArray(una, array, offset);   //4 / 20
            offset = Utils.intToByteArray(data.length, array, offset); //4 / 24

            return offset - currentOffset;
        }
    }

    public final static ByteBuffer EMPTY = ByteBuffer.allocate(0);


    /**
     * @param conversationId The conversation id is used to identify each connection, which will not change
     * during the connection life-time.
     *
     * It is represented by a 32 bits integer which is given at the moment the KCP
     * control block (aka. struct ikcpcb, or kcp object) has been created. Each
     * packet sent out will carry the conversation id in the first 4 bytes and a
     * packet from remote endpoint will not be accepted if it has a different
     * conversation id.
     *
     * The value can be any random number, but in practice, both side between a
     * connection will have many KCP objects (or control block) storing in the
     * containers like a map or an array. A index is used as the key to look up one
     * KCP object from the container.
     *
     * So, the higher 16 bits of conversation id can be used as caller's index while
     * the lower 16 bits can be used as callee's index. KCP will not handle
     * handshake, and the index in both side can be decided and exchanged after
     * connection establish.
     *
     * When you receive and accept a remote packet, the local index can be extracted
     * from the conversation id and the kcp object which is in charge of this
     * connection can be find out from your map or array.
     */
    public KCP(int conversationId, KCPListener kcpListener) {
        this.conversationId = conversationId;
        this.kcpListener = kcpListener;
    }

    //---------------------------------------------------------------------
    // user/upper level recv: returns size, returns below zero for EAGAIN
    //---------------------------------------------------------------------
    // Pass the data in the receive queue to the upper reference
    public Collection<ByteBuffer> recv() {

        if (nrcv_que.size() == 0) {
            LOG.debug("we have don't have any data to deliver");
            return Collections.EMPTY_SET;
        }

        boolean recover = false;
        if (nrcv_que.size() >= rcv_wnd) {
            LOG.debug("our queue [{}] is larger than the receiving window [{}]. Indicate recover", nrcv_que.size(), rcv_wnd);
            recover = true;
        }

        final List<ByteBuffer> received = new ArrayList<>(rcv_wnd);
        final Iterator<Segment> segmentIteratorDone = nrcv_que.iterator();
        while(segmentIteratorDone.hasNext()) {
            Segment seg = segmentIteratorDone.next();
            LOG.debug("we have data to deliver, len:{}, convId: {}, seq: {}", seg.data.length, seg.conversationId, seg.sn);
            received.add(ByteBuffer.wrap(seg.data));
            segmentIteratorDone.remove();
            if (seg.frg == 0) {
                break;
            }
        }

        // fast recover
        if (nrcv_que.size() < rcv_wnd && recover) {
            LOG.debug("our queue [{}] was larger than the receiving window [{}]. But now its good again", nrcv_que.size(), rcv_wnd);
            // ready to send back IKCP_CMD_WINS in ikcp_flush
            // tell remote my window size
            probe |= IKCP_ASK_TELL;
        }

        return received;
    }

    //---------------------------------------------------------------------
    // user/upper level send, returns below zero for error
    //---------------------------------------------------------------------
    // The data to be sent by the upper layer is sent to the sending queue, and the sending queue is fragmented according to the mtu size.
    public int send(ByteBuffer buffer) {

        int length = buffer.remaining();
        if (length == 0) {
            return -1;
        }

        //number of fragments
        int count = (length + mss - 1) / mss;

        if (count > 255) {
            return -2;
        }

        // Add to the send queue after fragmentation
        for (int i = 0; i < count; i++) {
            int size = (int) (length > mss ? mss : length);
            Segment seg = new Segment(size);
            buffer.get(seg.data, 0, size);
            length = buffer.remaining();
            seg.frg = count - i - 1;
            nsnd_que.add(seg);
        }
        return 0;
    }

    //---------------------------------------------------------------------
    // parse ack
    //---------------------------------------------------------------------
    private void update_ack(int rtt) {
        if (0 == rx_srtt) {
            rx_srtt = rtt;
            rx_rttval = rtt / 2;
        } else {
            int delta = (int) (rtt - rx_srtt);
            if (0 > delta) {
                delta = -delta;
            }

            rx_rttval = (3 * rx_rttval + delta) / 4;
            rx_srtt = (7 * rx_srtt + rtt) / 8;
            if (rx_srtt < 1) {
                rx_srtt = 1;
            }
        }

        int rto = (int) (rx_srtt + Math.max(1, 4 * rx_rttval));
        rx_rto = Math.min(Math.max(rx_minrto,rto), IKCP_RTO_MAX);
    }

    // Calculate local real snd_una
    private void shrink_buf() {
        if (nsnd_buf.size() > 0) {
            snd_una = nsnd_buf.firstKey();
        } else {
            snd_una = snd_nxt;
        }
    }

    // The ack returned by the peer confirms that the corresponding packet is removed from the send buffer when the transmission is successful.
    private void parse_ack(long sn) {
        if (sn - snd_una < 0 || sn - snd_nxt >= 0) {
            return;
        }

        for (Segment seg : nsnd_buf.values()) {
            if (sn < seg.sn) {
                break;
            }

            // The original ikcp_parse_fastack&ikcp_parse_ack logic is repeated
            seg.fastack++;
        }
        nsnd_buf.remove(sn);
    }

    private void parse_una(final long una) {
        final Iterator<Segment> segmentIterator = nsnd_buf.values().iterator();
        while(segmentIterator.hasNext()) {
            final Segment seg = segmentIterator.next();
            if (una - seg.sn > 0) {
                segmentIterator.remove();
            } else {
                break;
            }
        }
    }

    private void parse_fastack(int sn) {
        if (sn -snd_una < 0 || sn - snd_nxt >= 0) {
            return;
        }
        for (Segment seg : this.nsnd_buf.values()) {
            if (sn - seg.sn < 0) {
                break;
            } else if (sn != seg.sn) {
                seg.fastack++;
            }
        }
    }


    //---------------------------------------------------------------------
    // ack append
    //---------------------------------------------------------------------
    // After receiving the data packet, you need to send the peer back ack, and send it out when flushing.
    private void ack_push(long sn, long ts) {
        // c original version to expand capacity by *2
        acklist.add(sn);
        acklist.add(ts);
    }

    //---------------------------------------------------------------------
    // parse data
    //---------------------------------------------------------------------
    // User packet parsing
    private void parse_data(Segment newseg) {
        long sn = newseg.sn;

        if (sn - (rcv_nxt + rcv_wnd) >= 0 || sn - rcv_nxt < 0) {
            return;
        }

        if(!nrcv_buf.containsKey(sn)) {
            nrcv_buf.put(sn, newseg);
        }

        // move available data from nrcv_buf -> nrcv_que
        // Add continuous packets to the receive queue
        final Iterator<Map.Entry<Long, Segment>> mapIterator = nrcv_buf.entrySet().iterator();
        while(mapIterator.hasNext()) {
            final Segment seg = mapIterator.next().getValue();
            if (seg.sn == rcv_nxt && nrcv_que.size() < rcv_wnd) {
                nrcv_que.add(seg);
                rcv_nxt++;
                // Remove from receive buffer
                mapIterator.remove();
            } else {
                break;
            }
        }
    }

    public static int conv(byte[] data) {
        if (data.length < IKCP_OVERHEAD) {
            return 0;
        }
        return Utils.byteArrayToInt(data, 0);
    }

    // when you received a low level packet (eg. UDP packet), call it
    //---------------------------------------------------------------------
    // input data
    //---------------------------------------------------------------------
    // The bottom layer is called after the packet is received, and then the upper layer obtains the processed data through the Recv.
    public int input(byte[] data) {

        long s_una = snd_una;
        int flag = 0, maxack = 0;
        if (data.length < IKCP_OVERHEAD) {
            LOG.debug("data to long");
            return -1;
        }

        int offset = 0;

        while (true) {

            boolean readed = false;
            long ts, sn, length, una;
            int wnd;
            byte cmd, frg;

            if (data.length - offset < IKCP_OVERHEAD) {
                break;
            }

            int currentConversationId = Utils.byteArrayToInt(data, offset);
            offset += 4;
            if (conversationId != currentConversationId) {
                LOG.debug("conv wrong {} != {} at {}", conversationId, currentConversationId, offset);
                return -1;
            }

            cmd = data[offset];
            offset += 1;
            frg = data[offset];
            offset += 1;
            wnd = Utils.byteArrayToShort(data, offset);
            offset += 2;
            ts = Utils.byteArrayToUint(data, offset);
            offset += 4;
            sn = Utils.byteArrayToUint(data, offset);
            offset += 4;
            una = Utils.byteArrayToUint(data, offset);
            offset += 4;
            length = Utils.byteArrayToUint(data, offset);
            offset += 4;

            if (data.length - offset < length) {
                LOG.debug("offset wrong");
                return -2;
            }

            if (cmd != IKCP_CMD_PUSH && cmd != IKCP_CMD_ACK && cmd != IKCP_CMD_WASK && cmd != IKCP_CMD_WINS) {
                LOG.debug("wrong command");
                return -3;
            }

            rmt_wnd = wnd;
            parse_una(una);
            shrink_buf();

            if (IKCP_CMD_ACK == cmd) {
                if (currentTs - ts >= 0) {
                    update_ack((int) (currentTs - ts));
                }
                parse_ack(sn);
                shrink_buf();
                if (flag == 0)  {
                    flag = 1;
                    maxack = (int)sn;
                } else if (sn - maxack > 0) {
                    maxack = (int)sn;
                }
            } else if (IKCP_CMD_PUSH == cmd) {
                if (sn - (rcv_nxt + rcv_wnd) < 0) {
                    ack_push(sn, ts);
                    if (sn - rcv_nxt >= 0) {
                        Segment seg = new Segment((int) length);
                        seg.conversationId = currentConversationId;
                        seg.cmd = cmd;
                        seg.frg = frg;
                        seg.wnd = wnd;
                        seg.ts = ts;
                        seg.sn = sn;
                        seg.una = una;

                        if (length > 0) {
                            System.arraycopy(data, offset, seg.data, 0, (int) length);
                            readed = true;
                        }

                        parse_data(seg);
                    }
                }
            } else if (IKCP_CMD_WASK == cmd) {
                // ready to send back IKCP_CMD_WINS in Ikcp_flush
                // tell remote my window size
                probe |= IKCP_ASK_TELL;
            } else if (IKCP_CMD_WINS == cmd) {
                // do nothing
            } else {
                LOG.debug("something wrong");
                return -3;
            }

            if (!readed) {
                offset += (int) length;
            }
        }

        if (flag != 0) {
            parse_fastack(maxack);
        }

        if (snd_una - s_una > 0) {
            if (cwnd < rmt_wnd) {
                //long mss_ = mss;
                if (cwnd < ssthresh) {
                    //slow start
                    cwnd+=cwnd;
                } else {
                    //congestion avoidance
                    cwnd++;
                }
                if (cwnd > rmt_wnd) {
                    cwnd = rmt_wnd;
                }
            }
        }

        // https://github.com/xtaci/kcp-go/blob/master/kcp.go acks immediately

        //if ackNoDelay && len(kcp.acklist) > 0 { // ack immediately
        //		kcp.flush(true)
        //}

        /*if (!acklist.isEmpty()) {
            LOG.debug("Flush ACKs");
            flush(true);
        }*/

        return 0;
    }

    // Receive window available size
    private int wnd_unused() {
        if (nrcv_que.size() < rcv_wnd) {
            return rcv_wnd - nrcv_que.size();
        }
        return 0;
    }

    //---------------------------------------------------------------------
    // ikcp_flush
    //---------------------------------------------------------------------
    private void flush(boolean ackOnly) {
        long current_ = currentTs;
        int change = 0;
        int lost = 0;

        Segment seg = new Segment(0);
        seg.conversationId = conversationId;
        seg.cmd = IKCP_CMD_ACK;
        seg.wnd = wnd_unused();
        seg.una = rcv_nxt;

        // flush acknowledges
        // Send the ack in acklist
        int count = acklist.size() / 2;
        int offset = 0;
        for (int i = 0; i < count; i++) {
            if (offset + IKCP_OVERHEAD > mtu) {
                kcpListener.output(buffer, 0, offset);
                offset = 0;
            }
            // ikcp_ack_get
            seg.sn = acklist.get(i * 2 + 0);
            seg.ts = acklist.get(i * 2 + 1);
            offset += seg.encode(buffer, offset);
        }
        acklist.clear();
        if(ackOnly) {
            return;
        }

        // probe window size (if remote window size equals zero)
        // rmt_wnd=0，determine whether you need to request the peer receiving window
        if (0 == rmt_wnd) {
            if (0 == probe_wait) {
                probe_wait = IKCP_PROBE_INIT;
                ts_probe = currentTs + probe_wait;
            } else {
                // Gradually increase the request interval
                if (currentTs - ts_probe >= 0) {
                    if (probe_wait < IKCP_PROBE_INIT) {
                        probe_wait = IKCP_PROBE_INIT;
                    }
                    probe_wait += probe_wait / 2;
                    if (probe_wait > IKCP_PROBE_LIMIT) {
                        probe_wait = IKCP_PROBE_LIMIT;
                    }
                    ts_probe = currentTs + probe_wait;
                    probe |= IKCP_ASK_SEND;
                }
            }
        } else {
            ts_probe = 0;
            probe_wait = 0;
        }

        // flush window probing commands
        // Request peer receiving window
        if ((probe & IKCP_ASK_SEND) != 0) {
            seg.cmd = IKCP_CMD_WASK;
            if (offset + IKCP_OVERHEAD > mtu) {
                kcpListener.output(buffer, 0, offset);
                offset = 0;
            }
            offset += seg.encode(buffer, offset);
        }

        // flush window probing commands(c#)
        // Tell the peer's own receiving window
        if ((probe & IKCP_ASK_TELL) != 0) {
            seg.cmd = IKCP_CMD_WINS;
            if (offset + IKCP_OVERHEAD > mtu) {
                kcpListener.output(buffer, 0, offset);
                offset = 0;
            }
            offset += seg.encode(buffer, offset);
        }

        probe = 0;

        // calculate window size
        long cwnd_ = Math.min(snd_wnd, rmt_wnd);
        // If congestion control is used
        if (congestionControlEnabled) {
            cwnd_ = Math.min(cwnd, cwnd_);
        }

        // move data from snd_queue to snd_buf
        Iterator<Segment> segmentIterator = nsnd_que.iterator();
        //for (Segment nsnd_que1 : nsnd_que) {
        while(segmentIterator.hasNext()) {
            Segment newseg = segmentIterator.next();
            if (snd_nxt - (snd_una + cwnd_) >= 0) {
                break;
            }
            newseg.conversationId = conversationId;
            newseg.cmd = IKCP_CMD_PUSH;
            newseg.wnd = seg.wnd;
            newseg.ts = current_;
            newseg.sn = snd_nxt;
            newseg.una = rcv_nxt;
            newseg.resendts = current_;
            newseg.rto = rx_rto;
            newseg.fastack = 0;
            newseg.xmit = 0;
            nsnd_buf.put(newseg.sn, newseg);
            snd_nxt++;
            segmentIterator.remove();
        }

        // calculate resent
        long resent = (fastresend > 0) ? fastresend : -1;
        long rtomin = (!nodelayEnabled) ? (rx_rto >> 3) : 0;

        // flush data segments
        for (Segment segment : nsnd_buf.values()) {
            boolean needsend = false;
            if (0 == segment.xmit) {
                // First transmission
                needsend = true;
                segment.xmit++;
                segment.rto = rx_rto;
                segment.resendts = current_ + segment.rto + rtomin;
            } else if (current_ - segment.resendts >= 0) {
                // Packet loss retransmission
                needsend = true;
                segment.xmit++;
                xmit++;
                if (!nodelayEnabled) {
                    segment.rto += rx_rto;
                } else {
                    segment.rto += rx_rto / 2;
                }
                segment.resendts = current_ + segment.rto;
                lost = 1;
            } else if (segment.fastack >= resent) {
                // Fast retransmission
                needsend = true;
                segment.xmit++;
                segment.fastack = 0;
                segment.resendts = current_ + segment.rto;
                change++;
            }

            if (needsend) {
                segment.ts = current_;
                segment.wnd = seg.wnd;
                segment.una = rcv_nxt;

                int need = IKCP_OVERHEAD + segment.data.length;
                //as in https://github.com/beykery/jkcp/blob/master/src/main/java/org/beykery/jkcp/Kcp.java
                // it is > and not >=, otherwise we see an empty packet
                if (offset + need > mtu) {
                    kcpListener.output(buffer, 0, offset);
                    offset = 0;
                }

                offset += segment.encode(buffer, offset);
                if (segment.data.length > 0) {
                    System.arraycopy(segment.data, 0, buffer, offset, segment.data.length);
                    offset += segment.data.length;
                }

                if (segment.xmit >= dead_link) {
                    state = -1; // state = 0(c#)
                }
            }
        }

        // flash remain segments
        if (offset > 0) {
            kcpListener.output(buffer, 0, offset);
        }

        // update ssthresh
        // Congestion avoidance
        if (change != 0) {
            long inflight = snd_nxt - snd_una;
            ssthresh = inflight / 2;
            if (ssthresh < IKCP_THRESH_MIN) {
                ssthresh = IKCP_THRESH_MIN;
            }
            cwnd = ssthresh + resent;
        }

        if (lost != 0) {
            ssthresh = cwnd / 2;
            if (ssthresh < IKCP_THRESH_MIN) {
                ssthresh = IKCP_THRESH_MIN;
            }
            cwnd = 1;
        }

        if (cwnd < 1) {
            cwnd = 1;
        }
    }

    //---------------------------------------------------------------------
    // update state (call it repeatedly, every 10ms-100ms), or you can ask
    // ikcp_check when to call it again (without ikcp_input/_send calling).
    // 'current' - current timestamp in millisec.
    //---------------------------------------------------------------------
    public void update(final long currentTs) {
        //Call Update for the first time
        if(this.currentTs == 0) {
            this.ts_flush = currentTs;
        }
        this.currentTs = currentTs;

        //Two update intervals
        long slap = this.currentTs - this.ts_flush;

        //The interval setting is too large or the Update call is too long
        if (slap >= 10000 || slap < -10000)
        {
            this.ts_flush = this.currentTs;
            slap = 0;
        }

        // flush sets the next update time
        if (slap >= 0)
        {
            this.ts_flush += this.interval;
            if (this.currentTs - this.ts_flush >= 0)
            {
                this.ts_flush = this.currentTs + this.interval;
            }
            flush(false);
        }
    }

    /**
     * Change MTU size, default is 1400
     *
     * @param mtu_ Pure algorithm protocol is not responsible for MTU detection, the default mtu is 1400 bytes,
     *            which can be set using ikcp_setmtu. The value will affect the maximum transmission unit
     *             upon data packet merging and fragmentation.
     * @return -1 if too small, -2 if too large
     */
    public int setMtu(int mtu_) {
        if (mtu_ < 50 || mtu_ < IKCP_OVERHEAD) {
            return -1;
        }

        byte[] buffer_ = new byte[(mtu_ + IKCP_OVERHEAD) * 3];
        if (null == buffer_) {
            return -2;
        }

        mtu = mtu_;
        mss = mtu - IKCP_OVERHEAD;
        buffer = buffer_;
        return 0;
    }

    /**
     *
     * @param interval internal update timer interval in millisec, default is 100ms
     * @return the current interval
     */
    public int interval(int interval) {
        if (interval > 5000) {
            this.interval = 5000;
        } else if (interval < 10) {
            this.interval = 10;
        } else {
            this.interval = interval;
        }
        return this.interval;
    }

    /**
     * normal mode: noDelay(false, 40, 0, true);
     * fastest:     noDelay(true, 10, 2, false);
     *
     *nodelay: 0:disable(default), 1:enable
     *interval: internal update timer interval in millisec, default is 100ms
     *resend: 0:disable fast resend(default), 1:enable fast resend
     *nc: 0:normal congestion control(default), 1:disable congestion control
     *
     * @param nodelayEnabled Whether nodelay mode is enabled, 0 is not enabled; 1 enabled.
     * @param interval Protocol internal work interval, in milliseconds, such as 10 ms or 20 ms.
     * @param fastresend Fast retransmission mode, 0 is off by default, 2 can be set (2 ACK spans will result in direct retransmission)
     * @param congestionControlEnabled Whether to turn off flow control, false to disable
     * @return this
     */
    public KCP noDelay(boolean nodelayEnabled, int interval, int fastresend, boolean congestionControlEnabled) {
        /*
        * No matter TCP or KCP, they have the limitation for the minimum RTO when calculating
        * the RTO, even if the calculated RTO is 40ms, as the default RTO is 100ms, the protocol
        * can only detect packet loss after 100ms, which is 30ms in the fast mode, and the
        * value can be manually changed:
        */

        this.nodelayEnabled = nodelayEnabled;
        this.rx_minrto = nodelayEnabled ? IKCP_RTO_NDL : IKCP_RTO_MIN;
        interval(interval);
        this.fastresend = fastresend;
        this.congestionControlEnabled = congestionControlEnabled;
        return this;
    }

    /**
     * Set maximum window size: sndwnd=32, rcvwnd=32 by default. The call will set the maximum send window and maximum
     * receive window size of the procotol, which is 32 by default.
     *
     * @param sndwnd The number of packets in the send window
     * @param rcvwnd The number of packets in the receive window
     * @return
     */
    public KCP wndSize(int sndwnd, int rcvwnd) {
        if (sndwnd > 0) {
            snd_wnd = sndwnd;
        }

        if (rcvwnd > 0) {
            rcv_wnd = rcvwnd;
        }
        return this;
    }

    /**
     * @return How many packet is waiting to be sent
     */

    public int waitSnd() {
        return nsnd_buf.size() + nsnd_que.size();
    }

    /**
     * @return How many bytes can a buffer be when calling {@link #recv(byte[])}
     */
    public int maxRcvBuffer() {
        return rcv_wnd*mss;
    }
}
