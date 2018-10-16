package net.tomp2p.connection;

import net.tomp2p.utils.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * http://thushw.blogspot.com/2011/06/asynchronous-udp-server-using-java-nio.html
 * https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=10&ved=2ahUKEwjfqMTj-f3dAhXjhaYKHReWC8gQFjAJegQIARAB&url=http%3A%2F%2Fcs.baylor.edu%2F~donahoo%2Fpractical%2FJavaSockets2%2Fcode%2FUDPEchoServerSelector.java&usg=AOvVaw0agfzsqJ7N3uea5QHAVMgj
 */

public class AsyncUDPServer implements OutgoingData {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncUDPServer.class);

    private final AsyncUDPServer.IncomingData incomingData;
    private final DatagramChannel channel;
    private final Queue<Triple<InetSocketAddress, ByteBuffer, CompletableFuture<Integer>>> writeQueue = new ConcurrentLinkedQueue<>();
    private final SelectionKey clientKey;
    private final Selector selector;
    private final InetSocketAddress localSocket;
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    public AsyncUDPServer(final AsyncUDPServer.IncomingData incomingData, final InetSocketAddress bindTo) throws IOException {
        this.incomingData = incomingData;
        this.channel = DatagramChannel.open();
        this.channel.socket().bind(bindTo);
        final DatagramSocket s = channel.socket();
        this.localSocket = new InetSocketAddress(s.getLocalAddress(), s.getLocalPort());
        this.channel.configureBlocking(false);
        this.selector = Selector.open();
        this.clientKey = channel.register(selector, SelectionKey.OP_READ);
        LOG.debug("bound to {}", bindTo);
    }

    public CompletableFuture<Void> shutdown() throws IOException {
        this.selector.close();
        this.channel.close();
        Triple<InetSocketAddress, ByteBuffer, CompletableFuture<Integer>> triple = null;
        while((triple = writeQueue.poll())!=null) {
            triple.e2().completeExceptionally(new IOException("Shutdown in progres"));
        }
        return shutdownFuture;
    }

    public interface IncomingData {
        void incoming(InetSocketAddress sa, ByteBuffer buffer, OutgoingData outgoingData);
    }

    @Override
    public CompletableFuture<Integer> send(final InetSocketAddress address, final byte[] data, int offset, int length) {
        LOG.debug("add data (len: {}) to send queue, recipient {}", length, address);
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            writeQueue.add(Triple.of(address, ByteBuffer.wrap(data, offset, length), future));
            clientKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            selector.wakeup();
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public InetSocketAddress localSocket() {
        return localSocket;
    }

    public void process() {
        try {
            while (true) {
                if (selector.select(500) == 0) {
                    if (!selector.isOpen()) {
                        return;
                    }
                    continue;
                }
                final Iterator selectedKeys = selector.selectedKeys().iterator();
                while (selectedKeys.hasNext()) {
                    final SelectionKey key = (SelectionKey) selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isReadable()) {
                        read(key);
                    } else if (key.isWritable()) {
                        key.interestOps(SelectionKey.OP_READ);
                        write(key);
                    }
                }
            }
        } catch (IOException e) {
            LOG.debug("Connection close. ", e);
        } finally {
            shutdownFuture.complete(null);
        }
    }

    private void read(final SelectionKey key) {
        final DatagramChannel chan = (DatagramChannel) key.channel();
        byte[] me = new byte[1500];
        final ByteBuffer b = ByteBuffer.wrap(me);
        try {
            final InetSocketAddress remote = (InetSocketAddress) chan.receive(b);
            b.flip();
            LOG.debug("read data (len: {}) from {}", b.remaining(), remote);
            incomingData.incoming(remote, b, AsyncUDPServer.this);
        } catch (IOException e) {
            LOG.warn("glitch, continuing... ", e);
        }
    }

    private void write(final SelectionKey key) {
        final DatagramChannel chan = (DatagramChannel) key.channel();
        Triple<InetSocketAddress, ByteBuffer, CompletableFuture<Integer>> triple = null;
        while((triple = writeQueue.poll())!=null) {
            if (triple != null) {
                try {
                    LOG.debug("write data (len: {}) to {}", triple.e1().remaining(), triple.e0());
                    final int result = chan.send(triple.e1(), triple.e0());
                    if(result == 0) {
                        LOG.warn("message dropped, being busy");
                        triple.e2().completeExceptionally(new IOException("message dropped, being busy"));
                    } else {
                        LOG.debug("wrote {} bytes", result);
                        triple.e2().complete(result);
                    }
                } catch (IOException e) {
                    triple.e2().completeExceptionally(e);
                }
            }
        }
    }
}