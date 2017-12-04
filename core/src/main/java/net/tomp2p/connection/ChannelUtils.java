package net.tomp2p.connection;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import net.sctp4nat.core.SctpChannel;
import net.sctp4nat.core.SctpChannelBuilder;
import net.sctp4nat.core.SctpChannelFacade;
import net.sctp4nat.origin.SctpAcceptable;
import net.sctp4nat.origin.SctpNotification;
import net.sctp4nat.origin.SctpSocket.NotificationListener;
import net.sctp4nat.util.SctpUtils;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.IP.IPv4;

public class ChannelUtils {
	
	private static final Logger LOG = LoggerFactory.getLogger(ChannelUtils.class);
	
	public static ByteBuffer convert(ByteBuf buf) {
    	if(buf.nioBufferCount() == 0) {
    		return ByteBuffer.allocate(0);
    	} else if (buf.nioBufferCount() == 1) {
    		return buf.nioBuffer();
    	} else {
    		final byte[] bytes = new byte[buf.readableBytes()];
    		buf.getBytes(buf.readerIndex(), bytes);
    	    return ByteBuffer.wrap(bytes);
    	}
    }

	public static int localSctpPort(IPv4 ipv4, int port) {
		return Math.abs(((ipv4.toInt() ^ port)) % ChannelServer.MAX_PORT) + 1;
	}
	
	public static SctpChannel creatSCTPSocket(InetAddress remoteAddress, int remoteSctpPort, int localSctpPort, 
			FutureDone<SctpChannelFacade> futureSCTP) throws net.sctp4nat.util.SctpInitException {
		final SctpChannel socket = new SctpChannelBuilder().localSctpPort(localSctpPort)
				.remoteAddress(remoteAddress).remotePort(remoteSctpPort).mapper(SctpUtils.getMapper()).build();
		socket.listen();
		socket.setNotificationListener(new NotificationListener() {
			@Override
			public void onSctpNotification(SctpAcceptable socket2, SctpNotification notification) {
				LOG.debug("SCTP notification {}", notification.toString());
				if (notification.toString().indexOf("ADDR_CONFIRMED") >= 0) {
					futureSCTP.done((SctpChannelFacade) socket);
				} else if (notification.toString().indexOf("SHUTDOWN_COMP") >= 0) {
					socket.close();
					futureSCTP.failed("SHUTDOWN_COMP");
				} else if (notification.toString().indexOf("ADDR_UNREACHABLE") >= 0){
					LOG.error("Heartbeat missing! Now shutting down the SCTP connection...");
					socket.close();
					futureSCTP.failed("ADDR_UNREACHABLE");
				}  else if (notification.toString().indexOf("COMM_LOST") >= 0){
					LOG.error("Communication aborted! Now shutting down the udp connection...");
					socket.close();
					futureSCTP.failed("COMM_LOST");
				} 
			}
		});
		return socket;
	}
}
