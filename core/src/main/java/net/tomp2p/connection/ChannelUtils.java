package net.tomp2p.connection;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import net.tomp2p.peers.IP;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.IP.IPv6;

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

	public static byte[] convert2(ByteBuf buf) {
		byte[] bytes = new byte[buf.readableBytes()];
		buf.readBytes(bytes);
		return bytes;
	}
	
	/*public static int localSctpPort(InetSocketAddress inetSocketAddress) {
		int port = inetSocketAddress.getPort();
		if(inetSocketAddress.getAddress() instanceof Inet4Address) {
			return localSctpPort(IP.fromInet4Address((Inet4Address)inetSocketAddress.getAddress()), port);
		} else {
			return localSctpPort(IP.fromInet6Address((Inet6Address)inetSocketAddress.getAddress()), port);
		}
	}

	public static int localSctpPort(IPv4 ipv4, int udpPort) {
		int sctpPort = Math.abs(((ipv4.toInt() ^ udpPort)) % ChannelTransceiver.MAX_PORT - 1) + 1;
		LOG.debug("port calculation based on: {}/{}={}", ipv4.toInt(), udpPort, sctpPort);
		return sctpPort;
	}
	
	public static int localSctpPort(IPv6 ipv6, int port) {
		return Math.abs(((Long.hashCode(ipv6.toLongHi()) ^ Long.hashCode(ipv6.toLongLo()) ^ port)) % ChannelTransceiver.MAX_PORT - 1) + 1;
	}*/
}
