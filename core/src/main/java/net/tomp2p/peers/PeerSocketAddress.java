package net.tomp2p.peers;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.experimental.Wither;
import net.tomp2p.peers.IP.IPv4;
import net.tomp2p.peers.IP.IPv6;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

public abstract class PeerSocketAddress {
	
	public final static int PORT_SIZE = 2;
	public abstract int size();
	public abstract int encode(final byte[] array, int offset);
	public abstract PeerSocketAddress encode(final ByteBuffer buf);
	public abstract String toString();
	
	public abstract InetSocketAddress createUDPSocket();
	
	@Builder
	@RequiredArgsConstructor
	@Accessors(fluent = true, chain = true)
	public static class PeerSocket4Address extends PeerSocketAddress {
		
		//ports + ip size
		public final static int SIZE = PORT_SIZE + 4;
		
		@Getter @Wither final private IPv4 ipv4;
		@Getter @Wither final private int udpPort;

		
		public static Pair<PeerSocket4Address, Integer> decode(final byte[] array, int offset) {
			PeerSocket4AddressBuilder builder = new PeerSocket4AddressBuilder();

			final int ip = Utils.byteArrayToInt(array, offset);
			offset +=4;
			builder.ipv4(IP.fromInt(ip));

			final int udpPort = Utils.byteArrayToShort(array, offset);
			offset +=2;
			return new Pair<PeerSocket4Address, Integer> (
					builder.udpPort(udpPort)
						.build(), offset);
		}

		public static PeerSocket4Address decode(ByteBuffer buf) {
			return new PeerSocket4AddressBuilder()
					.ipv4(IP.fromInt(buf.getInt()))
					.udpPort(0x0000ffff & buf.getShort())
					.build();
		}
		
		@Override
		public int encode(final byte[] array, int offset) {
			offset = Utils.intToByteArray(ipv4.toInt(), array, offset);
			offset = Utils.shortToByteArray(udpPort, array, offset);
			return offset;
		}

		@Override
		public PeerSocket4Address encode(final ByteBuffer buf) {
			buf.putInt(ipv4.toInt());
			buf.putShort((short)udpPort);
			return this;
		}
		
		public InetSocketAddress createUDPSocket() {
			return new InetSocketAddress(ipv4.toInet4Address(), udpPort);
		}
		
		public InetSocketAddress createSocket(int port) {
			return new InetSocketAddress(ipv4.toInet4Address(), port);
		}

		@Override
		public int size() {
			return SIZE;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
	        return sb.append(ipv4)
	        	.append("/")
	        	.append(udpPort).toString();
		}
		
		@Override
	    public boolean equals(final Object obj) {
	        if (!(obj instanceof PeerSocket4Address)) {
	            return false;
	        }
	        if (this == obj) {
	            return true;
	        }
	        final PeerSocket4Address psa = (PeerSocket4Address) obj;
	        return Utils.equals(psa.ipv4, ipv4) 
	        		&& psa.udpPort == udpPort;
	    }
	    
	    public boolean equalsWithoutPorts(final Object obj) {
	        if (!(obj instanceof PeerSocket4Address)) {
	            return false;
	        }
	        if (this == obj) {
	            return true;
	        }
	        final PeerSocket4Address psa = (PeerSocket4Address) obj;
	        return Utils.equals(psa.ipv4, ipv4);
	    }
	    
	    @Override
	    public int hashCode() {
	    	return Utils.hashCode(ipv4) ^ udpPort;
	    }
	    
	    public static PeerSocket4Address create(Inet4Address inet, int udpPort) {
			return PeerSocket4Address.builder().ipv4(IP.fromInet4Address(inet)).udpPort(udpPort).build();
		}

	}
	
	@Builder
	@RequiredArgsConstructor
	@Accessors(fluent = true, chain = true)
	public static class PeerSocket6Address extends PeerSocketAddress {
		
		//ports + ip size
		public final static int SIZE = PORT_SIZE + 16;
		
		@Getter @Wither final private IPv6 ipv6;
		@Getter @Wither final private int udpPort;
		
		public static Pair<PeerSocket6Address, Integer> decode(final byte[] array, int offset) {
			return decode(array, offset, false);
		}
		
		public static Pair<PeerSocket6Address, Integer> decode(final byte[] array, int offset, final boolean skipAddress) {
			PeerSocket6AddressBuilder builder = new PeerSocket6AddressBuilder();
			if(!skipAddress) {
				final long hi = Utils.byteArrayToLong(array, offset);
				offset +=8;
				final long lo = Utils.byteArrayToLong(array, offset);
				offset +=8;
				builder.ipv6(IP.fromLong(hi, lo));
			}
			final int udpPort = Utils.byteArrayToShort(array, offset);
			offset +=2;
			
			return new Pair<PeerSocket6Address, Integer> (
					builder.udpPort(udpPort)
						.build(), offset);
		}

		public static PeerSocket6Address decode(ByteBuffer buf) {
			return new PeerSocket6AddressBuilder()
                    .ipv6(IP.fromLong(buf.getLong(),buf.getLong()))
			        .udpPort(0x0000ffff & buf.getShort())
					.build();
		}
		
		@Override
		public int encode(final byte[] array, int offset) {
			offset = Utils.longToByteArray(ipv6.toLongHi(), ipv6.toLongLo(), array, offset);
			offset = Utils.shortToByteArray(udpPort, array, offset);
			return offset;
		}

		@Override
		public PeerSocket6Address encode(final ByteBuffer buf) {
			buf.putLong(ipv6.toLongHi());
			buf.putLong(ipv6.toLongLo());
			buf.putShort((short)udpPort);
			return this;
		}
		
		public InetSocketAddress createUDPSocket() {
			return new InetSocketAddress(ipv6.toInet6Address(), udpPort);
		}
		
		@Override
		public int size() {
			return SIZE;
		}
		
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
	        return sb.append(ipv6)
	        	.append("/")
	        	.append(udpPort).toString();
		}
		
		@Override
	    public boolean equals(final Object obj) {
	        if (!(obj instanceof PeerSocket6Address)) {
	            return false;
	        }
	        if (this == obj) {
	            return true;
	        }
	        final PeerSocket6Address psa = (PeerSocket6Address) obj;
	        return Utils.equals(psa.ipv6, ipv6) 
	        		&& psa.udpPort == udpPort;
	    }
	    
	    public boolean equalsWithoutPorts(final Object obj) {
	        if (!(obj instanceof PeerSocket6Address)) {
	            return false;
	        }
	        if (this == obj) {
	            return true;
	        }
	        final PeerSocket6Address psa = (PeerSocket6Address) obj;
	        return Utils.equals(psa.ipv6, ipv6);
	    }
	    
	    @Override
	    public int hashCode() {
	    	return Utils.hashCode(ipv6) ^ udpPort;
	    }
	}

	public static PeerSocketAddress create(InetAddress inet, int udpPort) {
		if(inet instanceof Inet4Address) {
			return PeerSocket4Address.builder().ipv4(IP.fromInet4Address((Inet4Address)inet)).udpPort(udpPort).build();
		} else {
			return PeerSocket6Address.builder().ipv6(IP.fromInet6Address((Inet6Address)inet)).udpPort(udpPort).build();
		}
	}

    public static PeerSocketAddress create(InetSocketAddress sock) {
        if(sock.getAddress() instanceof Inet4Address) {
            return PeerSocket4Address.builder().ipv4(IP.fromInet4Address((Inet4Address)sock.getAddress())).udpPort(sock.getPort()).build();
        } else {
            return PeerSocket6Address.builder().ipv6(IP.fromInet6Address((Inet6Address)sock.getAddress())).udpPort(sock.getPort()).build();
        }
    }
}
