package net.tomp2p.peers;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import net.tomp2p.utils.Utils;

public class TestIP {
	@Test
	public void testMask4() throws UnknownHostException {
		InetAddress inet = Inet4Address.getByName("192.168.1.44");
		IP.IPv4 v4 = IP.fromInet4Address(inet);
		InetAddress inet2 = v4.toInetAddress();
		Assert.assertEquals(inet, inet2);
	}
	
	@Test
	public void testMask6() throws UnknownHostException {
		InetAddress inet = Inet6Address.getByName("2607:f0d0:1002:0051:0000:0000:0000:0004");
		IP.IPv6 v6 = IP.fromInet6Address(inet);
		InetAddress inet2 = v6.toInetAddress();
		Assert.assertEquals(inet, inet2);
	}
	
	@Test
	public void testMask4with24() throws UnknownHostException {
		InetAddress inet = Inet4Address.getByName("192.168.1.44");
		IP.IPv4 v4 = IP.fromInet4Address(inet);
		v4 = v4.maskWithNetworkMaskInv(24);
		InetAddress inet2 = Inet4Address.getByName("0.0.0.44");
		InetAddress inet3 = v4.toInetAddress();
		Assert.assertEquals(inet2, inet3);
	}
	
	@Test
	public void testMask4Set() throws UnknownHostException {
		InetAddress inet = Inet4Address.getByName("10.10.10.44");
		IP.IPv4 v4 = IP.fromInet4Address(inet);
		IP.IPv4 mask = v4.maskWithNetworkMask(24);
		
		InetAddress inet1 = Inet4Address.getByName("192.168.1.55");
		v4 = IP.fromInet4Address(inet1);
		IP.IPv4 masked = v4.maskWithNetworkMaskInv(24);
		
		IP.IPv4 test = mask.set(masked);
		
		InetAddress inet2 = Inet4Address.getByName("10.10.10.55");
		InetAddress inet3 = test.toInetAddress();
		Assert.assertEquals(inet2, inet3);
	}
	
	@Test
	public void testCalc1() throws UnknownHostException {
		
		IP.IPv4 mask = IP.fromInet4Address(InetAddress.getByName("10.0.0.3"));
		mask = mask.maskWithNetworkMask(24);
		IP.IPv4 masked = IP.fromInet4Address(InetAddress.getByName("0.0.0.2"));
		masked = masked.maskWithNetworkMaskInv(24);
		InetAddress i = mask.set(masked).toInetAddress();
		Assert.assertEquals(i, InetAddress.getByName("10.0.0.2") );
	}
	
	@Test
	public void testCalc2() throws UnknownHostException {
		
		IP.IPv4 mask = IP.fromInet4Address(InetAddress.getByName("10.0.0.3"));
		mask = mask.maskWithNetworkMask(16);
		IP.IPv4 masked = IP.fromInet4Address(InetAddress.getByName("0.0.1.2"));
		masked = masked.maskWithNetworkMaskInv(16);
		InetAddress i = mask.set(masked).toInetAddress();
		Assert.assertEquals(i, InetAddress.getByName("10.0.1.2") );
	}
	
	@Test
	public void testCalc3() throws UnknownHostException {
		
		IP.IPv4 mask = IP.fromInet4Address(InetAddress.getByName("10.0.0.3"));
		System.err.println(mask);
		mask = mask.maskWithNetworkMask(0);
		System.err.println(mask);
		IP.IPv4 masked = IP.fromInet4Address(InetAddress.getByName("1.2.3.4"));
		masked = masked.maskWithNetworkMaskInv(0);
		InetAddress i = mask.set(masked).toInetAddress();
		Assert.assertEquals(i, InetAddress.getByName("1.2.3.4") );
	}
}
