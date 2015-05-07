package net.tomp2p.holep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Add the following lines to sudoers
 * username ALL=(ALL) NOPASSWD: <location>/nat-net.sh
 * username ALL=(ALL) NOPASSWD: /usr/bin/ip
 * 
 * Make sure the network namespaces can resolve the hostname, otherwise huge delays are to be expected.
 * 
 * This testcase runs on a single machine and tests the two widely used NAT settings (port-preserving and symmetric). 
 * However, most likely this will never run on travis-ci as this requires some extra setup on the machine itself. 
 * Thus, this test-case is disabled by default and tests have to be performed manully.
 * 
 * @author Thomas Bocek
 *
 */
@Ignore
public class TestNATTypeDetection {
	final static private Random RND = new Random(42);
	static private Peer relayPeer = null;
	static private Number160 relayPeerId = new Number160(RND);
	
	@Before
	public void before() throws IOException, InterruptedException {
		executeNatSetup("start", "0");
		executeNatSetup("start", "1", "sym");
	}

	@After
	public void after() throws IOException, InterruptedException {
		executeNatSetup("stop", "0");
		executeNatSetup("stop", "1");
	}

	private static int executeNatSetup(String action, String... cmd) throws IOException, InterruptedException {
		String startDir = System.getProperty("user.dir");
		String[] cmds = new String[cmd.length + 3];
		cmds[0] = "/usr/bin/sudo";
		cmds[1] = startDir+"/src/test/resources/nat-net.sh";
		cmds[2] = action;
		for(int i=3;i<cmds.length;i++) {
			cmds[i] = cmd[i-3];
		}
		final ProcessBuilder builder = new ProcessBuilder(cmds);
		builder.redirectError(ProcessBuilder.Redirect.INHERIT);
		Process process = builder.start();
		process.waitFor();
		return process.exitValue();
	}

	private static Process executePeer(Class<?> klass, String arg1, String arg2) throws IOException, InterruptedException {
		String javaHome = System.getProperty("java.home");
		String javaBin = javaHome + File.separator + "bin" + File.separator
		        + "java";
		String classpath = System.getProperty("java.class.path");
		String className = klass.getCanonicalName();

		ProcessBuilder builder = new ProcessBuilder("/usr/bin/sudo", "ip", "netns", "exec", "unr" + arg1, javaBin, "-cp", classpath, className, arg1, arg2);

		builder.redirectError(ProcessBuilder.Redirect.INHERIT);
		for(String cmd:builder.command()) {
			System.err.print(cmd+" ");
		}
		Process process = builder.start();
		System.err.println("executed.");
		waitForLineOrDie(process, "started", null);
		System.err.println("peer started");
		return process;
	}

	private static String waitForLineOrDie(Process process, String prefix, String input) throws IOException {

		// Write to stream
		if (input != null) {
			PrintWriter pw = new PrintWriter(process.getOutputStream());
			pw.println(input);
			pw.flush();
		}
		String retVal = read(process.getInputStream(), prefix);
		if(retVal == null) {
			process.destroy();
		}
		return retVal;
	}
	
	private static String read(InputStream is, String prefix) throws IOException {
		// Read out dir output
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;
		
		while ((line = br.readLine()) != null) {
			if (line.trim().startsWith(prefix)) {
				String retVal = line.substring(prefix.length()).trim(); 
				System.err.println("found: "+prefix+" -> "+retVal);
				return retVal;
			}
		}
		System.err.println("null");
		return null;
	}

	private static int killPeer(Process process) throws InterruptedException, IOException {
		process.destroy();
		process.getErrorStream().close();
		process.getInputStream().close();
		process.getOutputStream().close();
		return process.waitFor();
	}
	
	/**
	 * As set in: tomp2p/nat/src/test/resources/nat-net.sh
	 */
	private static Peer createRealNode() throws Exception {
		// relay
		Bindings b2 = new Bindings();
		b2.addInterface("enp0s25");
		return new PeerBuilder(relayPeerId).ports(5002).bindings(b2).start();
	}

	/**
	 * If you have a terrible lag in InetAddress.getLocalHost(), make sure the hostname resolves in the other network domain.
	 */
	public static void main(String[] args) throws IOException {
		Peer peer = null;
		System.err.println("started on " +InetAddress.getLocalHost());
		try {
			Bindings b0 = new Bindings();
			int nr = Integer.parseInt(args[0]);
			Random rnd = new Random(args[0].hashCode());
			b0.addAddress(InetAddress.getByName("10.0." + args[0] + ".2"));
			peer = new PeerBuilder(new Number160(rnd)).ports(nr + 5000).bindings(b0).start();
			System.out.println("started " + peer.peerID());
			System.err.println("started " + peer.peerID());
			String command = read(System.in, "command");
			if (command.equals("detect")) {
				System.err.println("connect to relay at "+ args[1]);
				PeerAddress relayP = new PeerAddress(relayPeerId, args[1], 5002, 5002);
				FutureDone<NATType> type = NATTypeDetection.checkNATType(peer, relayP).awaitUninterruptibly();
				System.err.println("done " + type.failedReason());
				if(type.isSuccess()) {
					System.out.println("done " + type.object().name());
				} else {
					System.out.println("done " + type.failedReason());
				}
			} else {
				System.out.println("empty");
			}
		} finally {
			System.out.println("finish");
			if(peer != null) {
				peer.shutdown().awaitUninterruptibly();
			}
		}
	}

	@Test
	public void testDetection() throws Exception {
		relayPeer = null;
		Process unr1 = null;
		Process unr2 = null;
		try {
			relayPeer = createRealNode();
			InetAddress relayAddress = relayPeer.peerAddress().inetAddress();
			unr1 = executePeer(TestNATTypeDetection.class, "0", relayAddress.getHostAddress());
			unr2 = executePeer(TestNATTypeDetection.class, "1", relayAddress.getHostAddress());
			String result1 = waitForLineOrDie(unr1, "done", "command detect");
			String result2 = waitForLineOrDie(unr2, "done", "command detect");
			
			Assert.assertEquals(NATType.PORT_PRESERVING.toString(), result1);
			Assert.assertEquals(NATType.NON_PRESERVING_OTHER.toString(), result2);
			
		} finally {
			System.err.print("shutdown.");
			if (relayPeer != null) {
				relayPeer.shutdown().awaitUninterruptibly();
				relayPeer = null;
			}
			System.err.print(".");
			if (unr1 != null) {
				killPeer(unr1);
			}
			System.err.println(".");
			if (unr2 != null) {
				killPeer(unr2);
			}
		}
	}

	
}
