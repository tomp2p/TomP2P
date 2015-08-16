package net.tomp2p.holep.manual;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class LocalNATUtils {
	private static final String TAG = "##BASE64##:";
	
	private static final RemotePeerCallback DEFAULT_CALLBACK = new RemotePeerCallback() {
		@Override
		public void onNull(int i) {}
		@Override
		public void onFinished(int i) {}
	};
	
	/**
	 * If you have a terrible lag in InetAddress.getLocalHost(), make sure the
	 * hostname resolves in the other network domain.
	 * 
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		LocalNATUtils.handleMain(args);
	}

	public static int executeNatSetup(String action, String... cmd)
			throws IOException, InterruptedException {
		String startDir = System.getProperty("user.dir");
		String[] cmds = new String[cmd.length + 3];
		cmds[0] = "/usr/bin/sudo";
		cmds[1] = startDir + "/src/test/resources/nat-net.sh";
		cmds[2] = action;
		for (int i = 3; i < cmds.length; i++) {
			cmds[i] = cmd[i - 3];
		}
		final ProcessBuilder builder = new ProcessBuilder(cmds);
		
		Process process = builder.start();
		new StreamGobbler(process.getErrorStream(), "ERR_SETUP["+cmd[0]+"]").start();
		new StreamGobbler(process.getInputStream(), "OUT_SETUP["+cmd[0]+"]").start();
		process.waitFor();
		return process.exitValue();
	}
	
	public static RemotePeer executePeer(int nr, final Command... cmd)
			throws IOException, InterruptedException, ClassNotFoundException {
		return executePeer(LocalNATUtils.class, nr, cmd);
	}
	
	public static RemotePeer executePeer(int nr, final RemotePeerCallback remoteCallback, final Command... cmd)
			throws IOException, InterruptedException, ClassNotFoundException {
		return executePeer(LocalNATUtils.class, nr, remoteCallback, cmd);
	}
	
	public static RemotePeer executePeer(Class<?> klass, int nr, final Command... cmd)
			throws IOException, InterruptedException, ClassNotFoundException {
		return executePeer(LocalNATUtils.class, nr, DEFAULT_CALLBACK, cmd);
	}

	public static RemotePeer executePeer(Class<?> klass, final int nr, final RemotePeerCallback remoteCallback, final Command... cmd)
			throws IOException, InterruptedException, ClassNotFoundException {
		String javaHome = System.getProperty("java.home");
		String javaBin = javaHome + File.separator + "bin" + File.separator
				+ "java";
		String classpath = System.getProperty("java.class.path");
		String className = klass.getCanonicalName();

		final String[] cmds = new String[cmd.length + 10];
		cmds[0] = "sudo";
		cmds[1] = "ip";
		cmds[2] = "netns";
		cmds[3] = "exec";
		cmds[4] = "unr" + nr;
		cmds[5] = javaBin;
		cmds[6] = "-cp";
		cmds[7] = classpath;
		cmds[8] = className;
		cmds[9] = ""+nr;
		for (int i = 10; i < cmds.length; i++) {
			cmds[i] = toString(cmd[i - 10]);
		}

		ProcessBuilder builder = new ProcessBuilder(cmds);
		final Process process = builder.start();
		new StreamGobbler(process.getErrorStream(), "ERR["+nr+"]").start();
		final CountDownLatch cl = new CountDownLatch(cmd.length);
		final AtomicReferenceArray<Object> results = new AtomicReferenceArray<Object>(cmd.length);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					InputStreamReader isr = new InputStreamReader(process.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					for (int i = 0; i < cmd.length; i++) {
						boolean done = false;
						
						while (!done) {
							String line = br.readLine();
							if(line != null) {
								line = line.trim();
								if (line.startsWith(TAG)) {
									line = line.substring(TAG.length());
									Object o = fromString(line);
									results.set(i, o);
									done = true;
								} else {
									System.out.println("OUT["+nr+"]>" + line);
								}
							} else {
								System.out.println("OUT["+nr+"]>null");
								cl.countDown();
								remoteCallback.onNull(i);
								break;
							}
						}
						cl.countDown();
						remoteCallback.onFinished(i);
					}
					process.getInputStream().close();
				} catch (Throwable t) {
					t.printStackTrace();
				}

			}
		}).start();
		System.out.println("LOCAL> remote peer "+nr+" started");
		return new RemotePeer(process, cl, cmd, results);
	}

	public static int killPeer(Process process) throws InterruptedException,
			IOException {
		process.getErrorStream().close();
		process.getInputStream().close();
		process.getOutputStream().close();
		process.destroy();
		return process.waitFor();
	}

	/**
	 * As set in: tomp2p/nat/src/test/resources/nat-net.sh
	 */
	public static Peer createRealNode(Number160 relayPeerId, String iface, int port)
			throws Exception {
		// relay
		Bindings b2 = new Bindings();
		b2.addInterface(iface);
		return new PeerBuilder(relayPeerId).ports(port).bindings(b2).start();
	}

	/**
	 * As seen in: http://stackoverflow.com/questions/134492/how-to-serialize-an-object-into-a-string
	 */
	public static Object fromString(String s) throws IOException,
			ClassNotFoundException {
		byte[] data = Base64.getDecoder().decode(s);
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
				data));
		Object o = ois.readObject();
		ois.close();
		return o;
	}

	/**
	 * As seen in: http://stackoverflow.com/questions/134492/how-to-serialize-an-object-into-a-string
	 */
	/** Write the object to a Base64 string. */
	public static String toString(Serializable o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		try {
			oos.writeObject(o);
		} catch (NotSerializableException e) {
			System.err.println("could not serialize object: " + o.getClass().getName());
			throw e;
		}
		oos.close();
		return Base64.getEncoder().encodeToString(baos.toByteArray());
	}

	public static Command[] toObjects(String[] args) throws ClassNotFoundException, IOException {
		Command[] cmd = new Command[args.length-1];
		for(int i=1;i<args.length;i++) {
			cmd[i-1] = (Command) fromString(args[i]);
		}
		return cmd;
	}

	public static void handleMain(String[] args) throws ClassNotFoundException, IOException {
		final Command[] cmds = LocalNATUtils.toObjects(args);
		for(Command cmd:cmds) {
			try {
				Serializable result = cmd.execute();
				System.out.println(TAG + LocalNATUtils.toString(result));
			} catch (Throwable e) {
				e.printStackTrace();
				System.out.println(TAG + LocalNATUtils.toString(e.getMessage()));
			}
		}
		System.out.flush();
	}
	
	public static PeerAddress peerAddress(String ip, int port, int peerId) throws UnknownHostException {
		return new PeerAddress(Number160.createHash(peerId), ip, port, port);
	}
	
	public static Peer init(String ip, int port, int peerId)
			throws UnknownHostException, IOException {
		Bindings b = new Bindings();
		b.addAddress(InetAddress.getByName(ip));
		ChannelClientConfiguration ccc = PeerBuilder.createDefaultChannelClientConfiguration();
		ccc.senderTCP(InetAddress.getByName(ip));
		Peer peer = new PeerBuilder(Number160.createHash(peerId)).channelClientConfiguration(ccc).ports(port).bindings(b).behindFirewall().start();
		System.out.println("Init "+peer.peerAddress());
		return peer;
	}
	
	public static Peer init(String ip, int port, int peerId, int forwardedPort)
			throws UnknownHostException, IOException {
		Bindings b = new Bindings();
		b.addAddress(InetAddress.getByName(ip));
		
		ChannelClientConfiguration ccc = PeerBuilder.createDefaultChannelClientConfiguration();

		ccc.senderTCP(InetAddress.getByName(ip));
		Peer peer = new PeerBuilder(Number160.createHash(peerId)).portsExternal(forwardedPort).channelClientConfiguration(ccc).ports(port).bindings(b)
				.start();
		System.out.println("Init "+peer.peerAddress());
		return peer;
	}
	
	public static Serializable shutdown(Peer... peers) {
		if(peers != null) {
			for(Peer peer: peers) {
				if (peer != null) {
					peer.shutdown().awaitUninterruptibly();
				}
			}
		}
		return "shutdown done";
	}

	public static void shutdown(RemotePeer... unrs) {
		if(unrs != null) {
			for(RemotePeer unr : unrs) {
				if (unr != null) {
					try {
						LocalNATUtils.killPeer(unr.process());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
	private static class StreamGobbler extends Thread {
	    final private InputStream is;
	    final private String type;

	    private StreamGobbler(final InputStream is, final String type) {
	        this.is = is;
	        this.type = type;
	    }

	    @Override
	    public void run() {
	        try {
	        	final InputStreamReader isr = new InputStreamReader(is);
	        	final BufferedReader br = new BufferedReader(isr);
	            String line = null;
	            while ((line = br.readLine()) != null) {
	                System.out.println(type + "> " + line);
	            }
	            System.out.flush();
	        }
	        catch (IOException ioe) {
	            ioe.printStackTrace();
	        }
	    }
	}
	
	public static Peer createNattedPeer(final String ip, final int port, final int nr, final String retVal)
			throws UnknownHostException, IOException {
		Peer peer = LocalNATUtils.init(ip, port, nr);
		peer.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				return retVal;
			}
		});
		return peer;
	}
	
	public static Peer createNattedPeer(final String ip, final int port, final int nr, int forwardedPort, final String retVal)
			throws UnknownHostException, IOException {
		Peer peer = LocalNATUtils.init(ip, port, nr, forwardedPort);
		peer.objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				return retVal;
			}
		});
		return peer;
	}
	
}
