package net.tomp2p.holep.manual;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.Bindings;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

public class LocalNATUtils {
	private static final String TAG = "##BASE64##:";

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
		builder.redirectError(ProcessBuilder.Redirect.INHERIT);
		Process process = builder.start();
		process.waitFor();
		return process.exitValue();
	}

	public static RemotePeer executePeer(Class<?> klass, String nr, final Command... cmd)
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
		cmds[9] = nr;
		for (int i = 10; i < cmds.length; i++) {
			cmds[i] = toString(cmd[i - 10]);
		}

		ProcessBuilder builder = new ProcessBuilder(cmds);
		builder.redirectError(ProcessBuilder.Redirect.INHERIT);
		final Process process = builder.start();
		System.err.println("executed.");
		final CountDownLatch cl = new CountDownLatch(cmd.length);
		final AtomicReferenceArray<Object> results = new AtomicReferenceArray<Object>(cmd.length);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < cmd.length; i++) {
						boolean done = false;
						while (!done) {
							String line = read(process.getInputStream()).trim();
							if (line.startsWith(TAG)) {
								line = line.substring(TAG.length());
								System.out.println("from : " + line);
								Object o = fromString(line);
								results.set(i, o);
								done = true;
							} else {
								System.out.println("from remote: " + line);
							}
						}
						cl.countDown();
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}

			}
		}).start();
		System.err.println("peer started");
		return new RemotePeer(process, cl, cmd, results);
	}

	public static String read(InputStream is) throws IOException {
		// Read out dir output
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		return br.readLine();
	}

	public static int killPeer(Process process) throws InterruptedException,
			IOException {
		process.destroy();
		process.getErrorStream().close();
		process.getInputStream().close();
		process.getOutputStream().close();
		return process.waitFor();
	}

	/**
	 * As set in: tomp2p/nat/src/test/resources/nat-net.sh
	 */
	public static Peer createRealNode(Number160 relayPeerId, String iface)
			throws Exception {
		// relay
		Bindings b2 = new Bindings();
		b2.addInterface(iface);
		return new PeerBuilder(relayPeerId).ports(5002).bindings(b2).start();
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
		oos.writeObject(o);
		oos.close();
		return Base64.getEncoder().encodeToString(baos.toByteArray());
	}

	public static Command[] toObjects(String[] args) throws ClassNotFoundException, IOException {
		Command[] cmd = new Command[args.length-1];
		for(int i=1;i<args.length;i++) {
			System.err.println("["+args[i]+"]");
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
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
}
