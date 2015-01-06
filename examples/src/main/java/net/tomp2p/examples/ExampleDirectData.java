package net.tomp2p.examples;

import java.io.IOException;
import java.util.Arrays;

import net.tomp2p.dht.FutureSend;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class ExampleDirectData {
	static int p1Counter = 0;
	static int p2Counter = 0;

	public static void main(String[] args) throws IOException {
		final Number160 idP1 = Number160.createHash("p1");
		final Number160 idP2 = Number160.createHash("p2");
		PeerDHT p1 = new PeerBuilderDHT(new PeerBuilder(idP1).ports(1234).start()).start();
		PeerDHT p2 = new PeerBuilderDHT(new PeerBuilder(idP2).ports(1235).start()).start();
		BootstrapBuilder b = p2.peer().bootstrap();
		b.bootstrapTo(Arrays.asList(new PeerAddress(idP1, "localhost", 1234, 1234)));
		b.start().awaitUninterruptibly();

		p1.peer().objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (sender.peerId().equals(idP2)) {
					int val = (Integer) request;
					System.err.println(String.format("P1 received: %d", val));
					if (val != p1Counter) {
						System.err.println("something went wrong");
						throw new Exception("");
					}

					p1Counter++;

					return p1Counter - 1;
				}

				return null;
			}
		});

		p2.peer().objectDataReply(new ObjectDataReply() {
			@Override
			public Object reply(PeerAddress sender, Object request) throws Exception {
				if (sender.peerId().equals(idP1)) {
					int val = (Integer) request;
					System.err.println(String.format("P2 received: %d", val));
					if (val != p2Counter) {
						System.err.println("something went wrong");
						throw new Exception("");
					}

					p2Counter++;

					return p2Counter - 1;
				}

				return null;
			}
		});

		// p2 -> p1 -- Works
		p2SendNext(p2, idP1);

		// Uncomment for opposite direction p1 -> p2 -- WILL NOT WORK
		// p1SendNext(p1, idP2);

	}

	static void p1SendNext(final PeerDHT p, final Number160 idP2) {
		p1Counter++;
		p.send(idP2).object(p1Counter - 1).requestP2PConfiguration(new RequestP2PConfiguration(1, 5, 0)).start()
		        .addListener(new BaseFutureListener<FutureSend>() {

			        @Override
			        public void operationComplete(FutureSend future) throws Exception {

				        Object[] values = future.rawDirectData2().values().toArray();
				        if (values.length != 1) {
					        throw new Exception(String.format("Invalid length %d", values.length));
				        }

				        if (!((Integer) values[0] == p1Counter - 1)) {
					        throw new Exception("Invalid value");
				        }

				        System.err.println(String.format("P1 Received: %d", values[0]));
				        p1SendNext(p, idP2);
			        }

			        @Override
			        public void exceptionCaught(Throwable t) throws Exception {
				        System.err.println(t.toString());

			        }
		        });

	}

	static void p2SendNext(final PeerDHT p, final Number160 idP1) {
		p2Counter++;
		p.send(idP1).object(p2Counter - 1).requestP2PConfiguration(new RequestP2PConfiguration(1, 5, 0)).start()
		        .addListener(new BaseFutureListener<FutureSend>() {

			        @Override
			        public void operationComplete(FutureSend future) throws Exception {

				        Object[] values = future.rawDirectData2().values().toArray();
				        if (values.length == 1)
					        System.err.println(String.format("P2 received: %d", values[0]));
				        else if (values.length != 1) {
					        throw new Exception("Invalid length");
				        }

				        if (!((Integer) values[0] == p2Counter - 1)) {
					        throw new Exception("Invalid value");
				        }

				        // System.err.println(String.format("P2 Received: %d",
						// values[0]));
				        p2SendNext(p, idP1);
			        }

			        @Override
			        public void exceptionCaught(Throwable t) throws Exception {
				        System.err.println(t.toString());

			        }
		        });

	}
}
