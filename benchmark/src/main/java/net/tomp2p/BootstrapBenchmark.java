package net.tomp2p;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.utils.InteropRandom;

public class BootstrapBenchmark extends BaseBenchmark {

	private static final InteropRandom RND = new InteropRandom(42);
	private final List<FutureBootstrap> futures = new ArrayList<FutureBootstrap>(NETWORK_SIZE * NETWORK_SIZE);
	private Peer[] network;

	@Override
	protected void setup() throws IOException {
		network = setupNetwork(RND);
	}

	@Override
	protected void execute() {
		for (int i = 0; i < network.length; i++)
        {
            for (int j = 0; j < network.length; j++)
            {
                futures.add(network[i].bootstrap().peerAddress(network[j].peerAddress()).start());
            }
        }
		for (FutureBootstrap future : futures) {
			future.awaitUninterruptibly();
		}
	}
}
