package net.tomp2p;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.futures.FutureBootstrap;

public class BootstrapProfiler extends Profiler {

	private final int NETWORK_SIZE = 5;
	private final List<FutureBootstrap> futures = new ArrayList<FutureBootstrap>(NETWORK_SIZE * NETWORK_SIZE);

	@Override
	protected void setup(Arguments args) throws IOException {
		Network = BenchmarkUtil.createNodes(NETWORK_SIZE, Rnd, 9099, false, false);
	}

	@Override
	protected void shutdown() throws Exception {
		if (Network != null && Network[0] != null) {
			Network[0].shutdown().awaitUninterruptibly();
		}
	}
	
	@Override
	protected void execute() {
		for (int i = 0; i < Network.length; i++)
        {
            for (int j = 0; j < Network.length; j++)
            {
                futures.add(Network[i].bootstrap().peerAddress(Network[j].peerAddress()).start());
            }
        }
		for (FutureBootstrap future : futures) {
			future.awaitUninterruptibly();
		}
	}
}
