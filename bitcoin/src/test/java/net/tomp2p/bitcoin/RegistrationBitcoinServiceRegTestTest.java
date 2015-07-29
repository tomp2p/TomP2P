package net.tomp2p.bitcoin;

import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.RegTestParams;

/**
 * Unit test for {@link RegistrationBitcoinService}
 * Uses RegTest instead of TestNet
 * This TestClass requires bitcoind v0.10.0 to be installed and running.
 *
 * @author Alexander Mülli
 *
 */
public class RegistrationBitcoinServiceRegTestTest extends RegistrationBitcoinServiceTest {

    private static final NetworkParameters params = RegTestParams.get();

}
