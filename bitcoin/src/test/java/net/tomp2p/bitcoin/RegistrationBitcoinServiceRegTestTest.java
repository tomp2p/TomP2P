package net.tomp2p.bitcoin;

import org.bitcoinj.params.RegTestParams;
import org.bitcoinj.params.TestNet3Params;
import org.junit.BeforeClass;

import java.io.File;
import java.net.URL;

/**
 * Created by alex on 16/06/15.
 */
public class RegistrationBitcoinServiceRegTestTest extends RegistrationBitcoinServiceTest {

    @BeforeClass
    public static void setUp() throws Exception {
        URL resource = RegistrationServiceRegTestTest.class.getResource("/registrations/regtest");
        File dir = new File(resource.toURI());
        RegistrationStorage registrationStorage = new RegistrationStorageFile(dir, "registration.storage");
        registrationService = new RegistrationBitcoinService(registrationStorage, RegTestParams.get(), new File("."), "tomP2P-bitcoin-regtest");
//        registrationService = new RegistrationService(registrationStorage, RegTestParams.get(), new java.io.File("."), "tomP2P-bitcoin-regtest");
        registrationService.start();
        //load registrations form file
        validRegistrations = Utils.readRegistrations(dir, "validRegistrations.array").subList(0,99);
    }
}
