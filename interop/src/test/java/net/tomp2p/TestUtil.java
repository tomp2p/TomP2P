package net.tomp2p;

import net.tomp2p.connection.ChannelServerConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Ports;
import net.tomp2p.p2p.MaintenanceTask;
import net.tomp2p.p2p.PeerBuilder;

public class TestUtil {

	public static ChannelServerConfiguration createInfiniteTimeoutChannelServerConfiguration(int portUdp, int portTcp)
    {
        return PeerBuilder.createDefaultChannelServerConfiguration()
            .idleTCPSeconds(0)
            .idleUDPSeconds(0)
            .connectionTimeoutTCPMillis(0)
            .ports(new Ports(portTcp, portUdp));
    }

    public static DefaultConnectionConfiguration CreateInfiniteConfiguration()
    {
        return new DefaultConnectionConfiguration()
            .connectionTimeoutTCPMillis(Integer.MAX_VALUE)
            .idleUDPSeconds(Integer.MAX_VALUE)
            .idleTCPSeconds(Integer.MAX_VALUE);
    }

    public static MaintenanceTask CreateInfiniteIntervalMaintenanceTask()
    {
        return new MaintenanceTask()
            .intervalMillis(Integer.MAX_VALUE);
    }
}
