package net.tomp2p.p2p.config;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.p2p.VotingSchemeDHT;
import net.tomp2p.p2p.VotingSchemeTracker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.ShortString;

public class Configurations
{
	public final static Number160 DEFAULT_DOMAIN = new ShortString("P2P domain").toNumber160();
	public final static Number160 DEFAULT_TRACKER_DOMAIN=new ShortString("Tracker domain").toNumber160();

	public static ConfigurationStore defaultStoreConfiguration()
	{
		ConfigurationStore config = new ConfigurationStore();
		config.setRequestP2PConfiguration(new RequestP2PConfiguration(3, 5, 3));
		config.setRoutingConfiguration(new RoutingConfiguration(5, 10, 2));
		config.setDomain(DEFAULT_DOMAIN);
		config.setContentKey(Number160.ZERO);
		config.setStoreIfAbsent(false);
		config.setProtectDomain(false);
		config.setSignMessage(false);
		config.setRefreshSeconds(0);
		return config;
	}

	public static ConfigurationGet defaultGetConfiguration()
	{
		ConfigurationGet config = new ConfigurationGet();
		config.setRequestP2PConfiguration(new RequestP2PConfiguration(1, 5, 2));
		config.setRoutingConfiguration(new RoutingConfiguration(3, 5, 10, 2));
		config.setDomain(DEFAULT_DOMAIN);
		config.setContentKey(Number160.ZERO);
		config.setEvaluationScheme(new VotingSchemeDHT());
		//set this key to received only Data from signed by this public key
		config.setPublicKey(null);
		config.setSignMessage(false);
		return config;
	}

	public static ConfigurationRemove defaultRemoveConfiguration()
	{
		ConfigurationRemove config = new ConfigurationRemove();
		config.setRequestP2PConfiguration(new RequestP2PConfiguration(3, 5, 3));
		config.setRoutingConfiguration(new RoutingConfiguration(3, 5, 10, 2));
		config.setDomain(DEFAULT_DOMAIN);
		config.setContentKey(Number160.ZERO);
		config.setReturnResults(false);
		config.setSignMessage(false);
		config.setRepetitions(0);
		config.setRefreshSeconds(0);
		return config;
	}
	
	public static ConfigurationDirect defaultConfigurationDirect()
	{
		ConfigurationDirect config = new ConfigurationDirect();
		config.setRequestP2PConfiguration(new RequestP2PConfiguration(3, 5, 3));
		config.setRoutingConfiguration(new RoutingConfiguration(5, 10, 2));
		config.setSignMessage(false);
		config.setRefreshSeconds(0);
		config.setCancelOnFinish(false);
		config.setRepetitions(0);
		return config;
	}

	public static ConfigurationTrackerGet defaultTrackerGetConfiguration()
	{
		ConfigurationTrackerGet config= new ConfigurationTrackerGet();
		config.setTrackerConfiguration(new TrackerConfiguration(5, 2, 4, 30));
		config.setRoutingConfiguration(new RoutingConfiguration(4, 5, 10, 2));
		config.setDomain(DEFAULT_TRACKER_DOMAIN);
		config.setEvaluationScheme(new VotingSchemeTracker());
		config.setExpectAttachement(false);
		config.setSignMessage(false);
		config.setUseSecondaryTrackers(false);
		return config;
	}

	public static ConfigurationTrackerStore defaultTrackerStoreConfiguration()
	{
		ConfigurationTrackerStore config= new ConfigurationTrackerStore();
		config.setTrackerConfiguration(new TrackerConfiguration(5, 2, 5, 0));
		config.setRoutingConfiguration(new RoutingConfiguration(4, 5, 10, 2));
		config.setDomain(DEFAULT_TRACKER_DOMAIN);
		config.setAttachement(null);
		config.setSignMessage(false);
		return config;
	}
}
