package net.tomp2p.holep;

public class HolePTestDriver {

	public static void main(String[] args) throws Exception {

		HolePTestApp testApp = new HolePTestApp();
		if (!checkArguments(args)) {
			testApp.startMasterPeer();
		} else {
			testApp.startPeer(args);
		}
		testApp.setObjectDataReply();
		testApp.runTextInterface();
	}

	private static boolean checkArguments(String[] args) {
		if (args.length > 1) {
			return true;
		} else if (args.length == 1) {
			throw new IllegalArgumentException("The Application can't start with the given arguments. The arguments have to be like this: \n args[0] = 192.168.2.xxx \n args[1] = \"id\n");
		} else {
			return false;
		}
	}
}
