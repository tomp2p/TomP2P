package net.tomp2p.holep;

public class HolePTestDriver {

	public static void main(String[] args) throws Exception {

		HolePTestApp testApp = new HolePTestApp();
		if (!checkArguments(args)) {
			testApp.startServer();
		} else {
			testApp.startClient();
		}
		testApp.setObjectDataReply();
		testApp.runTextInterface();
	}

	private static boolean checkArguments(String[] args) {
		if (args.length > 0) {
			return true;
		} else {
			return false;
		}
	}
}
