package net.tomp2p.peers;

public class LocalMapConf {

	private int localMapTimout = Integer.MAX_VALUE;
	private int localMapSize = 1000;
	private int localMapRevTimeout = Integer.MAX_VALUE;
	private int localMapRevSize = 1000;
	private int offlineMapTimout = 60;
	private int offlineMapSize = 1000;
	private int[] intervalSeconds = new int[] { 2, 4, 8, 16, 32, 64 };

	public int localMapTimout() {
		return localMapTimout;
	}

	public LocalMapConf localMapTimout(int localMapTimout) {
		this.localMapTimout = localMapTimout;
		return this;
	}

	public int localMapSize() {
		return localMapSize;
	}

	public LocalMapConf localMapSize(int localMapSize) {
		this.localMapSize = localMapSize;
		return this;
	}

	public int localMapRevTimeout() {
		return localMapRevTimeout;
	}

	public LocalMapConf localMapRevTimeout(int localMapRevTimeout) {
		this.localMapRevTimeout = localMapRevTimeout;
		return this;
	}

	public int localMapRevSize() {
		return localMapRevSize;
	}

	public LocalMapConf localMapRevSize(int localMapRevSize) {
		this.localMapRevSize = localMapRevSize;
		return this;
	}

	public int offlineMapTimout() {
		return offlineMapTimout;
	}

	public LocalMapConf offlineMapTimout(int offlineMapTimout) {
		this.offlineMapTimout = offlineMapTimout;
		return this;
	}

	public int offlineMapSize() {
		return offlineMapSize;
	}

	public LocalMapConf offlineMapSize(int offlineMapSize) {
		this.offlineMapSize = offlineMapSize;
		return this;
	}

	public int[] intervalSeconds() {
		return intervalSeconds;
	}

	public LocalMapConf intervalSeconds(int[] intervalSeconds) {
		this.intervalSeconds = intervalSeconds;
		return this;
	}

}
