package net.tomp2p;

public final class Arguments {

	private final String bmArg;
	private final int repetitions;
	private final String resultsPath;
	private final int warmupSec;
	private final String suffix;
	
	public Arguments(String bmArg, int repetitions, String resultsPath, int warmupSec, String suffix) {
		this.bmArg = bmArg;
		this.repetitions = repetitions;
		this.resultsPath = resultsPath;
		this.warmupSec = warmupSec;
		this.suffix = suffix;
	}

	public String getBmArg() {
		return bmArg;
	}

	public int getRepetitions() {
		return repetitions;
	}

	public String getResultsPath() {
		return resultsPath;
	}

	public int getWarmupSec() {
		return warmupSec;
	}

	public String getSuffix() {
		return suffix;
	}
}
