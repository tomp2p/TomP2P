package net.tomp2p;

public final class Arguments {

	private final String bmArg;
	private final int nrRepetitions;
	private final int nrWarmups;
	private final String resultsDir;
	private final String suffix;
	
	public Arguments(String bmArg, int nrWarmups, int nrRepetitions, String resultsDir, String suffix) {
		this.bmArg = bmArg;
		this.nrRepetitions = nrRepetitions;
		this.nrWarmups = nrWarmups;
		this.resultsDir = resultsDir;
		this.suffix = suffix;
	}

	public String getBmArg() {
		return bmArg;
	}

	public int getNrWarmups() {
		return nrWarmups;
	}
	
	public int getNrRepetitions() {
		return nrRepetitions;
	}

	public String getResultsDir() {
		return resultsDir;
	}

	public String getSuffix() {
		return suffix;
	}
}
