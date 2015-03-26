package net.tomp2p;

public final class Arguments {

	private final String bmArg;
	private final String type;
	private final int nrWarmups;
	private final int nrRepetitions;
	private final String resultsDir;
	private final String suffix;
	
	public Arguments(String bmArg, String type, int nrWarmups, int nrRepetitions, String resultsDir, String suffix) {
		this.bmArg = bmArg;
		this.type = type;
		this.nrWarmups = nrWarmups;
		this.nrRepetitions = nrRepetitions;
		this.resultsDir = resultsDir;
		this.suffix = suffix;
	}

	public String getBmArg() {
		return bmArg;
	}

	public String getType() {
		return type;
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
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Arguments [bmArg = ")
        .append(bmArg)
        .append(", type = ").append(type)
        .append(", nrWarmups = ").append(nrWarmups)
        .append(", nrRepetitions = ").append(nrRepetitions)
        .append(", resultsDir = ").append(resultsDir)
        .append(", suffix = ").append(suffix);
		return sb.toString();
	}
}
