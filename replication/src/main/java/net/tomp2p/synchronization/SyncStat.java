package net.tomp2p.synchronization;

public class SyncStat {
    
    final private int dataCopy;
    final private int dataOrig;

    public SyncStat(int dataCopy, int dataOrig) {
	    this.dataCopy = dataCopy;
	    this.dataOrig = dataOrig;
    }
    
    public int dataCopy() {
        return dataCopy;
    }
    
    public int dataOrig() {
        return dataOrig;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("sync stats:");
        sb.append("send=").append(dataCopy).append("(orig=").append(dataOrig).append(")");
        sb.append(",ratio: ").append(dataOrig/(double)dataCopy);
        return sb.toString();
    }
}
