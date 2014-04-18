package net.tomp2p.synchronization;

public class SynchronizationStatistics {
    
    private int dataCopy;
    private int dataCopyCount;
    private int diffCount;
    private int dataNotCopied;

    public SynchronizationStatistics dataCopy(int dataCopy) {
        this.dataCopy = dataCopy;
        return this;
    }
    
    public int dataCopy() {
        return dataCopy;
    }

    public SynchronizationStatistics dataCopyCount(int dataCopyCount) {
        this.dataCopyCount = dataCopyCount;
        return this;  
    }
    
    public int dataCopyCount() {
        return dataCopyCount;
    }

    public SynchronizationStatistics diffCount(int diffCount) {
        this.diffCount = diffCount;
        return this;
        
    }
    
    public int diffCount() {
        return diffCount;
    }

    public SynchronizationStatistics dataNotCopied(int dataNotCopied) {
        this.dataNotCopied = dataNotCopied;
        return this;
    }
    
    public int dataNotCopied() {
        return dataNotCopied;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("rsync stats:");
        sb.append("data copied: ").append(dataCopy).append("(").append(dataCopyCount).append(")");
        sb.append(",data not copied: ").append(dataNotCopied).append("(").append(diffCount).append(")");
        sb.append(",ratio: ").append(dataCopy/(double)(dataNotCopied+dataCopy));
        return sb.toString();
    }
}
