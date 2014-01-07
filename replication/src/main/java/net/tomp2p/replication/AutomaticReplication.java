package net.tomp2p.replication;

import java.util.ArrayList;
import java.util.HashSet;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.ReplicationFactor;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.peers.PeerStatatistic;

/**
 * AutomaticReplication class observes the local network conditions and predicts the next departure count 
 * based on previous and current peer churn behaviors. This predicted value and given reliability are used 
 * to calculate the replication factor
 * 
 * @author Maxat Pernebayev
 *
 */
public class AutomaticReplication implements PeerMapChangeListener, ReplicationFactor {
	private PeerMap peerMap;	
    private HashSet<Number160> removedPeers = null;
    private double reliability;
    private static final int minReplicationFactor = 2;
    private static final int maxReplicationFactor = -1;
    private static final int observationLength = 10;
    
    private ArrayList<Integer> observations = null;
    private ArrayList<Double> averages = null;
	
    /**
     * Constructor.
     * 
     * @param reliability
     * 			The reliability
     * @param peerMap
     * 			The map of my neighbors
     */
    public AutomaticReplication(double reliability) {
    	this.removedPeers = new HashSet<Number160>();
    	this.reliability = reliability;
    	
    	this.observations = new ArrayList<Integer>();
    	this.averages = new ArrayList<Double>();
		this.averages.add(0.0);
		
    }
    
    @Override
    public void init(Peer peer) {
        this.peerMap = peer.getPeerBean().peerMap();
        peerMap.addPeerMapChangeListener(this);
    }
    
    
    /**
     * 
     * @return The number of neighbor peers.
     */
    public int getNeighbourPeersSize() {
    	return peerMap.size();
    }
    
    /**
     * 
     * @return The number of removed neighbor peers.
     */
    public int getRemovedPeersSize() {
    	return removedPeers.size();
    }
    
    /**
     * In order to track the number of removed neighbor peers every time, the hash set should be emptied 
     * for the next use.
     */
    public void clearRemovedPeers() {
    	removedPeers.clear();
    } 
    
    /**
     * @return The replication factor.
     */
    public int factor() {
    	observations.add(getRemovedPeersSize());
    	clearRemovedPeers();
    	double average = getAverage(observations, averages); 
    	averages.add(average);
    	double predictedValue = getPredictedValue(observations, average);
    	int replicationFactor = calculateReplicationFactor((int)Math.round(predictedValue));
    	if(observations.size()>=observationLength){
			observations.remove(0);
			averages.remove(0);
    	}
    	return replicationFactor;
    }    
    
    /**
     * The replication factor is calculated based on reliability and predicted value. 
     * 
     * @param predictedValue
     * 				Predicted value from prediction model
     * @return The replication factor
     */
    public int calculateReplicationFactor(int predictedValue) {
    	boolean ok = true;
    	int replicationFactor = minReplicationFactor-1;
    	if(predictedValue>=getNeighbourPeersSize()) {
    		replicationFactor = getNeighbourPeersSize();
    		ok = false;
    	}
    	while(ok){
    		replicationFactor++;
    		double probability = 1;
    		for(int i=0; i<replicationFactor; i++)
    			probability *= (double)(predictedValue-i)/(double)(getNeighbourPeersSize()-i);
    		
    		if((1-probability)>=reliability || replicationFactor==maxReplicationFactor)
    			ok = false;
    	}
    	return replicationFactor;
    }

	/**
	 * @param x
	 * 		The array of independent values, i.e. observations
	 * @param k
	 * 		The start index of subset
	 * @return	The mean of independent values
	 */
	public double getXMean(ArrayList<Integer> x, int k){
		double sum = 0;
		for(int i=k; i<x.size(); i++)
			sum += x.get(i);
		return sum / (x.size()-k);
	}

	/**
	 * @param y
	 * 		The array of dependent values, i.e. averages
	 * @param k
	 * 		The start index of subset
	 * @return	The mean of dependent values
	 */
	public double getYMean(ArrayList<Double> y, int k){
		double sum = 0;
		for(int i=k; i<y.size(); i++)
			sum += y.get(i);
		return sum / (y.size()-k);
	}    

	/**
	 * @param x
 * 			The array of independent values, i.e. observations
	 * @param y
	 * 		The array of dependent values, i.e. averages
	 * @param k
	 * 		The start index of subset
	 * @param xMean
	 * 		The mean of independent values
	 * @param yMean
	 * 		The mean of dependent values
	 * @return	The sum of multiplication of the variations of independent and dependent values
	 */
	public double getSumOfXVariationMultipliedYVariation(ArrayList<Integer> x, ArrayList<Double> y, int k, double xMean, double yMean) {
		double sum = 0;
		for(int i=k; i<x.size(); i++)
			sum += (x.get(i)-xMean)*(y.get(i)-yMean);
		return sum;		
	}    

	/**
	 * @param x
	 * 		The array of independent values, i.e. observations
	 * @param k
	 * 		The start index of subset
	 * @param xMean
	 * 		The mean of independent values
	 * @return	The sum of squared variation of independent values
	 */
	public double getSumOfXVariationSquared(ArrayList<Integer> x, int k, double xMean) {
		double sum = 0;
		for(int i=k; i<x.size(); i++) 
			sum += (x.get(i)-xMean)*(x.get(i)-xMean);
		return sum;
	}	

	/**
	 * @param x
	 * 		The array of independent values, i.e. observations
	 * @param k
	 * 		The start index of subset
	 * @param b0
	 * 		y-intercept of regression line
	 * @param b1
	 * 		The slope of regression line
	 * @param yMean
	 * 		The mean of dependent values
	 * @return	The sum of squared variation of regression
	 */
	public double getSumOfRegressionVariationSquared(ArrayList<Integer> x, int k, double b0, double b1, double yMean) {
		double sum = 0;
		for(int i=k; i<x.size(); i++){
			double regression = b0 + b1*x.get(i);
			sum += (regression-yMean)*(regression-yMean);
		}
		return sum;
	}	
	
	/**
	 * @param y
	 * 		The array of dependent values, i.e. averages
	 * @param k
	 * 		The start index of subset
	 * @param yMean
	 * 		The mean of dependent values
	 * @return	The sum of squared variation of dependent values
	 */
	public double getSumOfYVariationSquared(ArrayList<Double> y, int k, double yMean) {
		double sum = 0;
		for(int i=k; i<y.size(); i++)
			sum += (y.get(i)-yMean)*(y.get(i)-yMean);
		return sum;
	}    
	
	/**
	 * @param x
	 * 		The array of independent values, i.e. observations
	 * @param y
	 * 		The array of dependent values, i.e. averages
	 * @param initialInterval
	 * 		The initial interval
	 * @return	The best smoothing factor
	 */
	public double getBestSmoothingFactor(ArrayList<Integer> x, ArrayList<Double> y, int initialInterval) {
		double max = 0;
		double rSquared=0;
		int interval = initialInterval;
		for(int i=0; i<x.size()-2; i++){
			double xMean = getXMean(x, i);
			double yMean = getYMean(y, i);
			double b1 = getSumOfXVariationMultipliedYVariation(x, y, i, xMean, yMean)/getSumOfXVariationSquared(x, i, xMean);
			double b0 = yMean - b1*xMean;
			rSquared = getSumOfRegressionVariationSquared(x, i, b0, b1, yMean)/getSumOfYVariationSquared(y, i, yMean);
			if(max<=rSquared){
				max = rSquared;
				interval = x.size()-i;
			}
		}
		return 2.0/(interval+1);
	}     

	/**
	 * Exponential Moving Average with Dynamic Smoothing Factor
	 * 
	 * @param observations
	 * 			Array that contains peer departures numbers.
	 * @param averages
	 * 			Array that contains values of exponential moving averages
	 * @param interval
	 * 			The length of interval that is used to calculate smoothing factor
	 * @return The exponential moving average for given observations of peer departures.
	 */
	public Double getAverage(ArrayList<Integer> observations, ArrayList<Double> averages) {
		double smoothingFactor = getBestSmoothingFactor(observations, averages, observations.size());
		int lastObservation = observations.get(observations.size()-1);
		double lastAverage = averages.get(averages.size()-1);
		
		return (lastObservation-lastAverage)*smoothingFactor+lastAverage;
    }    

    /**
     * Calculation of Standard Deviation for a set of values
     * 
     * @param range
     * 			A set of values
     * @param currentAverage
     * 			Average value for the set
     * @return The standard deviation
     */
    public double getStandardDeviation(ArrayList<Integer> range, double currentAverage) {
    	double sum = 0;
    	for(int i=0; i<range.size(); i++)
    		sum += (range.get(i)-currentAverage)*(range.get(i)-currentAverage);
    	if(range.size()==1)
    		return 0.0;
    	sum = sum / (range.size()-1);
    	return Math.sqrt(sum);
    }	
	
    /**
     * Exponential Moving Average with Dynamic Smoothing Factor plus Deviation
     * 
     * @param observations
     * 			Array that contains recent peer departures numbers.
     * @param currentAverage
     * 			Current exponential moving average
     * @param interval
     * 			The length of interval that is used to calculate deviation
     * @return The exponential moving average plus deviation for a given observation of peer departures.
     * 		This value is regarded as a prediction value for the next second.
     */
    public Double getPredictedValue(ArrayList<Integer> observations, double currentAverage/*, int interval*/) {
    	double deviation = getStandardDeviation(observations, currentAverage);
		
		return Math.ceil(currentAverage+deviation);
    }    

	@Override
	public void peerInserted(PeerAddress peerAddress, boolean verified) {
	}

	@Override
	public void peerRemoved(PeerAddress peerAddress,
			PeerStatatistic storedPeerAddress) {
		removedPeers.add(peerAddress.getPeerId());
	}

	@Override
	public void peerUpdated(PeerAddress peerAddress,
			PeerStatatistic storedPeerAddress) {
	}
}
