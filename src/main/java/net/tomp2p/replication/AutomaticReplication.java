package net.tomp2p.replication;

import java.util.ArrayList;
import java.util.HashSet;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMapChangeListener;

/**
 * @author Maxat
 *
 */
public class AutomaticReplication implements PeerMapChangeListener {
	
    private HashSet<Number160> neighbourPeers =null;
    private HashSet<Number160> removedNeighbourPeers = null;
    private double reliability;
    
    private ArrayList<Integer> observation = null;
    private ArrayList<Double> ema5_dsf = null;
	private ArrayList<Double> ema5_dsf_d = null;	
	
	/**
	 * Constructor.
	 * 
	 * @param reliability
	 * 				The reliability coefficient that is used to calculate replication factor
	 */
    public AutomaticReplication(double reliability) {
    	this.neighbourPeers = new HashSet<Number160>();
    	this.removedNeighbourPeers = new HashSet<Number160>();
    	this.reliability = reliability;
    	
    	this.observation = new ArrayList<Integer>();
    	this.ema5_dsf = new ArrayList<Double>();
		this.ema5_dsf_d = new ArrayList<Double>();    	
		this.ema5_dsf.add(0.0);
		this.ema5_dsf_d.add(0.0);
    }
    
    /**
     * 
     * @return The number of neighbor peers.
     */
    public int getNeighbourPeersSize() {
    	return neighbourPeers.size();
    }
    
    /**
     * 
     * @return The number of removed neighbor peers.
     */
    public int getRemovedNeighbourPeersSize() {
    	return removedNeighbourPeers.size();
    }
    
    /**
     * In order to track the number of removed neighbor peers every time, the hash set should be emptied 
     * for the next use.
     */
    public void clearRemovedNeighbourPeers() {
    	for(Number160 entry: removedNeighbourPeers)
    		neighbourPeers.remove(entry);
    	removedNeighbourPeers.clear();
    } 
    
    /**
     * @return The replication factor.
     */
    public int getReplicationFactor() {
    	observation.add(getRemovedNeighbourPeersSize());
    	ema5_dsf.add(getEMA_DSF(observation, ema5_dsf, 5));
		ema5_dsf_d.add(getEMA_DSF_D(observation, ema5_dsf.get(ema5_dsf.size()-1), 5));
    	clearRemovedNeighbourPeers();
    	int replicationFactor = calculateReplicationFactor((int)Math.round(ema5_dsf_d.get(ema5_dsf_d.size()-1)));
    	if(observation.size()>5){
			observation.remove(0);
			ema5_dsf.remove(0);
			ema5_dsf_d.remove(0);    		
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
    	int replicationFactor = 0;
    	if(predictedValue>=getNeighbourPeersSize()) {
    		replicationFactor = getNeighbourPeersSize();
    		ok = false;
    	}
    	while(ok){
    		replicationFactor++;
    		double probability = 1;
    		for(int i=0; i<replicationFactor; i++)
    			probability *= (double)(predictedValue-i)/(double)(getNeighbourPeersSize()-i);
    		
    		if((1-probability)>=reliability)
    			ok = false;
    	}
    	return replicationFactor;
    }
    
    /**
     * Calculation of Standard Deviation for a set of values
     * 
     * @param range
     * 			A set of values
     * @param avg
     * 			Average value for the set
     * @return The standard deviation
     */
    private double getStandardDeviation(ArrayList<Integer> range, double avg) {
    	double sum = 0;
    	for(int i=0; i<range.size(); i++)
    		sum += (range.get(i)-avg)*(range.get(i)-avg);
    	if(range.size()==1)
    		return 0.0;
    	sum = sum / (range.size()-1);
    	return Math.sqrt(sum);
    }    
    
	private double getSumOfPredictedVariationSqured(ArrayList<Double> predicted, int k, double predictedMean) {
		double sum = 0;
		for(int i=k; i<predicted.size(); i++)
			sum += (predicted.get(i)-predictedMean)*(predicted.get(i)-predictedMean);
		return sum;
	}    
	private double getSumOfRegressionVariationSquared(ArrayList<Integer> observed, ArrayList<Double> predicted, int k, double b0, double b1, double predictedMean) {
		double sum = 0;
		for(int i=k; i<predicted.size(); i++){
			double regression = b0 + b1*observed.get(i);
			sum += (regression-predictedMean)*(regression-predictedMean);
		}
		return sum;
	}	
	private double getSumOfObservedVariationSquared(ArrayList<Integer> observed, int k, double observedMean) {
		double sum = 0;
		for(int i=k; i<observed.size(); i++) 
			sum += (observed.get(i)-observedMean)*(observed.get(i)-observedMean);
		return sum;
	}	
	private double getSumOfObservedVariationMultipliedPredictedVariation(ArrayList<Integer> observed, ArrayList<Double> predicted, int k, double observedMean, double predictedMean) {
		double sum = 0;
		for(int i=k; i<observed.size(); i++)
			sum += (observed.get(i)-observedMean)*(predicted.get(i)-predictedMean);
		return sum;		
	}    
	private double getPredictedMean(ArrayList<Double> predicted, int k){
		double sum = 0;
		for(int i=k; i<predicted.size(); i++)
			sum += predicted.get(i);
		sum = sum / (predicted.size()-k);
		return sum;		
	}    
	private double getObservedMean(ArrayList<Integer> observed, int k){
		double sum = 0;
		for(int i=k; i<observed.size(); i++)
			sum += observed.get(i);
		sum = sum / (observed.size()-k);
		return sum;		
	}    
	private double getBestSmoothingFactor(ArrayList<Integer> observed, ArrayList<Double> predicted) {
		double min = 1;
		int interval = 1;
		for(int i=0; i<observed.size(); i++){
			double observedMean = getObservedMean(observed, i);
			double predictedMean = getPredictedMean(predicted, i);
			double b1 = getSumOfObservedVariationMultipliedPredictedVariation(observed, predicted, i, observedMean, predictedMean)/getSumOfObservedVariationSquared(observed, i, observedMean);
			double b0 = predictedMean - b1*observedMean;
			double rSquared = getSumOfRegressionVariationSquared(observed, predicted, i, b0, b1, predictedMean)/getSumOfPredictedVariationSqured(predicted, i, predictedMean);
			if(min>=rSquared) {
				min = rSquared;
				interval = observed.size()-i;
			}
		}
		return (double)2/(interval+1);
	}     

	/**
	 * Exponential Moving Average with Dynamic Smoothing Factor
	 * 
	 * @param observed
	 * 			Array that contains recent peer departures numbers.
	 * @param ema_dsf
	 * 			Array that contains recent values of exponential moving averages
	 * @param interval
	 * 			The length of interval that is used to calculate smoothing factor
	 * @return The exponential moving average for a given observation of peer departures.
	 */
	public Double getEMA_DSF(ArrayList<Integer> observed, ArrayList<Double> ema_dsf, int interval) {
    	ArrayList<Integer> range = new ArrayList<Integer>();
		int m=0;
		if(observed.size()>interval) m=observed.size()-interval;
		for(int i=m; i<observed.size(); i++)
			range.add(observed.get(i));    	
		ArrayList<Double> ema_dsf_range = new ArrayList<Double>(); 
		m=0;
		if(ema_dsf.size()>interval) m=ema_dsf.size()-interval;
		for(int i=m; i<ema_dsf.size(); i++)
			ema_dsf_range.add(ema_dsf.get(i));
    	
		double smoothingFactor = getBestSmoothingFactor(range, ema_dsf_range);
		double ema = (observed.get(observed.size()-1)-ema_dsf.get(ema_dsf.size()-1))*smoothingFactor+ema_dsf.get(ema_dsf.size()-1);
		
		return ema;
    }    
	
    /**
     * Exponential Moving Average with Dynamic Smoothing Factor plus Deviation
     * 
     * @param observed
     * 			Array that contains recent peer departures numbers.
     * @param currentEMA_DSF
     * 			Current exponential moving average
     * @param interval
     * 			The length of interval that is used to calculate deviation
     * @return The exponential moving average plus deviation for a given observation of peer departures.
     * 		This value is regarded as a prediction value for the next second.
     */
    public Double getEMA_DSF_D(ArrayList<Integer> observed, double currentEMA_DSF, int interval) {
    	ArrayList<Integer> range = new ArrayList<Integer>();
		int m=0;
		if(observed.size()>interval) m=observed.size()-interval;
		for(int i=m; i<observed.size(); i++)
			range.add(observed.get(i));    	
		
		double dev = getStandardDeviation(range, currentEMA_DSF);
		
		return Math.ceil(currentEMA_DSF+dev);
		//return currentEMA_DSF+dev;
    }    

	@Override
	public void peerInserted(PeerAddress peerAddress) {
		// TODO Auto-generated method stub
		neighbourPeers.add(peerAddress.getPeerId());
	}

	@Override
	public void peerRemoved(PeerAddress peerAddress) {
		// TODO Auto-generated method stub
		removedNeighbourPeers.add(peerAddress.getPeerId());
	}

	@Override
	public void peerUpdated(PeerAddress peerAddress) {
		// TODO Auto-generated method stub
		
	}

}
