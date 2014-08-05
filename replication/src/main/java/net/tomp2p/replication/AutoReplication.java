package net.tomp2p.replication;

import java.util.ArrayList;
import java.util.HashSet;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.peers.PeerStatatistic;

/**
 * AutomaticReplication class observes the local network conditions and predicts
 * the next departure count based on previous and current peer churn behaviors.
 * This predicted value and given reliability are used to calculate the
 * replication factor
 * 
 * @author Maxat Pernebayev
 * @author Thomas Bocek
 * 
 */
public class AutoReplication implements PeerMapChangeListener, ReplicationFactor {
	private final HashSet<Number160> removedPeers = new HashSet<Number160>();
	private final ArrayList<Integer> observations = new ArrayList<Integer>();
	private final ArrayList<Double> emas = new ArrayList<Double>();
	private final PeerMap peerMap;

	private double reliability;
	private int minReplicationFactor = 2;
	private int maxReplicationFactor = 100;
	private int observationLength = 10;

	/**
	 * Constructor.
	 * 
	 * @param reliability
	 *            The reliability
	 * @param peerMap
	 *            The map of my neighbors
	 */
	public AutoReplication(Peer peer) {
		this.emas.add(0.0);
		this.peerMap = peer.peerBean().peerMap();
		
	}
	
	public AutoReplication start() {
		peerMap.addPeerMapChangeListener(this);
		return this;
	}
	
	public AutoReplication shutdown() {
		peerMap.removePeerMapChangeListener(this);
		return this;
	}
	
	@Override
	public void peerInserted(PeerAddress peerAddress, boolean verified) {}
	
	@Override
	public void peerUpdated(PeerAddress peerAddress, PeerStatatistic storedPeerAddress) {}

	@Override
	public void peerRemoved(PeerAddress peerAddress, PeerStatatistic storedPeerAddress) {
		synchronized (removedPeers) {
			removedPeers.add(peerAddress.peerId());
		}
	}

	public AutoReplication reliability(double reliability) {
		this.reliability = reliability;
		return this;
	}

	public double reliability() {
		return reliability;
	}

	public AutoReplication minReplicationFactor(int minReplicationFactor) {
		this.minReplicationFactor = minReplicationFactor;
		return this;
	}

	public int minReplicationFactor() {
		return minReplicationFactor;
	}

	public AutoReplication maxReplicationFactor(int maxReplicationFactor) {
		this.maxReplicationFactor = maxReplicationFactor;
		return this;
	}

	public int maxReplicationFactor() {
		return maxReplicationFactor;
	}

	public AutoReplication observationLength(int observationLength) {
		this.observationLength = observationLength;
		return this;
	}

	public int observationLength() {
		return observationLength;
	}

	

	/**
	 * 
	 * @return The number of neighbor peers.
	 */
	public int peerMapSize() {
		return peerMap.size();
	}

	/**
	 * @return The replication factor.
	 */
	public int replicationFactor() {
		final int removedPeerSize;
		synchronized (removedPeers) {
			removedPeerSize = removedPeers.size();
			removedPeers.clear();
		}

		observations.add(removedPeerSize);
		double average = ema(observations, emas);
		emas.add(average);
		int predictedValue = predictedValue(observations, average);

		int replicationFactor = replicationFactor2(predictedValue, peerMapSize(), reliability, minReplicationFactor,
		        maxReplicationFactor);

		if (observations.size() >= observationLength) {
			observations.remove(0);
			emas.remove(0);
		}
		return replicationFactor;
	}

	/**
	 * Calculate the replication factor given the reliability within a churn
	 * interval and the (predicted) number of peers that left the system.
	 * 
	 * @param m
	 *            (predicted) number of peers that left the system
	 * @param n
	 *            number of all peers
	 * @param r
	 *            reliability
	 * @param minReplicationFactor
	 *            the minimal replication factor, not considering the
	 *            reliability
	 * @param maxReplicationFactor
	 *            the maximal replication factor, not considering the
	 *            reliability
	 * 
	 * @return the replication factor for the desired reliability rate
	 */

	public static int replicationFactor(int m, int n, double r, int minReplicationFactor, int maxReplicationFactor) {
		// f = replication factor
		for (int f = minReplicationFactor; f < maxReplicationFactor; f++) {
			// probability that we hit exactly all the f peers where we have our
			// replicas.
			// p1 = 1/(n choose f)
			//
			// probability that we hit exactly all the f peers that went offline
			// p2 = 1/(m choose f)
			//
			// probability that we hit all replicas with the peers that went
			// offline / probability of f peers being part of the departing m
			// peers
			// p = p1/p2
			double p = choose(m, f) / choose(n, f);
			// 1 − p gives the probability of f peers not being within predicted
			// departing m peers. As automatic replication mechanism is supposed
			// to provide at least reliability r,
			// 1 − p should be more than or equal to r
			//
			// 1-p >= r
			if (1 - p >= r) {
				return f;
			}
		}
		return maxReplicationFactor;
	}

	/**
	 * nCR (ohne Zurücklegen, ohne Beachtung der Reihenfolge)
	 * 
	 * http://stackoverflow.com/questions/1678690/what-is-a-good-way-to-
	 * implement-choose-notation-in-java
	 * 
	 * @param n
	 * @param k
	 * @return
	 */
	public static double choose(int n, int k) {
		if (k < 0 || k > n)
			return 0;
		if (k > n / 2) {
			// choose(n,k) == choose(n,n-k),
			// so this could save a little effort
			k = n - k;
		}

		double answer = 1.0;
		for (int i = 1; i <= k; i++) {
			answer *= (n + 1 - i);
			answer /= i;
		}
		return answer;
	}

	/**
	 * Calculate the replication factor given the reliability within a churn
	 * interval and the (predicted) number of peers that left the system.
	 * 
	 * Faster than replicationFactor as we only use one for loop.
	 * 
	 * @param m
	 *            (predicted) number of peers that left the system
	 * @param n
	 *            number of all peers
	 * @param r
	 *            reliability
	 * @param minReplicationFactor
	 *            the minimal replication factor, not considering the
	 *            reliability
	 * @param maxReplicationFactor
	 *            the maximal replication factor, not considering the
	 *            reliability
	 * @return the replication factor for the desired reliability rate
	 */
	public static int replicationFactor2(int m, int n, double r, int minReplicationFactor, int maxReplicationFactor) {
		// f = replication factor
		for (int f = minReplicationFactor; f < maxReplicationFactor; f++) {
			double p = 1.0;
			for (int i = 0; i < f; i++) {
				p = p * (m - i) / (n - i);
			}
			// 1 − p gives the probability of f peers not being within predicted
			// departing m peers. As automatic replication mechanism is supposed
			// to provide at least reliability r,
			// 1 − p should be more than or equal to r
			//
			// 1-p >= r
			if (1 - p >= r) {
				return f;
			}
		}
		return maxReplicationFactor;
	}

	/**
	 * The interval length which results in maximum value of R squared is
	 * considered to be the best solution for the smoothing factor.
	 * 
	 * @param x
	 *            The array of independent values, i.e. observations
	 * @param y
	 *            The array of dependent values, i.e. averages
	 * @return Smoothing factor alpha, which is based on the interval length
	 *         which results in maximum value of R squared.
	 */
	public static double bestSmoothingFactor(ArrayList<Integer> x, ArrayList<Double> y) {
		final int size = x.size();
		double max = 0;
		int interval = size;
		//make sure we have more than 2 points, otherwise we will have a perfect linear regression
		for (int i = size; i >= 3; i--) {
			double r2 = linearRegression(x, y, i);
			if (r2 >= max) {
				max = r2;
				interval = i;
			}
		}
		return 2.0 / (interval + 1);
	}

	/**
	 * LinearRegression, adapted from:
	 * http://introcs.cs.princeton.edu/java/97data/LinearRegression.java.html
	 * 
	 * @param x
	 *            The array of independent values, i.e. observations
	 * @param y
	 *            The array of dependent values, i.e. averages
	 * @param interval
	 *            How much values should be considered
	 * @return R squared - defines how much percent of dependent values can be
	 *         explained by independent values. The closer R squared is to 1,
	 *         the better relationship between dependent and independent values.
	 */
	public static double linearRegression(final ArrayList<Integer> x, final ArrayList<Double> y, final int n) {
		// first pass: read in data, compute xbar and ybar
		double sumx = 0.0, sumy = 0.0;
		for (int i = n - 1; i >= 0; i--) {
			sumx += x.get(i);
			sumy += y.get(i);
		}
		double xbar = sumx / n;
		double ybar = sumy / n;

		// second pass: compute summary statistics
		double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
		for (int i = n - 1; i >= 0; i--) {
			xxbar += (x.get(i) - xbar) * (x.get(i) - xbar);
			yybar += (y.get(i) - ybar) * (y.get(i) - ybar);
			xybar += (x.get(i) - xbar) * (y.get(i) - ybar);
		}
		double beta1 = xybar / xxbar;
		double beta0 = ybar - beta1 * xbar;

		// analyze results
		double ssr = 0.0; // regression sum of squares
		for (int i = n - 1; i >= 0; i--) {
			double fit = beta1 * x.get(i) + beta0;
			ssr += (fit - ybar) * (fit - ybar);
		}
		double r2 = ssr / yybar;
		return r2;
	}

	/**
	 * Exponential Moving Average with Dynamic Smoothing Factor
	 * 
	 * @param observations
	 *            Array that contains peer departures numbers.
	 * @param emas
	 *            Array that contains values of exponential moving averages
	 * @param interval
	 *            The length of interval that is used to calculate smoothing
	 *            factor
	 * @return The exponential moving average for given observations of peer
	 *         departures.
	 */
	public static double ema(ArrayList<Integer> observations, ArrayList<Double> emas) {
		double alpha = bestSmoothingFactor(observations, emas);
		final int lastObservation = observations.get(observations.size() - 1);
		final double lastEMA = emas.get(emas.size() - 1);
		// EMA today = EMA_yesterday + alpha * (value_today - EMA_yesterday)
		return lastEMA + (alpha * (lastObservation - lastEMA));
	}

	/**
	 * Calculation of sample standard deviation for a set of values. Inspired
	 * by: http://introcs.cs.princeton.edu/java/stdlib/StdStats.java.html
	 * 
	 * @param range
	 *            A set of values
	 * @param currentAverage
	 *            Average value for the set
	 * @return The standard deviation
	 */
	public static double standardDeviation(ArrayList<Integer> range, double average) {
		if (range.size() <= 1) {
			return 0.0;
		}
		double sd = 0;
		final int size = range.size();
		for (int i = 0; i < size; i++) {
			sd = sd + Math.pow(range.get(i) - average, 2);
		}
		sd = sd / (size - 1);
		return Math.sqrt(sd);
	}

	/**
	 * Exponential Moving Average with Dynamic Smoothing Factor plus Deviation
	 * 
	 * @param observations
	 *            Array that contains recent peer departures numbers.
	 * @param currentAverage
	 *            Current exponential moving average
	 * @param interval
	 *            The length of interval that is used to calculate deviation
	 * @return The exponential moving average plus deviation for a given
	 *         observation of peer departures. This value is regarded as a
	 *         prediction value for the next second.
	 */
	public static int predictedValue(ArrayList<Integer> observations, double currentAverage) {
		double deviation = standardDeviation(observations, currentAverage);
		//round up, as we defined a minimum reliability
		return (int) Math.floor(currentAverage + deviation);
	}

}
