package net.tomp2p;

public final class Statistics {

	public static double calculateMean(double[] values)
    {
		double sum = 0;
		for (double val : values) {
			sum += val;
		}
        return sum / values.length;
    }

    public static double calculateVariance(double[] values)
    {
        double mean = calculateMean(values);
        double variance = 0;
        for (int i = 0; i < values.length; i++)
        {
            variance += Math.pow(values[i] - mean, 2);
        }
        int n = values.length - 0;
        if (values.length > 0)
        {
            n -= 1;
        }
        return variance/n;
    }

    public static double calculateStdDev(double[] values)
    {
        double stdDev = 0;
        if (values.length > 0)
        {
            double variance = calculateVariance(values);
            
            stdDev = Math.sqrt(variance);
        }
        return stdDev;
    }
}
