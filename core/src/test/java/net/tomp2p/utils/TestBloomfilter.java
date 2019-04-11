package net.tomp2p.utils;

import org.junit.Test;

import java.math.BigDecimal;

public class TestBloomfilter {
    @Test
    public void testLargeBF() {
        BigDecimal bigDecimal1 = new BigDecimal("1");
        BigDecimal bigDecimal2 = new BigDecimal("2");


        BigDecimal bigDecimal4 = bigDecimal1.divide(bigDecimal2.pow(256));
        System.out.println("prob to find number: "+bigDecimal4);

        System.out.println("prob to find falpos: "+expectedFalsePositiveProbability(40,1_000_000,100_000_000));

    }

    private double expectedFalsePositiveProbability(int k, int expectedElements, int bitArraySize) {
        return Math.pow((1 - Math.exp(-k * (double) expectedElements / bitArraySize)), k);
    }
}
