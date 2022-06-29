package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] heights;
    private int maxVal;
    private int minVal;
    private int buckets;
    private int totalTuples;
    private int width;
    private int range;
    private int lastRange;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.minVal = min;
        this.maxVal = max;
        this.range = max-min+1;
        this.buckets = Math.min(buckets,range);
        this.heights = new int[buckets];
        this.totalTuples = 0;
        this.width = range/this.buckets;
        this.lastRange = range-(this.width*(this.buckets-1));
        //System.out.println(this);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v<minVal || v>maxVal)return;
        int idx = (v-this.minVal)/width;
        if (idx>=buckets)idx = buckets-1;
        heights[idx]++;
        this.totalTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int idx = Math.min((v-this.minVal)/width, buckets-1);
        int count;
        switch (op){
            case GREATER_THAN:
                if (v<minVal)return 1.0;
                if (v>=maxVal)return 0.0;
                count = getGreater(v,idx);
                return count*1.0/totalTuples;
            case GREATER_THAN_OR_EQ:
                if (v<=minVal)return 1.0;
                if (v>maxVal)return 0.0;
                count = getGreater(v,idx) + getEqual(v,idx);
                return count*1.0/totalTuples;
            case LESS_THAN:
                if (v>maxVal)return 1.0;
                if (v<=minVal)return 0.0;
                count = getLess(v,idx);
                return count*1.0/totalTuples;
            case LESS_THAN_OR_EQ:
                if (v>=maxVal)return 1.0;
                if (v<minVal)return 0.0;
                count = getLess(v,idx) + getEqual(v,idx);
                return count*1.0/totalTuples;
            case EQUALS:
                if (v>maxVal || v<minVal)return 0.0;
                count = getEqual(v,idx);
                return count*1.0/totalTuples;
            case NOT_EQUALS:
                if (v>maxVal || v<minVal)return 1.0;
                count = getEqual(v,idx);
                return 1.0 - count*1.0/totalTuples;
        }
    	// some code goes here
        return -1.0;
    }

    private int getGreater(int v, int idx){
        if (idx != buckets-1) {
            int count = (width - 1 - (v - minVal) % width) * heights[idx] / width;
            for (int i = idx+1; i<buckets;i++)count += heights[i];
            return count;
        }
        else
            return  (maxVal-v) * heights[idx] / lastRange;
    }

    private int getLess(int v, int idx){
        return totalTuples-getEqual(v,idx)-getGreater(v,idx);
    }

    private int getEqual(int v, int idx){

        if (idx != buckets-1) return heights[idx] / width;
        else return heights[idx] / lastRange;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram{" + "maxVal=" + maxVal + ", minVal=" + minVal + ", heights=" + Arrays.toString(heights)
                + ", buckets=" + buckets + ", totalTuples=" + totalTuples + ", width=" + width + ", lastRange="
                + lastRange + '}';
    }
}
