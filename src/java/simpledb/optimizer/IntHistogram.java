package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int min;
    private int max;
    private double avg;
    private MyGram[] myGrams;
    private int ntups;

    public class MyGram {
        double left;
        double right;
        double w;
        int h;

        //前闭后开
        public MyGram(double left, double right) {
            this.left = left;
            this.right = right;
            this.w = this.right - this.left;
            this.h = 0;
        }

        public boolean inRange(int v) {
            if (v >= left && v < right) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "MyGram{" +
                    "left=" + left +
                    ", right=" + right +
                    ", w=" + w +
                    ", h=" + h +
                    '}';
        }
    }

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.ntups = 0;
        this.myGrams = new MyGram[buckets];
        this.avg = (double) (max - min) / (double) buckets;
        double l = min;
        if (this.avg % 1 != 0) {
            this.avg = (int) (this.avg + 1);
        }
        for (int i = 0; i < buckets; i++) {
            myGrams[i] = new MyGram(l, l + avg); //前闭后开
            l = l + avg;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public int binarySearch(int v) {
        int l = 0;
        int r = buckets - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (myGrams[mid].inRange(v)) {
                return mid;
            }
            if (myGrams[mid].left > v) {
                r = mid - 1;
            } else if (v >= myGrams[mid].right) {
                l = mid + 1;
            }
        }
        return -1;
    }

    public void addValue(int v) {
        // some code goes here
        int i = binarySearch(v);
        if (i != -1) {
            myGrams[i].h++;
            ntups++;
        }

    }

    public void setNtups(int ntups) {
        this.ntups = ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int i = binarySearch(v);
        MyGram b;
        if (i != -1) {
            b = myGrams[i];
        } else {
            b = null;
        }
        if (op == Predicate.Op.EQUALS) {
            if (b != null) {
                return ((b.h / b.w)) / (double) ntups;
            } else {
                return 0.0;
            }
        } else if (op == Predicate.Op.GREATER_THAN) {
            if (v < min) {
                return 1.0;
            } else if (v >= max) {
                return 0.0;
            } else if (b != null) {
                double res = 0;
                double add = ((double) b.h / (double) ntups) * ((b.right - v) / b.w);
                res = res + add;
                int j;
                for (j = i + 1; j < buckets; j++) {
                    res = res + (double) myGrams[j].h / (double) ntups;
                }
                return res;
            }
        } else if (op == Predicate.Op.LESS_THAN) {
            if (v <= min) {
                return 0.0;
            } else if (v >= max) {
                return 1.0;
            } else if (b != null) {
                double res = 0;
                double add = ((double) b.h / (double) ntups) * ((v - b.left) / b.w);
                res = res + add;
                int j;
                for (j = 0; j < i; j++) {
                    res = res + (double) myGrams[j].h / (double) ntups;
                }
                return res;
            }
        } else if (op == Predicate.Op.NOT_EQUALS) {
            if (b != null) {
                return 1 - (((b.h / b.w)) / (double) ntups);
            } else {
                return 1.0;
            }
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v <= min) {
                return 1.0;
            } else if (v > max) {
                return 0.0;
            } else if (b != null) {
                double res = 0;
                double add = ((double) b.h * ((b.right - v + 1) / b.w)) / (double) ntups;
                res = res + add;
                int j;
                for (j = i + 1; j < buckets; j++) {
                    res = res + (double) myGrams[j].h / (double) ntups;
                }
                return res;
            }
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v < min) {
                return 0.0;
            } else if (v >= max) {
                return 1.0;
            } else if (b != null) {
                double res = 0;
                double add = ((double) b.h / (double) ntups) * ((v - b.left + 1) / b.w);
                res = res + add;
                int j;
                for (j = 0; j < i; j++) {
                    res = res + (double) myGrams[j].h / (double) ntups;
                }
                return res;
            }
        }

        return 0.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return avg;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + avg +
                ", myGrams=" + Arrays.toString(myGrams) +
                '}';
    }
}
