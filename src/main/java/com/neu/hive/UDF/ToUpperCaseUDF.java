package com.neu.hive.UDF;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.LinkedList;


public final class ToUpperCaseUDF extends UDAF {
    public static class StringDoublePair implements Comparable<StringDoublePair> {
        public StringDoublePair() {
            this.key = null;
            this.value = null;
        }

        public StringDoublePair(String key, double value) {
            this.key = key;
            this.value = value;
        }

        public String getString() {
            return this.key;
        }

        public int compareTo(StringDoublePair o) {
            return o.value.compareTo(this.value);
        }

        private String key;
        private Double value;
    }


    /**
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     *
     * The internal state can also contains fields with types like
     * ArrayList<String> and HashMap<String,Double> if needed.
     */
    public static class UDAFTopNState {
        // The head of the queue should contain the smallest element.
        //	private PriorityQueue<StringDoublePair> queue;
        private LinkedList<StringDoublePair> queue;
        private Integer N;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class UDAFTopNEvaluator implements UDAFEvaluator {

        UDAFTopNState state;

        public UDAFTopNEvaluator() {
            super();
            state = new UDAFTopNState();
            init();
        }

        /**
         * Reset the state of the aggregation.
         */
        public void init() {
            //	    state.queue = new PriorityQueue<StringDoublePair>();
            state.queue = new LinkedList<StringDoublePair>();
            state.N = null;
        }

        /**
         * Iterate through one row of original data.
         *
         * The number and type of arguments need to the same as we call this UDAF
         * from Hive command line.
         *
         * This function should always return true.
         */
        public boolean iterate(String key, Double value, Integer N) {

            if (state.N == null) {
                state.N = N;
            }
            if (value != null) {
                state.queue.add(new StringDoublePair(key, value));
                prune(state.queue, state.N);
            }
            return true;
        }

        /**
         * Terminate a partial aggregation and return the state. If the state is a
         * primitive, just return primitive Java classes like Integer or String.
         */
        public UDAFTopNState terminatePartial() {
            if (state.queue.size() > 0) {
                return state;
            } else {
                return null;
            }
        }

        /**
         * Merge with a partial aggregation.
         *
         * This function should always have a single argument which has the same
         * type as the return value of terminatePartial().
         */
        public boolean merge(UDAFTopNState o) {
            //	public boolean merge(UDAFTopNState o) throws UDAFTopNException {
            if (o != null) {
                state.queue.addAll(o.queue);
                if (o.N != state.N) {
                    //		    throw new UDAFTopNException();
                }
                prune(state.queue, state.N);
            }
            return true;
        }

        void prune(LinkedList<StringDoublePair> queue, int N) {
            while (queue.size() > N) {
                queue.removeLast();
            }
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        public LinkedList<String> terminate() {
            LinkedList<String> result = new LinkedList<String>();
            while (state.queue.size() > 0) {
                StringDoublePair p = state.queue.poll();
                result.addFirst(p.getString());
            }
            return result;
        }
    }

}
