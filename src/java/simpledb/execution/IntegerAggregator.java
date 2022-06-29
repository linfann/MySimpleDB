package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;

    private Map<Field,ResInfo> map;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        map = new HashMap<>();
    }

    private static class ResInfo{
        int count;
        int sum;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;

        private void add(IntField field){
            int value = field.getValue();
            count++;
            sum += value;
            max = Math.max(max,value);
            min = Math.min(min,value);
        }

        private int getResult(Op what){
            switch (what){
                case COUNT:return count;
                case SUM:return sum;
                case AVG:return sum/count;
                case MIN:return min;
                case MAX:return max;
            }
            return 0;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (tupleDesc == null)generateTupleDesc(tup.getTupleDesc());

        IntField field = (IntField) tup.getField(afield);
        Field group = tup.getField(gbfield);
        if (map.get(group)!=null){
            ResInfo resInfo = map.get(group);
            resInfo.add(field);
        }else{
            ResInfo resInfo = new ResInfo();
            resInfo.add(field);
            map.put(group,resInfo);
        }
    }

    private void generateTupleDesc(TupleDesc td){
        if (gbfield == NO_GROUPING){
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        }else {
            this.tupleDesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE},
                    new String[]{td.getFieldName(gbfield),td.getFieldName(afield)});
        }
    }
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        if (this.gbfield != NO_GROUPING){
            this.map.forEach((key,value)->{
                Tuple tuple = new Tuple(this.tupleDesc);
                tuple.setField(0,key);
                tuple.setField(1,new IntField(value.getResult(what)));
                tuples.add(tuple);
            });
        }else {
            Tuple tuple = new Tuple(this.tupleDesc);
            ResInfo resInfo = map.get(null);
            tuple.setField(0,new IntField(resInfo.getResult(what)));
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
