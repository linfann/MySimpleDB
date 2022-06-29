package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;
    private int count;
    private Map<Field, Integer> map;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */


    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT)throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.count = 0;
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.tupleDesc==null)generateTupleDesc(tup.getTupleDesc());
        Field group = tup.getField(gbfield);
        int count = map.getOrDefault(group, 0);
        count++;
        map.put(group,count);
    }

    private void generateTupleDesc(TupleDesc td){
        if (gbfield == NO_GROUPING){
            this.tupleDesc = new TupleDesc(new Type[]{Type.STRING_TYPE});
        }else {
            this.tupleDesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE},
                    new String[]{td.getFieldName(gbfield), td.getFieldName(afield)});
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        if (this.gbfield != NO_GROUPING){
            this.map.forEach((key,value)->{
                Tuple tuple = new Tuple(this.tupleDesc);
                tuple.setField(0,key);
                tuple.setField(1,new IntField(value));
                tuples.add(tuple);
            });
        }else {
            Tuple tuple = new Tuple(this.tupleDesc);
            int count = map.get(null);
            tuple.setField(0,new IntField(count));
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
