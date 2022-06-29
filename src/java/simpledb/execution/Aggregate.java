package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator childOpIterator;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private TupleDesc tupleDesc;
    private TupleDesc newTd;
    private Aggregator aggregator;
    private TupleIterator tupleIterator;
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.childOpIterator = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.tupleDesc = child.getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return tupleDesc.getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return tupleDesc.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        childOpIterator.open();
        if (tupleDesc.getFieldType(afield) == Type.INT_TYPE){
            this.aggregator = new IntegerAggregator(this.gfield,gfield==-1?null:tupleDesc.getFieldType(gfield),this.afield,aop);
        }else{
            this.aggregator = new StringAggregator(this.gfield,gfield==-1?null:tupleDesc.getFieldType(gfield),this.afield,aop);
        }

        while (this.childOpIterator.hasNext()){
            Tuple next = this.childOpIterator.next();
            this.aggregator.mergeTupleIntoGroup(next);
        }
        this.tupleIterator = (TupleIterator) this.aggregator.iterator();
        this.tupleIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.tupleIterator.hasNext())return this.tupleIterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();

    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (newTd == null){
            if (tupleIterator!=null){
                newTd = tupleIterator.getTupleDesc();
                System.out.println(newTd);
            }else if(this.gfield == -1){
                Type[] types = new Type[] { Type.INT_TYPE };
                String[] names = new String[] { this.aggregateFieldName() };
                this.newTd = new TupleDesc(types, names);
            }else {
                Type[] types = new Type[] { this.childOpIterator.getTupleDesc().getFieldType(gfield), Type.INT_TYPE };
                String[] names = new String[] { this.groupFieldName(), this.aggregateFieldName() };
                this.newTd = new TupleDesc(types, names);
            }
        }
        return newTd;
    }

    public void close() {
        // some code goes here
        childOpIterator.close();
        tupleIterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.childOpIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length > 0){
            this.childOpIterator = children[0];
        }
    }

}
