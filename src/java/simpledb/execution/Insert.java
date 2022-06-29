package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator childIterator;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean isCalled;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (child.getTupleDesc() == null || !child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("TupleDesc of child differs from table into which we are to insert");
        this.tid = t;
        this.tableId = tableId;
        this.childIterator = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
}

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        childIterator.open();
        super.open();
    }

    public void close() {
        // some code goes here
        childIterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (isCalled)return null;
        int cnt = 0;
        while (childIterator.hasNext()){
            Tuple next = childIterator.next();
            try {
                Database.getBufferPool().insertTuple(tid,tableId,next);
            } catch (IOException e) {
                e.printStackTrace();
            }
            cnt++;
        }
        //if (isCalled&&cnt==0)return null;
        isCalled = true;
        Tuple tuple = new Tuple(this.tupleDesc);
        tuple.setField(0,new IntField(cnt));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{childIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length>0)this.childIterator = children[0];
    }
}
