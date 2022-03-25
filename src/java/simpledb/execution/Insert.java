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

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private ArrayList<Tuple> tupleList = new ArrayList<>();
    private Iterator<Tuple> iterator;
    TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
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
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("ha");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;

    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        int cnt = 0;
        while(child.hasNext()){
            Tuple next = child.next();
            cnt++;
            try {
                Database.getBufferPool().insertTuple(t, tableId, next);
            }catch (Exception e){

            }
        }
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0,new IntField(cnt));
        tupleList.add(tuple);
        iterator = tupleList.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        iterator = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        iterator = tupleList.iterator();
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
        if(iterator!=null && iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
