package simpledb.util;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author freya
 * @date 2022/4/22
 **/
public class HeapFileIterator implements DbFileIterator {

    private TransactionId tid;
    private int tableId;
    int numPages;
    private int pagePos = 0;
    private Iterator<Tuple> pageIterator;
//    private HeapPage heapPage;


    public HeapFileIterator(TransactionId tid, int tableId, int numPages) throws DbException, TransactionAbortedException {
        this.tid = tid;
        this.tableId = tableId;
        this.numPages = numPages;
    }

    private Iterator<Tuple> getPageIterator(PageId pid){
        try {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid, Permissions.READ_ONLY);
            //System.out.println("It: " + page + "  " + pid.getPageNumber() + "  empty: " + page.getNumEmptySlots());
            return page.iterator();
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pagePos = 0;
        pageIterator = getPageIterator(new HeapPageId(tableId,pagePos));
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        while(pagePos < numPages){
            if (pageIterator == null)return false;
            if (pageIterator.hasNext())return true;
            pagePos++;
            if (pagePos >= numPages)return false;
            pageIterator = getPageIterator(new HeapPageId(tableId,pagePos));
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) return pageIterator.next();
        else throw new NoSuchElementException();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        pageIterator = null;
    }
}
