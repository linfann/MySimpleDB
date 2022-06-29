package simpledb.transaction;

/**
 * @author freya
 * @date 2022/6/20
 **/
public class PageLock {
    public static final int SHARE = 0;
    public static final int EXCLUSIVE = 1;

    private TransactionId tid;
    private int type;

    public PageLock(TransactionId tid, int type) {
        this.tid = tid;
        this.type = type;
    }

    public TransactionId getTid() {
        return tid;
    }

    public void setTid(TransactionId tid) {
        this.tid = tid;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
