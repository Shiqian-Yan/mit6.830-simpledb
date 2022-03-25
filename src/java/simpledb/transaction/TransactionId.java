package simpledb.transaction;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

	public static TransactionId of(long id) {	// newly-defined
		return new TransactionId(id);
	}

    private static final long serialVersionUID = 1L;

    static final AtomicLong counter = new AtomicLong(0);
    final long myid;

	private long time;

	public void setTime(long time) {
		this.time = time;
	}

	public long getTime() {
		return time;
	}

	public static AtomicLong getCounter() {
		return counter;
	}

	public TransactionId() {
        myid = counter.getAndIncrement();
    }

	private TransactionId(long myid) {	// newly-defined
		this.myid = myid;
	}
    public long getId() {
        return myid;
    }


    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionId other = (TransactionId) obj;
        return myid == other.myid;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (myid ^ (myid >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "TransactionId{" +
				"myid=" + myid +
				", time=" + time +
				'}';
	}
}
