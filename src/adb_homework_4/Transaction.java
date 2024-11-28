package adb_homework_4;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Extends Java's runnable Interface with an additional rollback() method. And other stuff you need to manage transactions and avoid or release deadlocks.
 * 
 * For deadlock detection the Transaction should known what locks the hold, and what type they are.
 * @author b1074672
 *
 */
public abstract class  Transaction implements Runnable{
	/**
	 * In this list you should manage all the read locks the transaction currently holds.
	 */
	ArrayList<Integer> myReadLocks = new ArrayList<Integer>();
	/**
	 * In this list you should manage all the write locks the transaction currently holds.
	 */
	ArrayList<Integer> myWriteLocks = new ArrayList<Integer>();
	/**
	 * true should indicate that the transaction was rolled back and thus needs t be started again.
	 */
	AtomicBoolean isRolledBack = new AtomicBoolean(false);

	public boolean begin_of_transaction() {
		//clear just in case there are locks remaining from last execution attemp
		myReadLocks.clear();
		myWriteLocks.clear();
		return true;
	}
	
	public ArrayList<Integer> myReadLocks() {
		return this.myReadLocks;
	}

	public ArrayList<Integer> myWriteLocks() {
		return this.myWriteLocks;
	}
	
	public abstract boolean rollback();
	
	/**
	 * If you do not change the LockingTable implementation, this method ensures that all the read locks you acquire are stored.
	 * @param id
	 */
	public void readLockGranted(final int id){
		myReadLocks.add(id);
	}
	/**
	 * If you do not change the LockingTable implementation, this method ensures that all the read locks you acquire are stored.
	 * @param id
	 */
	public void writeLockGranted(final int id){
		myWriteLocks.add(id);
	}
	
	public boolean isRolledBack() {
		return this.isRolledBack.get();
	}
	
}
