package adb_homework_4;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author Martin Schaeler martin.schaeler@kit.edu
 * 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
*/

public class LockingTable {
	/** Number of Writer tasks updating the DB concurrently. */
	static int numTasks			= 10;
	/** Number of update statements per tasks. */
	static int numQueriesPerTask 	= 100;
	/**
	 * These are the transactions issuing only write locks.
	 */
	final Writer[] myWriter;
	static final int THOUSAND = 1000;
	/** Parameter scaling the write cost in update()*/
	static final long ADDITIONAL_WRITE_COST = 10;
	
	Random rand = new Random(123456);
	int numTuples = 5*THOUSAND*THOUSAND;
	/** maximum value of data_value attribute*/
	double maxValue = 50.0f;
	/**
	 * Counts the number of finished tasks. Needs to be atomic, since multiple tasks may write to it concurrently..
	 */
	AtomicInteger tasksFinished;
	public static final int NOT_MODIFIED = -1;
	
	/** Shall we start the reader task?*/
	boolean runReader = true;
	/** Shall we start the writer task?*/
	boolean runWriter = true;
	
	/******************* Begin Table and pseudo schema ********************/
	/**
	 * tableData[tid] gives me the tid'th tuple.
	 * Each tuple has three attributes. You can access them via:  table_data[tid][data_id] etc.
	 */
	private double[][] tableData;
	/**  Attribute offset of DATA_ID in tuple, i.e., use tuple[DATA_ID]	 */
	static final int DATA_ID 		= 0;
	/**  Attribute offset of DATA_VALUE in tuple, i.e., use tuple[DATA_VALUE]	 */
	static final int DATA_VALUE 	= 1;
	/**  Attribute offset of MODIFIED_BY in tuple, i.e., use tuple[MODIFIED_BY]	 */
	static final int MODIFIED_BY 	= 2;
	
	/******************* Begin SQL isolation levels ********************/
	
	/** Definition according to SQL standard: No Islation */
	static final int READ_UN_COMMITED	= 0;
	/** Definition according to SQL standard:  Nonrepeatable reads and phantom reads possible*/
	static final int READ_COMMITED	 	= READ_UN_COMMITED+1;
	/** Definition according to SQL standard phantom reads possible*/
	static final int REPEATABLE_READ	= READ_COMMITED+1;
	/** enforce serial execution: no anomaly possible */
	static final int USE_TABLE_LOCKS 	= REPEATABLE_READ+1;

	final int isolationLevel;
	static final String[] LOCK_LEVEL_NAMES = {"READ_UN_COMMITED",  "READ_COMMITED" , "REPEATABLE_READ" , "SERIAL"};
	//static final long ADDTIONAL_QUERY_COST = 1;//For Thread.sleep()
	
	/******************* Begin locking stuff ********************/
	
	/** Read/Write lock for each tuple. Use functions getReadLock(), getWriteLock, releaseReadLock, releaseWriteLock */
	private final Semaphore rowLocks[];
	/** Table lock for serial execution. */
	private final Semaphore table_lock = new Semaphore(1, true);//only one Table can access, FIFO guarantee.
	long TIME_OUT = 5;//seconds
	
	public LockingTable(final int isolation_level){
		System.out.println("Creating Table with isolation level "+LOCK_LEVEL_NAMES[isolation_level] +" with "+numTuples+" tuples");
		this.isolationLevel = isolation_level;
		this.tasksFinished 	= new AtomicInteger(0);
		this.myWriter 		= new Writer[numTasks];
		
		this.tableData 		= new double[numTuples][3];	//three attributes
		this.rowLocks 		= new Semaphore[numTuples];	//one for each tuple
		
		System.out.print("Creating DB ");
		//insert data and prepare locks
		for(int i=0;i<numTuples;i++){
			this.tableData[i][DATA_ID] 		= i;							//set key 
			this.tableData[i][DATA_VALUE] 	= rand.nextDouble()*maxValue;	//some value
			this.tableData[i][MODIFIED_BY] 	= NOT_MODIFIED;					//no one modified me
			
			//also create locking stuff
			this.rowLocks[i] = new Semaphore(numTasks, true);//they all can read concurrently requiring one permit. A write, however, requires *all* permits.
		}
		System.out.println("[DONE]");
	}
	
	void startWriters(ThreadPoolExecutor executor) {
		//create them
		for(int i=0;i<numTasks;i++) {
			myWriter[i] = new Writer(i, this); 
		}
	
		//start them
		for(Writer task : myWriter) {
			System.out.println("Starting "+task);
			executor.execute((Runnable) task);
		}
		executor.shutdown();
	}
	
	/**
	 * Call this to start the experiment.
	 */
	public void run() {		
		if(runReader==false) {
			System.out.println("Not running reader");
		}
		if(runWriter==false) {
			System.out.println("Not running writers");
		}		
		//Execute tasks
		ThreadPoolExecutor executor = 
				  (ThreadPoolExecutor) Executors.newFixedThreadPool(numTasks);
		
		if(runReader) {
			Reader r = new Reader(this);   
			executor.execute((Runnable) r);
		}
		
		if(runWriter) {
			startWriters(executor);
		}
		
		double start, stop;
		start = System.currentTimeMillis();
		
		//wait for the tasks to finish
		while(!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+tasksFinished+" of "+numTasks);
	}
	
	public static void main(String[] args){
		//Config the infrastructure
		int isolation_level;
		//isolation_level = USE_TABLE_LOCKS;
		isolation_level = REPEATABLE_READ;
		//isolation_level = READ_COMMITED;
		//isolation_level = READ_UN_COMMITED;
		LockingTable.numTasks = 10;// On my machine 10 tasks is enough plenty of deadlocks for REPEATABLE_READ isolation level. XXX-set it before creating LockingTable
		
		//Create DB
		LockingTable b = new LockingTable(isolation_level);

		//Parameters relevant for experimentation
		//b.run_reader = false;//set it to true if you want only writers to start.
		b.run();
	}

	/**
	 * Only for reading. Do not modify the tuple. 
	 * It will be visible in the table without any isolation level guarantees. 
	 * For writing to a tuple call update();
	 * @param tid
	 * @return
	 */
	public double[] getTuple(final int tid) {
		return this.tableData[tid];
	}

	/**
	 * Use this method to update a tuple. It sleeps after the actual write to emulate that writing usually is costly.
	 * @param tid
	 * @param task
	 * @param new_value
	 * @return before image of the modified tuple
	 */
	public double[] update(final int tid, final int task, final double new_value) {
		double[] tuple = this.tableData[tid];
		double[] beforeImage = new double[tuple.length]; 
		System.arraycopy(tuple, 0, beforeImage, 0, tuple.length);//copy tuple into beforeImage
		tuple[MODIFIED_BY] = task;
		tuple[DATA_VALUE]  = new_value;
		try {
			Thread.sleep(ADDITIONAL_WRITE_COST);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return beforeImage;
	}
	
	/**
	 * Call this in rollback to restore the old values of a tuple. 
	 * Note, we assume that you hold a write lock on this tuple, or no locking is required. 
	 * @param beforeImage
	 * @param tid
	 */
	public void restoreOldTupleValues(double[] beforeImage, int tid) {
		double[] tuple = this.tableData[tid];
		System.arraycopy(beforeImage, 0, tuple, 0, tuple.length);//copy beforeImage into tuple
	}
	
	/**
	 * Exclusively lock the table. Do not forget to release this lock. There is no check for row locks. So, do not mix the isolation levels.
	 */
	public void getTableLock() {
		this.table_lock.acquireUninterruptibly();
	}
	
	public void releaseTableLock() {
		this.table_lock.release();
	}
	
	/**
	 * Get a non exclusive lock on this tuple. It uses one permit of the Semaphore. 
	 * The calling Thread notifies you every TIME_OUT seconds when it is still waiting.
	 * @param tid
	 */
	public void getReadLock(final int tid, Transaction t){
		//try as long as we get this permit nd report every time_out second that I am still waiting
		try {
			while(!this.rowLocks[tid].tryAcquire(TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
				System.out.println(t.toString()+" Waiting for read lock of tuple "+tid+ " Deadlock?");
			}
			t.readLockGranted(tid);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get an exclusive lock, by using all permits.
	 * The calling Thread notifies you every TIME_OUT seconds when it is still waiting.
	 * @param tid
	 */
	public void getWriteLock(final int tid,  Transaction t){
		try {
			while(!this.rowLocks[tid].tryAcquire(numTasks,TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
				System.out.println(t.toString()+" Waiting for write lock of tuple "+tid+ " Deadlock?");
			}
			t.writeLockGranted(tid);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Release a non exclusive lock on this tuple.
	 * @param tid
	 */
	public void releaseReadLock(final int tid){
		this.rowLocks[tid].release();
	}
	
	/**
	 * Release an exclusive lock, by giving back all permits.
	 * @param tid
	 */
	public void releaseWriteLock(final int tid){
		this.rowLocks[tid].release(numTasks);
	}
	
	//You can play with these functions, but be careful. 
	void upgrateReadToWriteLock(final int tid){
		try {
			Semaphore s = this.rowLocks[tid];
			if(s.availablePermits()==numTasks) {
				System.err.println("I am sure you have no read lock on this tuple "+tid);
			}
			s.acquire(numTasks-1);//-1 Because we already use one permit for the read lock
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	void downgrateWriteToReadLock(final int tid){
		Semaphore s = this.rowLocks[tid];
		if(s.availablePermits()!=0) {
			System.err.println("There are permits left. So, no one has an exclusive lock "+tid);
		}
		s.release(numTasks-1);//we keep one permit, i.e., a read lock preventing anybody from getting an exclusive lock on this tuple
	}
}
