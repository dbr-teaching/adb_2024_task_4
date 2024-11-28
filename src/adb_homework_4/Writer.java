package adb_homework_4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is simply a task attempting to hold many write locks for long time and updating major parts of the database.
 * @author Martin Schaeler martin.schaeler@kit.edu
 *
 *# Permission is hereby granted, free of charge, to any person obtaining a copy
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
public class Writer extends Transaction  {
	final int task;
	/**
	 * Contains the id's of the tuples that shall be updated.
	 * (1) The id's must not be ordered. 
	 * (2) The id's must not contain duplicates. Otherwise, we will lock the table multiple times.
	 */
	final int[] ids_to_update;
	/** Indicates whether the results of this transaction should be visible already. Is only used for checking by the reader.*/
	final public AtomicBoolean comitted = new AtomicBoolean(false); 
	final LockingTable table;
	final ArrayList<double[]> beforeImages= new ArrayList<double[]>();
	
	/**
	 * 
	 * @param id - some unique id
	 * @param t  - which table to we write to?
	 */
	public Writer(int id, LockingTable t){
		this.task  = id;
		this.table = t;
		this.ids_to_update = new int[LockingTable.numQueriesPerTask];
		getTuplesForUpdate();
	}
	
	/**
	 * (1) The id's must not be ordered. 
	 * (2) The id's must not contain duplicates. Otherwise, we will lock the table multiple times.
	 */
	private void getTuplesForUpdate() {
		for(int q=0;q<ids_to_update.length;q++) { 
			int id;
			while(contains((id = table.rand.nextInt(table.numTuples)),this.ids_to_update));
			ids_to_update[q] = id;
		}
	}

	private boolean contains(int id, int[] array) {
		for(int v : array ) {
			if(v==id) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void run() {
		if(table.isolationLevel == LockingTable.USE_TABLE_LOCKS) {
			table.getTableLock();		// (1) Exclusively lock the table
			update();					// (2) Do the work
			comitted.set(true);			// Needs to done before the locks are released
			table.releaseTableLock();	// (3) Release the exclusive lock
		}else if(table.isolationLevel == LockingTable.REPEATABLE_READ) {
			update_repeatable_read();
		}else if(table.isolationLevel == LockingTable.READ_COMMITED) {
			update_read_commited();
		}else if(table.isolationLevel == LockingTable.READ_UN_COMMITED) {
			update();
			comitted.set(true);
		}else{
			System.err.println("ISOLATION_LEVEL="+table.isolationLevel+" currently not supported.");
			return;
		}
		
		System.out.println("Done task "+task+". I am finisher number "+table.tasksFinished.incrementAndGet());
	}

	void update_repeatable_read() {
		for(int tid : ids_to_update) {
			table.getWriteLock(tid, this);
			double new_value = table.rand.nextDouble()*table.maxValue;//some hopefully different value
			table.update(tid,task,new_value);
		}
		comitted.set(true);	// Needs to done before the locks are released
		for(int tid : ids_to_update) {
			table.releaseWriteLock(tid);
		}
	}

	void update_read_commited() {
		for(int tid : ids_to_update) {
			table.getWriteLock(tid, this);
			double new_value = table.rand.nextDouble()*table.maxValue;//some hopefully different value
			table.update(tid,task,new_value);
		}
		comitted.set(true);	// Needs to done before first lock is released
		for(int tid : ids_to_update) {
			table.releaseWriteLock(tid);
		}
	}
	
	/**
	 * Called if table.isolationLevel == LockingTable.USE_TABLE_LOCKS (after getting the table lock), or in case no locks are use at all (table.isolationLevel == LockingTable.READ_UN_COMMITED). 
	 */
	private void update() {
		for(int tid : ids_to_update) {
			double new_value = table.rand.nextDouble()*table.maxValue;//some hopefully different value
			table.update(tid,task,new_value);
		}
	}
	
	public String toString() {
		return "Task "+task +" updating "+ ( (ids_to_update.length < 10 ) ? Arrays.toString(ids_to_update) : "["+ids_to_update[0]+", "+ids_to_update[1]+", "+ids_to_update[2]+", ... ");
	}

	@Override
	public boolean rollback() {
		for(int i=0;i<this.myWriteLocks.size();i++) {
			//XXX assumes that index i belongs to the same tuple
			int tid = this.myWriteLocks.get(i);
			double[] beforeImage = this.beforeImages.get(i);
			this.table.restoreOldTupleValues(beforeImage, tid);//order is important
			this.table.releaseWriteLock(tid);
		}
		if(!this.myReadLocks.isEmpty()) {
			for(int tid : myReadLocks) {
				this.table.releaseReadLock(tid);
			}
		}
		return true;
	}
}
