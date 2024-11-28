package adb_homework_4;

import java.util.ArrayList;

/**
 * Executes two (read only) full-table scans per transactions and checks whether it sees uncommitted changes.
 * In addition, it checks for non-repeatable reads.
 * Emulates some read workload on the table.
 * Measures throughput.
 * /**
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
*
 *
 */
public class Reader extends Transaction {
	public boolean displayErrors = false;
	final double[] repeatbale_read_checking;
	long start, stop, last_stop;
	int numExecutions = 0;
	ArrayList<UncommittedReads> un_commited = new ArrayList<UncommittedReads>();
	ArrayList<NonRepeatableReads> non_repeatable = new ArrayList<NonRepeatableReads>();
	
	final LockingTable table;
	
	public Reader(LockingTable t){
		this.table = t;
		this.repeatbale_read_checking = new double[table.numTuples];
	}
	
	@Override
	public void run() {
		start = System.currentTimeMillis();
		last_stop = start;
		while(table.tasksFinished.get()!=LockingTable.numTasks) {// runs until last Writer task is done. Objective is to execute as many loops as possible.
			if(table.isolationLevel == LockingTable.USE_TABLE_LOCKS) {
				// (1) Exclusively lock the table
				table.getTableLock();
				//(2) Do the work
				read();
				table.releaseTableLock();
			}else if(table.isolationLevel == LockingTable.REPEATABLE_READ) {
				read_repeatable();
			}else if(table.isolationLevel == LockingTable.READ_COMMITED) {
				read_commited();
			}else if(table.isolationLevel == LockingTable.READ_UN_COMMITED) {
				read();// We do not need locks here.
			}else{
				System.err.println("ISOLATION_LEVEL="+table.isolationLevel+" currently not supported.");
				return;
			}
			stop = System.currentTimeMillis();
			
			// Print statistics
			System.out.println("Reader finished query "+(numExecutions++)+" needed "+(stop-last_stop)+ " avg run time is "+(stop-start)/(long)numExecutions);
			if(!un_commited.isEmpty()) {
				if(table.isolationLevel <= LockingTable.READ_UN_COMMITED)
					System.out.println("Found un_commited data "+this.un_commited.size()+ " [OK for this level]");
				else
					System.err.println("Found un_commited data "+this.un_commited.size());
			}
			if(!non_repeatable.isEmpty()) {
				if(table.isolationLevel < LockingTable.REPEATABLE_READ)
					System.out.println("Found non_repeatable reads "+this.non_repeatable.size()+ " [OK for this level]");
				else
					System.err.println("Found non_repeatable reads "+this.non_repeatable.size());
			}
		}
	}

	void read_commited() {
		//first full-table scan
		for(int tid=0;tid<table.numTuples;tid++) {
			table.getReadLock(tid, this);
			check_query_1(tid);
			table.releaseReadLock(tid);
		}
		
		//second full-table scan
		for(int tid=0;tid<table.numTuples;tid++) {
			table.getReadLock(tid, this);
			check_query_2(tid);
			table.releaseReadLock(tid);
		}
	}

	void read_repeatable() {
		//first full-table scan
		for(int tid=0;tid<table.numTuples;tid++) {
			table.getReadLock(tid, this);
			check_query_1(tid);
		}
		
		//second full-table scan
		for(int tid=0;tid<table.numTuples;tid++) {
			check_query_2(tid);
			table.releaseReadLock(tid);
		}
	}

	/**
	 * Only checks whether the current tuples contains uncommitted changes. Also stores the current value, to check for non-repeatable reads in the 2nd query.
	 * @param tid
	 */
	void check_query_1(final int tid) {
		double[] tuple = table.getTuple(tid);
		
		double check = tuple[LockingTable.DATA_VALUE];
		repeatbale_read_checking[tid] = check;
		int modified_by = (int) tuple[LockingTable.MODIFIED_BY];
		if(modified_by != LockingTable.NOT_MODIFIED) {
			if(!table.myWriter[modified_by].comitted.get()) {
				if(displayErrors) System.err.println("Seeing non comitted values of task "+modified_by +" at "+tid);
				this.un_commited.add(new UncommittedReads(numExecutions, tid, modified_by));
			}
		}
	}
	
	/**
	 * Checks for uncommitted data and non-repeatable reads
	 */
	private void check_query_2(final int tid) {
		double[] tuple = table.getTuple(tid);
		
		double check = tuple[LockingTable.DATA_VALUE];
		if(check != repeatbale_read_checking[tid]){
			if(displayErrors) System.err.println("Found non-repeatable read at "+tid+ " issued by "+ tuple[LockingTable.MODIFIED_BY]);
			this.non_repeatable.add(new NonRepeatableReads(numExecutions, tid));
		}
		int modified_by = (int) tuple[LockingTable.MODIFIED_BY];
		if(modified_by != LockingTable.NOT_MODIFIED) {
			if(!table.myWriter[modified_by].comitted.get()) {
				if(displayErrors) System.err.println("Seeing non comitted values of task "+modified_by +" at "+tid);
				this.un_commited.add(new UncommittedReads(numExecutions, tid, modified_by));
			}
		}
	}
	
	private void read() {
		//first full-table scan
		for(int tid=0;tid<table.numTuples;tid++) {
			check_query_1(tid);
		}
		
		//second full-table scan
		for(int tid=0;tid<table.numTuples;tid++) {
			check_query_2(tid);
		}
	}

	class UncommittedReads{
		final int num_executions, tid, modified_by;
		
		public UncommittedReads(int num_executions, int tid, int modified_by) {
			this.tid = tid;
			this.num_executions = num_executions;
			this.modified_by = modified_by;
		}
		public String toString() {
			return "Seeing non comitted values of task \"+modified_by +\" at \"+tid";
		}
	}
	
	class NonRepeatableReads{
		final int num_executions, tid;
		
		public NonRepeatableReads(int num_executions, int tid) {
			this.tid = tid;
			this.num_executions = num_executions;
		}
		public String toString() {
			return "Found non-repeatable read at "+tid;
		}
	}

	@Override
	public boolean rollback() {
		if(!myWriteLocks.isEmpty()) {
			System.err.println("rollback(): Readers should not holds write locks");
		}
		for(int tid_locked : myReadLocks) {
			this.table.releaseReadLock(tid_locked);
		}
		return true;
	}
}