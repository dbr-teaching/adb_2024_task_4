import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

public class ToyExample {
	final static long TIME_OUT = 1;//seconds
		
	//Imagine a Table with two tuples. Each one having a lock.
	final Semaphore[] locks = { 
			new Semaphore(1, true)
			, new Semaphore(1, true)
	};
	
	public static void main(String[] args) {
		ToyExample te = new ToyExample();
		MyTask t_1 = te.new MyTask(0);
		MyTaskLockInverse t_2 = te.new MyTaskLockInverse(1);
	
		run(te, t_1, t_2);
		System.out.println("Let's try something else named lazy cancellation");
		System.out.println("We simply set a flag such that the Thread aborts itslef");
		
		run_cancellation(t_1, te.new MyTaskLockLazyCancellation(3));
	}
	
	static void run(ToyExample te, Runnable t_1, Runnable t_2){
		ThreadPoolExecutor executor = 
				  (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
		executor.execute(t_1);
		executor.execute(t_2);
		executor.shutdown();//Terminate if all tasks done
		
		int counter = 0;
		while(!executor.isTerminated()) {//Wait until all tasks terminated
			try {
				Thread.sleep(2*TIME_OUT*1000);
				counter++;
				if(counter>3) {
					System.err.println("Obviously something is wrong...");
					executor.shutdownNow();//Kill them all
					System.out.println("As you see, we cannot really kill them, but only send an interrrupt. This also holds if change the Task from implementing the runnable Intface to Futures etc.");
					//Cleaning
					te.locks[0].release();
					te.locks[1].release();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	static void run_cancellation(Runnable t_1, MyTaskLockLazyCancellation t_2){
		ThreadPoolExecutor executor = 
				  (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
		executor.execute(t_1);
		executor.execute(t_2);
		executor.shutdown();//Terminate if all tasks done
		
		int counter = 0;
		while(!executor.isTerminated()) {//Wait until all tasks terminated
			try {
				Thread.sleep(2*TIME_OUT*1000);
				counter++;
				if(counter>3) {
					System.err.println("Obviously something is wrong... set the cancellation thread");
					t_2.ask_for_abort();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	class MyTask implements Runnable{
		final int id;
		
		MyTask(int id){
			this.id = id;
		}
		
		@Override
		public void run() {
			try {
				// Try to the lock. Keep trying if we do not get it immediately.
				int tuple_id = 0;
				while(!locks[tuple_id].tryAcquire(1,TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
					System.err.println("MyTask "+id+" still waiting for lock on "+tuple_id);
				}
				System.out.println("MyTask "+id+" got lock on "+tuple_id);
				Thread.sleep(TIME_OUT);
				
				//Try to ge the other lock a well...
				tuple_id = 1;
				while(!locks[tuple_id].tryAcquire(1,TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
					System.err.println("MyTask "+id+" still waiting for lock on "+tuple_id);
				}
				System.out.println("MyTask "+id+" got all locks");
			} catch (InterruptedException e) {
				System.err.println("MyTask "+id+" interrupted - did some one try to kill me?");
				//e.printStackTrace();
			}
		}
	}
	
	class MyTaskLockInverse implements Runnable{
		final int id;
		
		MyTaskLockInverse(int id){
			this.id = id;
		}
		
		@Override
		public void run() {
			try {
				// Try to the lock. Keep trying if we do not get it immediately.
				int tuple_id = 1;
				while(!locks[tuple_id].tryAcquire(1,TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
					System.err.println("MyTask "+id+" still waiting for lock on "+tuple_id);
				}
				System.out.println("MyTask "+id+" got lock on "+tuple_id);
				Thread.sleep(TIME_OUT);
				
				//Try to ge the other lock a well...
				tuple_id = 0;
				while(!locks[tuple_id].tryAcquire(1,TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
					System.err.println("MyTask "+id+" still waiting for lock on "+tuple_id);
				}
				
				System.out.println("MyTask "+id+" got all locks");
			} catch (InterruptedException e) {
				System.err.println("MyTask "+id+" interrupted - did some one try to kill me?");
				//e.printStackTrace();
			}
		}
	}
	
	class MyTaskLockLazyCancellation implements Runnable{
		final int id;
		volatile boolean abort_me = false;
		ArrayList<Integer> my_locks = new ArrayList<Integer>(2);
		
		MyTaskLockLazyCancellation(int id){
			this.id = id;
		}
		
		void lock(final int tuple_id) throws InterruptedException{
			while(!locks[tuple_id].tryAcquire(1,TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
				System.err.println("MyTask "+id+" still waiting for lock on "+tuple_id);
				if(abort_me==true) {
					abort();
				}
			}
			System.out.println("MyTask "+id+" got lock on "+tuple_id);
			my_locks.add(tuple_id);
		}
		
		@Override
		public void run() {
			try {
				// Try to the lock. Keep trying if we do not get it immediately.
				int tuple_id = 1;
				lock(tuple_id);
				Thread.sleep(TIME_OUT);
				
				//Try to ge the other lock a well...
				tuple_id = 0;
				lock(tuple_id);
				
				System.out.println("MyTask "+id+" got all locks");
			} catch (InterruptedException e) {
				System.err.println("MyTask "+id+" interrupted - did some one try to kill me?");
				//e.printStackTrace();
			}
		}
		
		void ask_for_abort() {
			System.out.println("Requesting abort for Task "+id);
			abort_me = true;
		}
		
		private void abort() {
			System.out.println("Abort Task "+id);
			for(int lock : my_locks) {
				locks[lock].release(1);
			}
		}
	}
}
