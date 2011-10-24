package net.tomp2p.p2p;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.tomp2p.futures.FutureRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scheduler
{
	private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
	//private static final String THREAD_NAME1="Send-later-thread";
	private static final String THREAD_NAME2="Peer-call-later-thread";
	//private final BlockingQueue<FutureRunnable> invokeLater1 = new LinkedBlockingQueue<FutureRunnable>();
	private final BlockingQueue<FutureRunnable> invokeLater2 = new LinkedBlockingQueue<FutureRunnable>();
	//private final Thread laterThread1;
	private final Thread laterThread2;
	private volatile boolean running = true;
	
	
	public Scheduler()
	{
		//laterThread1 = createThread(THREAD_NAME1, invokeLater1);
		laterThread2 = createThread(THREAD_NAME2, invokeLater2);
	}

	private Thread createThread(String name, final BlockingQueue<FutureRunnable> invokeLater) 
	{
		Thread laterThread=new Thread(new Runnable() 
		{
			@Override
			public void run() 
			{
				FutureRunnable runner=null;
				while(running)
				{
					try 
					{
						runner = invokeLater.take();
						runner.run();
					}
					catch (InterruptedException ie)
					{
						if(logger.isDebugEnabled())
						{
							logger.debug("interrupted, check if we need to exit this thread");
						}
					}
					catch (Exception e) 
					{
						logger.error("Exception in runner: "+e.toString());
						e.printStackTrace();
					}
				}
				//those are the ones in the queue that have not yet been processed.
				while((runner=invokeLater.poll())!=null)
				{
					runner.failed("Shutting down...");
				}
			}
		});
		laterThread.start();
		return laterThread;
	}
	
	/*public void sendLater(FutureRunnable runner)
	{
		if(running == false)
		{
			runner.failed("Shutting down...");
			return;
		}
		invokeLater1.offer(runner);
	}*/
	
	public void callLater(FutureRunnable runner)
	{
		if (Thread.currentThread().getName().equals(THREAD_NAME2))
		{
			throw new RuntimeException("This is not a good idea to call callLater() here, since we are already " +
					"coming from a call later. Typically, you will reserve a connection, which " +
					"blocks and will block this thread.");
		}
		if(running == false)
		{
			runner.failed("Shutting down...");
			return;
		}
		invokeLater2.offer(runner);
	}
	
	public void shutdown()
	{
		running = false;
		//laterThread1.interrupt();
		laterThread2.interrupt();
	}
}
