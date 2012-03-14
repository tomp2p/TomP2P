package net.tomp2p.mapreduce;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.TaskStatus.Status;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DigestInfo;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskManager
{
	final private static Logger logger = LoggerFactory.getLogger(TaskManager.class);
	final private  PeerBean peerBean;
	final private ConnectionBean connectionBean;
	final private Object lock = new Object();
	final private ThreadPoolExecutor executor;
	// may grow
	final private Map<Number160, Status> status = new HashMap<Number160, Status>();
	final private Map<Number160, String> exceptions = new HashMap<Number160, String>();
	final private Collection<TaskResultListener> listeners = new ArrayList<TaskResultListener>();
	
	private TaskRPC taskRPC;
	
	private class Task implements Runnable
	{
		private final Number160 taskId;
		private final Worker mapper;
		private final Map<Number160, Data> inputData;
		private final PeerAddress senderAddress;
		private final boolean sign;
		public Task(Number160 taskId, Worker mapper, Map<Number160, Data> inputData, PeerAddress senderAddress, boolean sign)
		{
			this.taskId = taskId;
			this.mapper = mapper;
			this.inputData = inputData;
			this.senderAddress = senderAddress;
			this.sign = sign;
		}
		@Override
		public void run()
		{
			if(logger.isDebugEnabled())
			{
				logger.debug("started task "+taskId+" which came from "+senderAddress);
			}
			synchronized (lock)
			{
				status.put(taskId, Status.STARTED);
			}
			Map<Number160, Data> outputData = null;
			try
			{
				outputData = mapper.execute(inputData, peerBean.getStorage());
			}
			catch (Exception e)
			{
				outputData = null;
				registerException(taskId, e.toString());
			}
			synchronized (lock)
			{
				status.put(taskId, Status.SUCCESS_RESULT_NOT_SENT);
			}
			final Map<Number160, Data> outputData2 = outputData;
			FutureChannelCreator futureChannelCreator = connectionBean.getConnectionReservation().reserve(1);
			futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>()
			{
				@Override
				public void operationComplete(final FutureChannelCreator futureChannelCreator) throws Exception
				{
					if(futureChannelCreator.isSuccess())
					{
						FutureResponse futureResponse = getTaskRPC().sendResult(senderAddress, futureChannelCreator.getChannelCreator(), taskId, outputData2, peerBean.getKeyPair(), false, sign);
						futureResponse.addListener(new BaseFutureAdapter<FutureResponse>()
						{
							@Override
							public void operationComplete(FutureResponse future) throws Exception
							{
								if(future.isSuccess())
								{
									synchronized (lock)
									{
										status.put(taskId, Status.SUCCESS_RESULT_SENT);
									}
								}
								else
								{
									registerException(taskId, "could not send result back");
								}
								connectionBean.getConnectionReservation().release(futureChannelCreator.getChannelCreator());
							}
						});
					}
					else
					{
						registerException(taskId, "could not reserve connection");
					}
				}
			});
		}
	}
	
	public TaskManager(PeerBean peerBean, ConnectionBean connectionBean, int threads)
	{
		this.peerBean = peerBean;
		this.connectionBean = connectionBean;
		this.executor = new ThreadPoolExecutor(threads, threads,
	            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
	}
	
	public void addListener(TaskResultListener taskResultListener)
	{
		listeners.add(taskResultListener);
	}
	
	public void removeListener(TaskResultListener taskResultListener)
	{
		listeners.remove(taskResultListener);
	}
	
	public void notifyListeners(Number160 taskId, Map<Number160, Data> dataMap)
	{
		for(TaskResultListener taskResultListener: listeners)
		{
			taskResultListener.taskReceived(taskId, dataMap);
		}
	}
	
	public void init(TaskRPC taskRPC)
	{
		this.taskRPC = taskRPC;
	}
	
	public TaskRPC getTaskRPC()
	{
		if(taskRPC == null)
		{
			throw new IllegalStateException("init() was not called yet");
		}
		return taskRPC;
	}
	
	public TaskStatus taskStatus(Number160 taskId)
	{
		TaskStatus statusResult = new TaskStatus();
		String exception;
		synchronized (lock)
		{
			exception = exceptions.get(taskId);
		}
		if(exception != null)
		{
			statusResult.setFaildeReason(exception);
			statusResult.setStatus(TaskStatus.Status.FAILED);
			if(logger.isDebugEnabled())
			{
				logger.debug("finished task failed for task with ID "+ taskId);
			}
			return statusResult;
		}
		
		int pos = 0;
		Task taskFound = null;
		for(Runnable runnable: executor.getQueue())
		{
			Task task = (Task)runnable;
			if(task.taskId.equals(taskId))
			{
				taskFound = task;
				break;
			}
			pos++;
		}
		if(taskFound != null)
		{
			statusResult.setQueuePosition(pos);
			statusResult.setStatus(TaskStatus.Status.QUEUE);
			if(logger.isDebugEnabled())
			{
				logger.debug("finished task queue for task with ID "+ taskId);
			}
			return statusResult;
		}
		synchronized (lock)
		{
			statusResult.setStatus(status.get(taskId));
		}
		if(logger.isDebugEnabled())
		{
			logger.debug("finished task status for task with ID "+ taskId);
		}
		return statusResult;
	}

	public int submitTask(Number160 taskId, Worker mapper, Map<Number160, Data> data, PeerAddress senderAddress, boolean sign)
	{
		synchronized (lock)
		{
			status.put(taskId, 	TaskStatus.Status.QUEUE);
		}
		Task task = new Task(taskId, mapper, data, senderAddress, sign);
		executor.execute(task);
		return executor.getQueue().size();
	}
	
	private void registerException(Number160 taskId2, String string)
	{
		synchronized (lock)
		{
			exceptions.put(taskId2, string);
		}
	}

	public DigestInfo digest()
	{
		return new DigestInfo(executor.getQueue().size());
	}
}