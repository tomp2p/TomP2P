package net.tomp2p.mapreduce;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.Scheduler;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancellable;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTask implements TaskResultListener
{
	final private static Logger logger = LoggerFactory.getLogger(AsyncTask.class);
	final private TaskRPC taskRPC;
	final private Scheduler scheduler;
	final private PeerBean peerBean;
	final private Map<Number160, FutureAsyncTask> tasks = new ConcurrentHashMap<Number160, FutureAsyncTask>();
	public AsyncTask(TaskRPC taskRPC, Scheduler scheduler, PeerBean peerBean)
	{
		this.taskRPC = taskRPC;
		this.scheduler = scheduler;
		this.peerBean = peerBean;
	}
	
	public FutureAsyncTask submit(final PeerAddress remotePeer, ChannelCreator channelCreator, final Number160 taskId, 
			Map<Number160, Data> dataMap, Worker mapper, boolean forceUDP, boolean sign)
	{
		final FutureAsyncTask futureAsyncTask = new FutureAsyncTask();
		futureAsyncTask.addCancellation(new Cancellable()
		{
			@Override
			public void cancel()
			{
				taskFailed(taskId);
			}
		});
		tasks.put(taskId, futureAsyncTask);
		FutureResponse futureResponse = taskRPC.sendTask(remotePeer, channelCreator, taskId, dataMap, mapper, 
				peerBean.getKeyPair(), forceUDP, sign);
		futureResponse.addListener(new BaseFutureAdapter<FutureResponse>()
		{
			@Override
			public void operationComplete(FutureResponse future) throws Exception
			{
				if(future.isSuccess())
				{
					//keep track of it and poll to see if its still alive
					scheduler.keepTrack(remotePeer, taskId, (TaskResultListener)AsyncTask.this);
				}
				else
				{
					futureAsyncTask.setFailed(future);
				}
			}
		});
		return futureAsyncTask;	
	}

	@Override
	public void taskReceived(Number160 taskId, Map<Number160, Data> dataMap)
	{
		if(logger.isDebugEnabled())
		{
			logger.debug("Task received "+taskId);
		}
		scheduler.stopKeepTrack(taskId);
		FutureAsyncTask futureAsyncTask = tasks.remove(taskId);
		if(futureAsyncTask == null)
		{
			return;
		}
		futureAsyncTask.setDataMap(dataMap);
	}
	
	@Override
	public void taskFailed(Number160 taskId)
	{
		if(logger.isDebugEnabled())
		{
			logger.debug("Task failed "+taskId);
		}
		//this comes from the scheduler
		FutureAsyncTask futureAsyncTask = tasks.remove(taskId);
		if(futureAsyncTask == null)
		{
			return;
		}
		futureAsyncTask.setFailed("polling faild, maybe peer died");
	}
}