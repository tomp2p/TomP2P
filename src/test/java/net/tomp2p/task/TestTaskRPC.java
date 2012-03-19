package net.tomp2p.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import net.tomp2p.futures.FutureAsyncTask;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage;
import net.tomp2p.task.TaskStatus;
import net.tomp2p.task.Worker;
import net.tomp2p.utils.Utils;

import org.junit.Test;

public class TestTaskRPC
{
	final private static Random rnd = new Random(42L);
	
	@Test
	public void testRPC1() throws Exception
	{
		Peer peer1 = null;
		Peer peer2 = null;
		try
		{
			peer1 = new Peer(new Number160(rnd), 1);
			peer1.listen(4001, 4001);
			peer2 = new Peer(new Number160(rnd), 1);
			peer2.listen(4002, 4002);
			FutureChannelCreator futureChannelCreator = peer1.getConnectionBean().getConnectionReservation().reserve(1);
			futureChannelCreator.awaitUninterruptibly();
			Number160 taskId = new Number160(11);
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			dataMap.put(new Number160(22), new Data("testme"));
			FutureResponse futureResponse = peer1.getTaskRPC().sendTask(peer1.getPeerAddress(), futureChannelCreator.getChannelCreator(), 
					taskId, dataMap, new MyWorker1(), null, false, false);
			Utils.addReleaseListenerAll(futureResponse, peer1.getConnectionBean().getConnectionReservation(), futureChannelCreator.getChannelCreator());
			futureResponse.awaitUninterruptibly();
			Assert.assertEquals(true, futureResponse.isSuccess());
			Thread.sleep(1000);
		}
		finally
		{
			if(peer1!=null)
			{
				peer1.shutdown();
			}
			if(peer2!=null)
			{
				peer2.shutdown();
			}
		}
	}
	
	@Test
	public void testRPC2() throws Exception
	{
		MapReducePeer peer1 = null;
		MapReducePeer peer2 = null;
		try
		{
			peer1 = new MapReducePeer(new Number160(rnd), 1);
			peer1.listen(4001, 4001);
			peer2 = new MapReducePeer(new Number160(rnd), 1);
			peer2.listen(4002, 4002);
			FutureChannelCreator futureChannelCreator = peer1.getConnectionBean().getConnectionReservation().reserve(5);
			futureChannelCreator.awaitUninterruptibly();
			Number160 taskId1 = new Number160(11);
			Number160 taskId2 = new Number160(12);
			Number160 taskId3 = new Number160(13);
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			dataMap.put(new Number160(22), new Data("testme"));
			FutureResponse futureResponse1 = peer1.getTaskRPC().sendTask(peer1.getPeerAddress(), futureChannelCreator.getChannelCreator(), 
					taskId1, dataMap, new MyWorker2(), null, false, false);
			futureResponse1.awaitUninterruptibly();
			FutureResponse futureResponse2 = peer1.getTaskRPC().sendTask(peer1.getPeerAddress(), futureChannelCreator.getChannelCreator(), 
					taskId2, dataMap, new MyWorker2(), null, false, false);
			futureResponse2.awaitUninterruptibly();
			FutureResponse futureResponse3 = peer1.getTaskRPC().sendTask(peer1.getPeerAddress(), futureChannelCreator.getChannelCreator(), 
					taskId3, dataMap, new MyWorker2(), null, false, false);
			futureResponse3.awaitUninterruptibly();
			Assert.assertEquals(true, futureResponse1.isSuccess());
			Assert.assertEquals(true, futureResponse2.isSuccess());
			Assert.assertEquals(true, futureResponse3.isSuccess());
			Collection<Number160> taskIDs = new ArrayList<Number160>();
			taskIDs.add(taskId1);
			taskIDs.add(taskId2);
			taskIDs.add(taskId3);
			FutureResponse futureResponse4 = peer1.getTaskRPC().taskStatus(peer1.getPeerAddress(), futureChannelCreator.getChannelCreator(), 
					taskIDs, false);
			futureResponse4.awaitUninterruptibly();
			Assert.assertEquals(3, futureResponse4.getResponse().getDataMap().size());
			Map<Number160, Data> map = futureResponse4.getResponse().getDataMap();
			TaskStatus status1 = (TaskStatus) map.get(taskId1).getObject();
			TaskStatus status2 = (TaskStatus) map.get(taskId2).getObject();
			TaskStatus status3 = (TaskStatus) map.get(taskId3).getObject();
			Assert.assertEquals(TaskStatus.Status.STARTED, status1.getStatus());
			Assert.assertEquals(TaskStatus.Status.QUEUE, status2.getStatus());
			Assert.assertEquals(0, status2.getQueuePosition());
			Assert.assertEquals(TaskStatus.Status.QUEUE, status3.getStatus());
			Assert.assertEquals(1, status3.getQueuePosition());
			Thread.sleep(1000);
			FutureResponse futureResponse5 = peer1.getTaskRPC().taskStatus(peer1.getPeerAddress(), futureChannelCreator.getChannelCreator(), 
					taskIDs, false);
			Utils.addReleaseListenerAll(futureResponse5, peer1.getConnectionBean().getConnectionReservation(), futureChannelCreator.getChannelCreator());
			futureResponse5.awaitUninterruptibly();
			Assert.assertEquals(3, futureResponse5.getResponse().getDataMap().size());
			map = futureResponse5.getResponse().getDataMap();
			status1 = (TaskStatus) map.get(taskId1).getObject();
			status2 = (TaskStatus) map.get(taskId2).getObject();
			status3 = (TaskStatus) map.get(taskId3).getObject();
			Assert.assertEquals(TaskStatus.Status.SUCCESS_RESULT_SENT, status1.getStatus());
			Assert.assertEquals(TaskStatus.Status.SUCCESS_RESULT_SENT, status2.getStatus());
			Assert.assertEquals(TaskStatus.Status.SUCCESS_RESULT_SENT, status3.getStatus());	
		}
		finally
		{
			if(peer1!=null)
			{
				peer1.shutdown();
			}
			if(peer2!=null)
			{
				peer2.shutdown();
			}
		}
	}
	
	@Test
	public void testRPCAsync() throws Exception
	{
		MapReducePeer peer1 = null;
		MapReducePeer peer2 = null;
		try
		{
			peer1 = new MapReducePeer(new Number160(rnd), 1);
			peer1.listen(4001, 4001);
			peer2 = new MapReducePeer(new Number160(rnd), 1);
			peer2.listen(4002, 4002);
			FutureChannelCreator futureChannelCreator = peer1.getConnectionBean().getConnectionReservation().reserve(1);
			futureChannelCreator.awaitUninterruptibly();
			Number160 taskId = new Number160(11);
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			dataMap.put(new Number160(22), new Data("testme"));
			FutureAsyncTask futureAsyncTask = peer1.getAsyncTask().submit(peer2.getPeerAddress(), 
					futureChannelCreator.getChannelCreator(), taskId, dataMap, new MyWorker3(), false, false);
			Utils.addReleaseListenerAll(futureAsyncTask, peer1.getConnectionBean().getConnectionReservation(), futureChannelCreator.getChannelCreator());
			futureAsyncTask.awaitUninterruptibly();
			Assert.assertEquals(true, futureAsyncTask.isSuccess());
			Assert.assertEquals("yup", futureAsyncTask.getDataMap().get(Number160.ONE).getObject());
		}
		finally
		{
			if(peer1!=null)
			{
				peer1.shutdown();
			}
			if(peer2!=null)
			{
				peer2.shutdown();
			}
		}
	}
	
	@Test
	public void testRPCAsyncFailed() throws Exception
	{
		MapReducePeer peer1 = null;
		MapReducePeer peer2 = null;
		try
		{
			peer1 = new MapReducePeer(new Number160(rnd), 1);
			peer1.listen(4001, 4001);
			peer2 = new MapReducePeer(new Number160(rnd), 1);
			peer2.listen(4002, 4002);
			FutureChannelCreator futureChannelCreator = peer1.getConnectionBean().getConnectionReservation().reserve(1);
			futureChannelCreator.awaitUninterruptibly();
			Number160 taskId = new Number160(11);
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			dataMap.put(new Number160(22), new Data("testme"));
			FutureAsyncTask futureAsyncTask = peer1.getAsyncTask().submit(peer2.getPeerAddress(), 
					futureChannelCreator.getChannelCreator(), taskId, dataMap, new MyWorker3(), false, false);
			Utils.addReleaseListenerAll(futureAsyncTask, peer1.getConnectionBean().getConnectionReservation(), futureChannelCreator.getChannelCreator());
			peer2.shutdown();
			futureAsyncTask.awaitUninterruptibly();
			Assert.assertEquals(false, futureAsyncTask.isSuccess());
		}
		finally
		{
			if(peer1!=null)
			{
				peer1.shutdown();
			}
		}
	}
	
}
class MyWorker1 implements Worker
{
	private static final long serialVersionUID = -4738180600791265774L;

	@Override
	public Map<Number160, Data> execute(Map<Number160, Data> inputData, Storage storage)
			throws Exception
	{
		System.out.println("executed!");
		return null;
	}
}
class MyWorker2 implements Worker
{
	private static final long serialVersionUID = -4738180600791265774L;

	@Override
	public Map<Number160, Data> execute(Map<Number160, Data> inputData, Storage storage)
			throws Exception
	{
		System.out.println("executed, now waiting 250msec");
		Thread.sleep(250);
		return null;
	}
}
class MyWorker3 implements Worker
{
	private static final long serialVersionUID = -4738180600791265774L;

	@Override
	public Map<Number160, Data> execute(Map<Number160, Data> inputData, Storage storage)
			throws Exception
	{
		System.out.println("executed, now waiting 1250msec");
		Thread.sleep(1250);
		Map<Number160, Data> outputData = new HashMap<Number160, Data>();
		outputData.put(Number160.ONE, new Data("yup"));
		return outputData;
	}
}