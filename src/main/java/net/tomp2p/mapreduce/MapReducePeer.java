package net.tomp2p.mapreduce;


import net.tomp2p.connection.ConnectionHandler;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.Number160;

public class MapReducePeer extends Peer
{
	private final int workerThreads;
	private AsyncTask asyncTask;
	private TaskRPC taskRPC;
	
	public MapReducePeer(Number160 nodeId, int workerThreads)
	{
		super(nodeId);
		this.workerThreads = workerThreads;
	}
	@Override
	protected void init(ConnectionHandler connectionHandler, Statistics statistics)
	{
		super.init(connectionHandler, statistics);
		// create task manager
		getPeerBean().setTaskManager(new TaskManager(getPeerBean(), getConnectionBean(), workerThreads));
		taskRPC = new TaskRPC(getPeerBean(), getConnectionBean());
		getPeerBean().getTaskManager().init(taskRPC);
		asyncTask = new AsyncTask(taskRPC, getConnectionBean().getScheduler(), getPeerBean());
		getPeerBean().getTaskManager().addListener(asyncTask);
		getConnectionBean().getScheduler().startTracking(taskRPC, getConnectionBean().getConnectionReservation());
	}
	
	public TaskRPC getTaskRPC()
	{
		if(taskRPC == null)
		{
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		}
		return taskRPC;
	}
	
	public AsyncTask getAsyncTask()
	{
		if(asyncTask == null)
		{
			throw new RuntimeException("Not listening to anything. Use the listen method first");
		}
		return asyncTask;
	}
}