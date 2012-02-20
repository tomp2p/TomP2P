package net.tomp2p.mapreduce;

public class TaskStatus
{
	enum Status {QUEUE, STARTED, SUCCESS, FAILED}
	private String faildeReason;
	private Status status;
	private int queuePosition;
	private int runningTimeSec;
	public String getFaildeReason()
	{
		return faildeReason;
	}
	public void setFaildeReason(String faildeReason)
	{
		this.faildeReason = faildeReason;
	}
	public Status getStatus()
	{
		return status;
	}
	public void setStatus(Status status)
	{
		this.status = status;
	}
	public int getQueuePosition()
	{
		return queuePosition;
	}
	public void setQueuePosition(int queuePosition)
	{
		this.queuePosition = queuePosition;
	}
	public int getRunningTimeSec()
	{
		return runningTimeSec;
	}
	public void setRunningTimeSec(int runningTimeSec)
	{
		this.runningTimeSec = runningTimeSec;
	}
}
