package net.tomp2p.mapreduce;

import java.io.Serializable;

public class TaskStatus implements Serializable
{
	private static final long serialVersionUID = 566212788378552194L;
	enum Status {QUEUE, STARTED, SUCCESS_RESULT_NOT_SENT, SUCCESS_RESULT_SENT, FAILED}
	private String faildeReason;
	private Status status;
	private int queuePosition;
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
}