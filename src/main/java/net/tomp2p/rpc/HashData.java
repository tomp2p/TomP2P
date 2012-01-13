package net.tomp2p.rpc;

import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class HashData
{
	private Number160 left;
	private Data right;
	
	public HashData(Number160 left, Data right)
	{
		this.left = left;
		this.right = right;
	}
	
	public HashData()
	{
	}
	
	public Number160 getLeft()
	{
		return left;
	}
	
	public HashData setLeft(Number160 left)
	{
		this.left = left;
		return this;
	}
	
	public Data getRight()
	{
		return right;
	}
	
	public HashData setRight(Data right)
	{
		this.right = right;
		return this;
	}
}
