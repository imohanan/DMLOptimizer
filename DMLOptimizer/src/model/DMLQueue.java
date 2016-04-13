package model;

import java.util.List;

public class DMLQueue {
	

	public static DML DMLQueueHead;
	public static DML DMLQueueTail;
	public static int queueSize = 0;


	public static void setDMLQueueHead(DML dMLQueueHead) {
		queueSize++;
		DMLQueueHead = dMLQueueHead;
	}


	public static void setDMLQueueTail(DML dMLQueueTail) {
		queueSize++;
		DMLQueueTail = dMLQueueTail;
	}
	
	public static void AddDML(DML dml) {
		queueSize++;
		if (DMLQueueTail == null || DMLQueueHead == null)
		{
			DMLQueueTail = dml;
			DMLQueueHead = dml;
			return;
		}
		dml.PrevNode = DMLQueueTail;
		DMLQueueTail.NextNode = dml;
		DMLQueueTail = dml;
	}
	
	public static DML RemoveDMLfromHead()
	{
		if (DMLQueueHead == null)
			return null;
		queueSize--;
		DML resultNode = DMLQueueHead;
		if (DMLQueueHead.NextNode != null)
		{
			DMLQueueHead = DMLQueueHead.NextNode;
			DMLQueueHead.PrevNode = null;
		}
		else
		{
			DMLQueueHead = null;
			DMLQueueTail = null;
		}
		resultNode.NextNode = null;
		return resultNode;	
	}
	
	public static void RemoveDML(DML dml)
	{
		queueSize--;
		if (dml.NextNode != null && dml.PrevNode != null) 
		{
			dml.NextNode.PrevNode = dml.PrevNode;
			dml.PrevNode.NextNode = dml.NextNode;
		}
		else if(DMLQueueHead == dml && DMLQueueTail == dml)
		{
			DMLQueueHead = null;
			DMLQueueTail = null;
		}
		else if(DMLQueueHead == dml)
		{
			DMLQueueHead = DMLQueueHead.NextNode;
			DMLQueueHead.PrevNode = null;
		}
		else if(DMLQueueTail == dml)
		{
			DMLQueueTail = DMLQueueTail.PrevNode;
			DMLQueueTail.NextNode = null;
		}
		dml.NextNode = null;
		dml.PrevNode = null;
	}
	
	public static void AddToHead(DML dml)
	{
		queueSize++;
		dml.NextNode = DMLQueueHead;
		DMLQueueHead.PrevNode = dml;
		DMLQueueHead = dml;
	}
	
	public static int getQueueSize(){
		return queueSize;
	}
	
	public static Boolean IsEmpty()
	{
		if (DMLQueueTail == null)
			return true;
		return false;
	}
}
