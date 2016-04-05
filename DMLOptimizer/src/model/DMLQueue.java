package model;

import java.util.List;

public class DMLQueue {
	

	private static DML DMLQueueHead;
	private static DML DMLQueueTail;

	public static DML getDMLQueueHead() {
		return DMLQueueHead;
	}

	public static void setDMLQueueHead(DML dMLQueueHead) {
		DMLQueueHead = dMLQueueHead;
	}

	public static DML getDMLQueueTail() {
		return DMLQueueTail;
	}

	public static void setDMLQueueTail(DML dMLQueueTail) {
		DMLQueueTail = dMLQueueTail;
	}
	
	public static void AddDML(DML dml) {
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
		dml.NextNode = DMLQueueHead;
		DMLQueueHead.PrevNode = dml;
		DMLQueueHead = dml;
	}
	public static int getQueueSize(){
		return 0;
	}
	
	public static Boolean IsEmpty()
	{
		if (DMLQueueTail == null)
			return true;
		return false;
	}
}
