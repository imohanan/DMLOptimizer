package model;

public class DMLQueue {
	
	public static DML DMLQueueHead;
	public static DML DMLQueueTail;
	
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
		DMLQueueHead = DMLQueueHead.NextNode;
		DMLQueueHead.PrevNode = null;
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
	

}
