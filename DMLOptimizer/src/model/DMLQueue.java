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
		DML.combcounter--;
		
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
	
	public static DML getMin(List<DML> dmls){
		//Return the DML from List which is the next one to get fired based on order in DMLQUEUE.
		DML dml=null;
		return dml;
	}
	public static DML getMinAndRemove(List<DML> dmls){
		//Return the DML from List which is the next one to get fired based on order in DMLQUEUE.
		//Then,remove that dml from DMLQUEUE.
		DML dml=null;
		return dml;
	}
	public static int getIndex(DML dml){
		//Return the index of dml in DMLQUEUE.Return Null if you could not find it.
		return 0;
	}
	public static boolean removeDML(DML dml){
		//Remove the dml from DMLQUEUE. Return ture if successful, false if unsuccessful.
		return true;
	}
	public static void AddToHead(DML dml)
	{
		dml.NextNode = DMLQueueHead;
		DMLQueueHead.PrevNode = dml;
		DMLQueueHead = dml;
	}
	
	
	public static Boolean IsEmpty()
	{
		if (DMLQueueTail == null)
			return true;
		return false;
	}
}
