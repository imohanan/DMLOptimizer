package test;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import model.DML;
import model.DMLQueue;
import model.DeleteDML;

public class TestDMLQueue {

	
	public static void main(String[] args)
	{
		List<String> DMLStrings = new LinkedList<String>();
		DMLStrings.add("DELETE FROM friendship WHERE inviterid=3 AND inviteeid=9 AND status=1;");
		DMLStrings.add("DELETE FROM friendship WHERE inviterid=16 AND inviteeid=11 AND status=2;");
		DMLStrings.add("DELETE FROM friendship WHERE inviterid=11 AND inviteeid=16 AND status=2;");
		
		Vector<DML> DMLs = new Vector<DML>(3);
		
		for(String dmlString: DMLStrings)
		{
			DML dml = new DeleteDML(dmlString);
			DMLs.addElement(dml);
			DMLQueue.AddDML(dml);
		}
		
		DMLQueue.RemoveDML(DMLs.elementAt(2));
		DMLQueue.RemoveDML(DMLs.elementAt(0));
	}

}
