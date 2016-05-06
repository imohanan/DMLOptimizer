package util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


public class VerifyAccuracy {
	//Add the path of the folder where your accuracy results are getting stored.
	//This ones windows path
	public static String folderpath = "C:\\Users\\imohanan\\git\\DMLOptimizer\\DMLOptimizer\\";
	//This ones linux path
	//public static String folderpath = "/home/nidhi/Spring2016/Database/project/DMLOptimizer/DMLOptimizer/";
	
	/*Arguments to this file are (dbname, original, manual, prepared)*/
	public static void main(String[] args) {
		try {
			int length = args.length;
			String [] files = new String[6];
			if(length == 1)
				System.out.println("There aren't enough arguments to compare");
			else {
				String db = args[0];
				for (int i=1; i < length; i++) {
					files[i-1] = getName(args[i],db);
				}
				for (int i=0; i<length -1; i ++)
					for (int j = i+1; j < length-1; j++) {
						File f1 = new File(files[i]);
				        File f2 = new File(files[j]);

				        FileReader fR1 = new FileReader(f1);
				        FileReader fR2 = new FileReader(f2);

				        BufferedReader reader1 = new BufferedReader(fR1);
				        BufferedReader reader2 = new BufferedReader(fR2);

				        String line1 = null;
				        String line2 = null;
				        int flag = 1;
				        while ((flag == 1) && ((line1 = reader1.readLine()) != null)
				                && ((line2 = reader2.readLine()) != null)) {
				            if (!line1.equalsIgnoreCase(line2))
				                flag = 0;
				        }
				        reader1.close();
				        reader2.close();
				        if (flag==1)
				        	System.out.println(args[i+1] + " and " + args[j+1] + " matched!");
				        else 
				        	System.out.println(args[i+1] + " and " + args[j+1] + " didn't match!");
				}
			}		
		}
		catch(Exception e) {
			System.out.println(e);
		}

	}
	public static String getName (String filename, String db){
		String result = "";
		if (filename.toLowerCase().equals("original"))
			result = folderpath + "original_accuracy_" + db + ".txt";
		else if (filename.toLowerCase().equals("manual"))
			result = folderpath + "manual_accuracy_" + db + ".txt";
		else if (filename.toLowerCase().equals("prepared"))
			result = folderpath + "prepared_accuracy_" + db + ".txt";
		return result;			
	}

}
