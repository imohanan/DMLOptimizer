package util;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;


public class Utilization {



public static class OSStatThread extends Thread {
	String cmd = "typeperf \"\\Memory\\Available bytes\" \"\\processor(_total)\\% processor time\" \"\\PhysicalDisk(_Total)\\Avg. Disk Write Queue Length\" \"\\Network Interface(*)\\Bytes Total/sec\" -sc 1";
	boolean end = false;
	double availableMem = 0.0;
	double cpuTime = 0.0;
	double avgQLength = 0.0;
	double netwrokBytesPerSec = 0.0;
	PrintWriter _outPrint = null;

	public OSStatThread(PrintWriter outPrint) {
		_outPrint = outPrint;
	}

	public void setEnd() {
		end = true;
	}

	public void run() {
		while (!end) {
			try {
				int cnt = 0;
				Process osStats = Runtime.getRuntime().exec(cmd);
				InputStream stdout = osStats.getInputStream();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(stdout));
				String osline = "";
				while ((osline = reader.readLine()) != null) {
					
					cnt++;
					
					// parse the line and send to coord
					if (cnt == 3) {
//						 System.out.println ("OSS: "+ osline);
						String memory,cpu,network,disk;
						String[] stats = osline.split(",");
						double mem=Double.parseDouble(stats[1].substring(stats[1].indexOf("\"") + 1,stats[1].lastIndexOf("\"")));
						mem=mem/(1024*1024);
						memory="Available MEM(MB):"+ mem + ",";
						_outPrint.print("MEM:"+ stats[1].substring(stats[1].indexOf("\"") + 1,stats[1].lastIndexOf("\"")) + " ");						
						_outPrint.flush();
						cpu="CPU:"+ stats[2].substring(stats[2].indexOf("\"") + 1,stats[2].lastIndexOf("\"")) + ",";
						_outPrint.print("CPU:"+ stats[2].substring(stats[2].indexOf("\"") + 1,stats[2].lastIndexOf("\"")) + " ");						
						_outPrint.flush();
						disk="DISK:"+ stats[3].substring(stats[3].indexOf("\"") + 1,stats[3].lastIndexOf("\"")) + ",";
						_outPrint.print("DISK:"+ stats[3].substring(stats[3].indexOf("\"") + 1,stats[3].lastIndexOf("\"")) + " ");						
						_outPrint.flush();
						double net=Double.parseDouble(stats[4].substring(stats[4].indexOf("\"") + 1,stats[4].lastIndexOf("\"")));
						net=net/(1024*1024);
						network="NTBW(MB/sec):"+ net + ",";
						_outPrint.println("NTBW:"+ stats[4].substring(stats[4].indexOf("\"") + 1,stats[4].lastIndexOf("\"")) + " ");
						_outPrint.flush();
						String line="OS="+cpu+memory+network+disk;
//						System.out.println(line);
					}
				}

				osStats.waitFor();
				if (osStats != null)
					osStats.destroy();
				Thread.sleep(10000);
			} catch (IOException e) {
				e.printStackTrace(System.out);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
		}
	}

}


class OSStatThreadLinux extends OSStatThread {
	public static final String NET_INTERFACE_NAME = "bond0";
	boolean end = false;
	double availableMem = 0.0;
	double cpuTime = 0.0;
	double avgQLength = 0.0;
	double netwrokBytesPerSec = 0.0;
	PrintWriter _outPrint = null;
	String cpuCommand= " sar -P ALL 1 1 ";
	String netCommand="  sar -n DEV 1 1  ";
	String memCommand=" sar -r 1 1 ";
	String diskCommand="sar -d 1 1 ";

	public OSStatThreadLinux(PrintWriter outPrint) {
		super(outPrint);
	}

	public void setEnd() {
		end = true;
	}

	public void run() {
		while (!end) {
			try {
				
				
				
				
				
				Process osStatsCPU = Runtime.getRuntime().exec(cpuCommand);
				Process osStatsMem = Runtime.getRuntime().exec(memCommand);
				Process osStatsDisk = Runtime.getRuntime().exec(diskCommand);
				Process osStatsNet = Runtime.getRuntime().exec(netCommand);
				
				InputStream stdout = osStatsCPU.getInputStream();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(stdout));
				String line = "";
				String osline="";
				line = reader.readLine();
				line = reader.readLine();
				line = reader.readLine();
				double cpui= 100;
				while ((line = reader.readLine()) != null) {
					if (line.equals("")){
						break;
					}
					osline=line;
					String[] cpuLine = osline.split("\\s+");
					 cpui = Math.min(cpui,Double.parseDouble(cpuLine[8]));
				}
			
				
				cpui = 100 - cpui;
				
				
				stdout = osStatsMem.getInputStream();
				reader = new BufferedReader(
						new InputStreamReader(stdout));
			
				
				while ((line = reader.readLine()) != null) {
					osline=line;
				}
				String[] memLine = osline.split("\\s+");
				double memi = Double.parseDouble(memLine[1])/1024;
				
				
				stdout = osStatsDisk.getInputStream();
				reader = new BufferedReader(
						new InputStreamReader(stdout));
			
				
				while ((line = reader.readLine()) != null) {
					osline=line;
				}
				String[] diskLine = osline.split("\\s+");
				
				stdout = osStatsNet.getInputStream();
				reader = new BufferedReader(
						new InputStreamReader(stdout));
			
				
				while ((line = reader.readLine()) != null) {
					if (!line.equals("")){
					if (line.split("\\s+")[2].compareTo(NET_INTERFACE_NAME)==0){
					osline=line;
					break;
					}
					}
				}
				String[] netLine = osline.split("\\s+");
				
				double rx = Double.parseDouble(netLine[5]);
				double tx = Double.parseDouble(netLine[6]);
				
				double neti=(rx + tx)/1024;
			
				
				
				
				
					// parse the line and send to coord
				
//						 System.out.println ("OSS: "+ osline);
						String memory,cpu,network,disk;
						memory="Available MEM(MB):"+ memi + ",";
						cpu="CPU:"+ cpui + ",";
						disk="DISK:"+diskLine[6] + ",";
						network="NTBW(MB/sec):"+ neti + ",";
						
				
						line="OS="+cpu+memory+network+disk;
						System.out.println(line);				
				

				
				if (osStatsCPU != null)
					osStatsCPU.destroy();
				if (osStatsNet != null)
					osStatsNet.destroy();
				if (osStatsDisk != null)
					osStatsDisk.destroy();
				if (osStatsMem != null)
					osStatsMem.destroy();
				Thread.sleep(10000);
			} catch (IOException e) {
				e.printStackTrace(System.out);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
		}
	}

}
}