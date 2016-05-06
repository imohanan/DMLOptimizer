package util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import com.sun.management.OperatingSystemMXBean;

public class SystemStats {

	OperatingSystemMXBean operatingSystemMXBean = null;
	int availableProcessors;
	long prevUpTime;
	long prevProcessCpuTime;
	Runtime runtime;
	RuntimeMXBean runtimeMXBean;
	util.Utilization.OSStatThread osThread= null;
	
	public SystemStats() throws IOException
	{
		File f=new File("system-stats.txt");
		f.delete();
		f.createNewFile();
		PrintWriter fw = new PrintWriter(f);
		osThread = new util.Utilization.OSStatThread(fw);
		System.out.println("Starting listener");
		osThread.start();
		
		runtime = Runtime.getRuntime();
		operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	    runtimeMXBean = ManagementFactory.getRuntimeMXBean();
	    availableProcessors = operatingSystemMXBean.getAvailableProcessors();
	    prevUpTime = runtimeMXBean.getUptime();
	    prevProcessCpuTime = operatingSystemMXBean.getProcessCpuTime();
	}
	
	public void stop()
	{
		osThread.setEnd();
		runtime.gc();
	    // Calculate the used memory
	    long memory = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used memory is bytes: " + memory);
	    operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	    long upTime = runtimeMXBean.getUptime();
	    long processCpuTime = operatingSystemMXBean.getProcessCpuTime();
	    long elapsedCpu = processCpuTime - prevProcessCpuTime;
	    long elapsedTime = upTime - prevUpTime;

	    double cpuUsage = Math.min(99F, elapsedCpu / (elapsedTime * 10000F * availableProcessors));
	    System.out.println("Java CPU: " + cpuUsage);
	    System.out.println(operatingSystemMXBean.getSystemCpuLoad());
	}
}
