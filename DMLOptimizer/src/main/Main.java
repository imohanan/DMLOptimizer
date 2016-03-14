package main;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Path filePath = Paths.get("./LogFiles/dmls_log.txt");
		Charset charset = Charset.forName("US-ASCII");
		try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
		    String line = null;
		    while ((line = reader.readLine()) != null) {
		        DML dml = new DML(line);
		    }
		} catch (IOException x) {
		    System.err.format("IOException: %s%n", x);
		}
		
	}

}
