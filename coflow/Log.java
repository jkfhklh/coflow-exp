package coflow;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class Log {
	
	PrintWriter cout = null;
	
	public Log(String file) throws IOException{
		cout = new PrintWriter(new FileOutputStream(file));
	}
	
	public void log(String str){
		String out = Math.round(Scheduler.currentTime*1000) + " : " + str;  //ms
		System.out.println(out);
		cout.println(out);
		cout.flush();
	}
}
