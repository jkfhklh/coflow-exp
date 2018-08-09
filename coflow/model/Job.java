package coflow.model;
import java.util.Comparator;

public class Job implements Comparator<Job>{
	public int index = 0;
	public boolean[][] d = null; // dependencies
	public Coflow[] coflows = null;
	public int priority = 0;
	public double releaseTime = 0;
	public double weight = 0;
	public double totalSize = 0;
	
	public double completionTime = Double.NaN;
	public boolean released = false;
	public boolean finished = false;

	public Job(int index, int priority, double releaseTime, int numOfCoflows){
		this.index = index;
		this.priority = priority;
		this.releaseTime = releaseTime;
		this.coflows = new Coflow[numOfCoflows];
		this.d = new boolean[numOfCoflows][numOfCoflows];
	}

	@Override
	public int compare(Job o1, Job o2) {
		return o1.priority - o2.priority;
	}
	
	
	
}
