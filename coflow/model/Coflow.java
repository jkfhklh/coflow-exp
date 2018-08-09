package coflow.model;

import java.util.Comparator;

import coflow.Config;

public class Coflow implements Comparator<Coflow>{
	public double[][] bytes = null;
	public Job job = null;
	public int index = 0;
	public int cd = 0;
	public double beginTime = 0;
	public double totalSize = 0;
	
	public boolean released = false;
	public boolean finished = false;
	
	public Coflow(Job job, int index){
		this.job = job;
		this.index = index;
		this.bytes = new double[Config.numOfMachines][Config.numOfMachines];
	}
	
	@Override
	public int compare(Coflow o1, Coflow o2) {
		return job.compare(o1.job, o2.job);
	}
}
