package coflow;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Random;

import coflow.events.EJobReleased;
import coflow.events.Event;
import coflow.events.PreemptiveEvent;
import coflow.model.Coflow;
import coflow.model.Job;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.TreeMap;

public class Scheduler {
	
	public static int trafficId = 0;
	public static class Item{
		public Item(String id, double size, double time, double weight){ Id = id; Size = size; Time = time; Weight = weight; }
		public String Id;
		public double Size, Time, Weight;
	}
	public static ArrayList<Item> jcts = new ArrayList<Item>();
	public static ArrayList<Item> ccts = new ArrayList<Item>();


	public static boolean testbedOutput = false;
	public static boolean enableDepencency = true;
	public static Log log = null;
	public static PrintWriter cout = null;
	
	// current scheduler evets
	public static double currentTime = 0;
	public static int collisionCounter = 0;
	public static Random random = new Random(0);
	private static TreeMap<Double, Event> events = new TreeMap<Double, Event>();
	private static TreeMap<Double, PreemptiveEvent> pevents = new TreeMap<Double, PreemptiveEvent>();
	public static int numOfReleasedJob = 0;


	// current data structure
	public static ArrayList<Coflow> activeCoflows = new ArrayList<Coflow>(); // active coflow set
	public static boolean[][] dependencies = null; // dependencies of running coflows
	
	public static Coflow[] topologicalCoflows = null; // results of topological sorting for active coflows
	public static int currentCoflow = 0;
	
	public static int[][][] bvnMatrix = null; // bvn slice for current coflow
	public static double[] bvnWeight = null;
	public static int currentBvn = 0;
	public static int[] currentBvnCoflow = null; // (index:tx) set the following 3 property before transmission
	public static int[] currentBvnRx = null;// (index:tx) tx:i rx:currentBvnRx[i]
	public static int[] currentBvnTx = null;// (index:rx) rx:i tx:currentBvnTx[i]
	public static double currentBvnSize = 0;// size to transmit (for current bvn slice)

	public static Job[] jobs = null;
	
	private static boolean dbg_exit = false;
	
	public int totalDelay = 0;

	
	
	private static void init(){
		log = null;
		currentTime = 0;
		collisionCounter = 0;
		random = new Random(0);
		events = new TreeMap<Double, Event>();
		pevents = new TreeMap<Double, PreemptiveEvent>();
		numOfReleasedJob = 0;

		activeCoflows = new ArrayList<Coflow>(); // active coflow set
		dependencies = null; // dependencies of running coflows
		
		topologicalCoflows = null; // results of topological sorting for active coflows
		currentCoflow = 0;
		
		bvnMatrix = null; // bvn slice for current coflow
		bvnWeight = null;
		currentBvn = 0;
		currentBvnCoflow = null; // (index:tx) set the following 3 property before transmission
		currentBvnRx = null;// (index:tx) tx:i rx:currentBvnRx[i]
		currentBvnTx = null;// (index:rx) rx:i tx:currentBvnTx[i]
		currentBvnSize = 0;// size to transmit (for current bvn slice)

		jobs = null;
		dbg_exit = false;
	}
	
	private static void run(String traceFile) throws IOException{
		String logFile = "test.log";
		
		loadTraceFile(traceFile);
		if(dbg_exit)return;
		log = new Log(logFile);

		for(Job job:jobs){
			EJobReleased event = new EJobReleased(job);
			double time = job.releaseTime;
			putEvent(time,event);
		}
		
		while(!events.isEmpty()){
			getEvent().run();
		}
		
		double sum = 0;
		for(Job job:jobs){
			sum += job.completionTime*job.weight;
		}
		System.out.println(sum);
	}
	
	
	public static void main(String[] args) throws IOException {
		//File dir = new File("F:\\我的坚果云\\Theoretical Scheduling\\Matlab\\M30T30A20");
		File dir = new File("F:\\我的坚果云\\Theoretical Scheduling\\Matlab\\newspaa\\M30T30A06");
		
		cout = new PrintWriter(new FileOutputStream("schedule.txt"));
		for(String f:dir.list()){
			if(!f.endsWith(".txt")) continue;
			if(testbedOutput) cout.println("[" + f + "]");
			init();
			run(dir.getAbsolutePath()+"\\"+f);
			if(testbedOutput) cout.println();
			++trafficId;
		}
		cout.close();
		
		PrintWriter cct = new PrintWriter(new FileOutputStream("ccts.txt"));
		for(Item item:ccts)
			cct.println(item.Id + " " + item.Size + " "+item.Time + " " + item.Weight);
		cct.close();
		
		PrintWriter jct = new PrintWriter(new FileOutputStream("jcts.txt"));
		for(Item item:jcts)
			jct.println(item.Id + " " + item.Size + " "+item.Time);
		jct.close();
		
	}
	
	public static void putEvent(double time, Event event){
		while(events.containsKey(time))
			time += (++collisionCounter)*1e-12;
		events.put(time, event);
		if(event instanceof PreemptiveEvent)
			pevents.put(time, (PreemptiveEvent)event);
	}
	
	public static double nextPreemptionTime(){
		if(pevents.isEmpty())
			return Double.POSITIVE_INFINITY;
		Entry<Double, PreemptiveEvent> entry = pevents.firstEntry();
		return entry.getKey();
	}
	
	private static Event getEvent() {
		Entry<Double, Event> entry = events.firstEntry();
		currentTime = entry.getKey();
		Event event = entry.getValue();
		events.remove(currentTime);
		if(event instanceof PreemptiveEvent)
			pevents.remove(currentTime);
		return entry.getValue();
	}
	
	private static void loadTraceFile(String file)throws IOException{
		Scanner cin = new Scanner(new FileInputStream(file));
		int priority = 0;
		boolean firstLine = true;
		while(cin.hasNextLine()){
			String[] elem = getLine(cin);
			if(elem==null) break;
			if(firstLine){
				firstLine = false;
				Config.numOfJobs = Integer.parseInt(elem[0]);
				Config.numOfMachines = Integer.parseInt(elem[1]);
				jobs = new Job[Config.numOfJobs];
			}else{ // 1st line of a job
				int nCoflows = Integer.parseInt(elem[0]);
				int nDependices = Integer.parseInt(elem[1]);
				int releaseTime = Integer.parseInt(elem[2]);
				double weight = Double.parseDouble(elem[3]);
				int idx = priority;
				Job job = new Job(idx, ++priority, releaseTime, nCoflows);
				job.weight = weight;
				jobs[idx] = job;
				double jsize = 0;
				for(int i=0;i<nCoflows;++i){
					int p = 0;
					String[] e = getLine(cin);
					Coflow coflow = new Coflow(job,i);
					double size = 0;
					for(int j=0;j<Config.numOfMachines;++j){
						for(int k=0;k<Config.numOfMachines;++k){
							coflow.bytes[j][k] = Double.parseDouble(e[p++]);
							size += coflow.bytes[j][k];
						}
					}
					coflow.totalSize = size;
					//coflow.cd =random.nextInt(250*2+1);
					job.coflows[i] = coflow;
					jsize += size;
				}
				job.totalSize = jsize;
				/*for(int i=0;i<nDependices;++i){
					String[] e = getLine(cin);
					job.d[Integer.parseInt(e[0])][Integer.parseInt(e[1])] = true;
				}*/
				if(enableDepencency) {
					nDependices = random.nextInt(nCoflows);
					while(nDependices-- > 0) {
						int s=0,t=0;
						while(!(s<t)){
							s = random.nextInt(nCoflows);
							t = random.nextInt(nCoflows);
						}
						job.d[s][t] = true;
					}
				}
			}
		}
		cin.close();
	}
	
	private static String[] getLine(Scanner cin){
		while(cin.hasNextLine()){
			String[] elem = cin.nextLine().split("\\s+");
			if(elem==null || elem.length==0 || elem[0].startsWith("%") || elem[0].length()==0)
				continue;
			return elem;
		}
		return null;
		/*while(cin.hasNextLine()){
			String[] elem = cin.nextLine().split("\\s+");
			if(elem[0].startsWith("%")){
				System.out.println(elem[2]);
				dbg_exit = true;
				return null;
			}
		}
		return null;*/
	}

}
