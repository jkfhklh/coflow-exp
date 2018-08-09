package coflow.events;

import coflow.Scheduler;
import coflow.model.BvN;
import coflow.model.Coflow;
import coflow.model.Job;

public abstract class Event {

	public abstract void run();
	
	protected void updateBvnSlice(){
		Coflow coflow = Scheduler.topologicalCoflows[Scheduler.currentCoflow];
		BvN bvn = new BvN();
		bvn.decompose(coflow.bytes);
		int k = bvn.correctedP.size();
		int n = coflow.bytes.length;
		Scheduler.bvnMatrix = new int[k][n][n];
		Scheduler.bvnWeight = new double[k];
		for(int i=0;i<k;++i){
			Scheduler.bvnMatrix[i] = bvn.correctedP.get(i);
			Scheduler.bvnWeight[i] = bvn.correctedAlpha.get(i)/(double)bvn.multiples*bvn.maximumRowAndColumuSums;
		}
		Scheduler.currentBvn = 0;
	}
	
	protected void backfillingBvnSlice(){ // O(n^2+nm^2)
		// update states for current bvn slice
		int[][] bvn = Scheduler.bvnMatrix[Scheduler.currentBvn];
		int n = bvn.length;
		
		Scheduler.currentBvnRx = new int[n];
		Scheduler.currentBvnTx = new int[n];
		Scheduler.currentBvnCoflow = new int[n];
		for(int i=0;i<n;++i){
			Scheduler.currentBvnRx[i] = -1;
			Scheduler.currentBvnTx[i] = -1;
			Scheduler.currentBvnCoflow[i] = -1;
		}
		
		// if it dependent coflows haven't finished their computation phase, just backfilling
		if(Scheduler.topologicalCoflows[Scheduler.currentCoflow].beginTime<=Scheduler.currentTime){
			for(int i=0;i<n;++i){
				for(int j=0;j<n;++j){
					if(bvn[i][j]==1){
						Scheduler.currentBvnTx[j] = i;
						Scheduler.currentBvnRx[i] = j;
						Scheduler.currentBvnCoflow[i] = Scheduler.currentCoflow;
						break;
					}
				}
			}
		}
		// backfilling
		for(int i=0;i<n;++i){
			// find a free tx-rx pair
			if(Scheduler.currentBvnRx[i] != -1)
				continue;
			int rnd = Scheduler.random.nextInt(n);
			int tx = i, rx = -1;
			for(int j=0;j<n;++j){
				rx = (j+rnd)%n;
				if(Scheduler.currentBvnTx[rx] == -1)
					break;
			}
			// find a backfilling flow
			for(int j = Scheduler.currentCoflow+1; j<Scheduler.topologicalCoflows.length; ++j){
				// check size
				Coflow coflow = Scheduler.topologicalCoflows[j];
				if(coflow.finished || coflow.bytes[tx][rx]<1e-8){
					coflow.bytes[tx][rx] = 0;
					continue;
				}
				// check dependency
				/*boolean depend = false;
				for(int k=i;k<j;++k)
					depend = depend || Scheduler.dependencies[k][j];*/
				boolean depend = Scheduler.dependencies[Scheduler.currentCoflow][j];
				if(depend)
					continue;
				// check computation delay
				if(coflow.beginTime>Scheduler.currentTime)
					continue;
				
				// update states
				Scheduler.currentBvnTx[rx] = tx;
				Scheduler.currentBvnRx[tx] = rx;
				Scheduler.currentBvnCoflow[tx] = j;		
				break;
			}
		}
	}
	
	protected void transmit(){
		// get size to transmit
		int n = Scheduler.currentBvnRx.length;
		// check whether current coflow finished
		boolean allBf = true;
		for(int i=0;i<n;++i){
			if(Scheduler.currentBvnCoflow[i] == Scheduler.currentCoflow){
				allBf = false;
				break;
			}
		}
		double size = Scheduler.bvnWeight[Scheduler.currentBvn];
		if(allBf) size = Double.POSITIVE_INFINITY;
		for(int i=0;i<n;++i){ // i:tx
			if(Scheduler.currentBvnCoflow[i] == Scheduler.currentCoflow ||
					Scheduler.currentBvnCoflow[i] == -1)
				continue;
			Coflow coflow = Scheduler.topologicalCoflows[Scheduler.currentBvnCoflow[i]];
			int tx = i, rx = Scheduler.currentBvnRx[i];
			size = Math.min(size, coflow.bytes[tx][rx]);
		}
		// infty means link is empty, we sleep for some time 
		if(Double.isInfinite(size)) size = 0.1; // skip 0.1s, smaller makes performance better but runs slower
		boolean preemption = false;
		double slack = Scheduler.nextPreemptionTime() - Scheduler.currentTime;
		if(slack-size<1e-8){//slack<=size
			size = Math.min(size,slack);
			preemption = true;
		}
		// check whether backfilling flow finished
		for(int i=0;i<n;++i){ // update backfilling flows
			if(Scheduler.currentBvnCoflow[i] == Scheduler.currentCoflow ||
					Scheduler.currentBvnCoflow[i] == -1)
				continue;
			Coflow coflow = Scheduler.topologicalCoflows[Scheduler.currentBvnCoflow[i]];
			int tx = i, rx = Scheduler.currentBvnRx[i];
			coflow.bytes[tx][rx] -= size;
			if(coflow.bytes[tx][rx] < 1e-8){ // backfilling flow finished
				coflow.bytes[tx][rx] = 0;
				boolean coflowFinished = true;// check whether backfilling coflow finished
				for(int ii=0;ii<n;++ii){
					for(int jj=0;jj<n;++jj){
						if(coflow.bytes[ii][jj]>=1e-8){
							coflowFinished = false;
							break;
						}
					}
					if(!coflowFinished)
						break;
				}
				if(coflowFinished){
					coflow.finished = true;
					Scheduler.ccts.add(new Scheduler.Item(Scheduler.trafficId+"@"+coflow.job.index+"@"+coflow.index, coflow.totalSize, Scheduler.currentTime, coflow.job.weight));
					
					for(int j=0;j<Scheduler.topologicalCoflows.length;++j)
						if(Scheduler.dependencies[Scheduler.currentCoflow][j])
							Scheduler.topologicalCoflows[j].beginTime = Math.max(Scheduler.topologicalCoflows[j].beginTime, Scheduler.currentTime + coflow.cd);
					Job job = coflow.job;
					if(isJobFinished(job)){ 
						job.completionTime = Scheduler.currentTime+size;
						job.finished = true;
						Scheduler.jcts.add(new Scheduler.Item(Scheduler.trafficId+"@"+coflow.job.index, job.totalSize, Scheduler.currentTime, job.weight));
					}
				}	
			}
		}
		Scheduler.currentBvnSize = size;
		if(Scheduler.testbedOutput) {
			// time coflow tx rx size
			double time = Scheduler.currentTime;
			for(int i=0;i<n;++i) {
				int tx = i;
				int rx = Scheduler.currentBvnRx[tx];
				if(rx == -1) continue;
				int coflow = Scheduler.currentBvnCoflow[tx];
				int job = Scheduler.topologicalCoflows[coflow].job.index;
				Scheduler.cout.println(time + " " + job + " " + tx + " " + rx + " " + size);
			}
		}
		

		if(!allBf)
			Scheduler.bvnWeight[Scheduler.currentBvn] -= size;
		for(int i=0;i<n;++i){ // update backfilling flows
			if(Scheduler.currentBvnCoflow[i] != Scheduler.currentCoflow)
				continue;
			Coflow coflow = Scheduler.topologicalCoflows[Scheduler.currentBvnCoflow[i]];
			int tx = i, rx = Scheduler.currentBvnRx[i];
			coflow.bytes[tx][rx] -= size;
			if(coflow.bytes[tx][rx]<1e-8)
				coflow.bytes[tx][rx] = 0;
		}
		if(Scheduler.bvnWeight[Scheduler.currentBvn] < 1e-8){ // current bvn finished
			Scheduler.bvnWeight[Scheduler.currentBvn] = 0;
			++ Scheduler.currentBvn;

			if(Scheduler.currentBvn == Scheduler.bvnWeight.length){ // last bvn in a coflow: current coflow finished
				Coflow coflow = Scheduler.topologicalCoflows[Scheduler.currentCoflow];
				coflow.finished = true;
				Scheduler.ccts.add(new Scheduler.Item(Scheduler.trafficId+"@"+coflow.job.index+"@"+coflow.index, coflow.totalSize, Scheduler.currentTime, coflow.job.weight));
				for(int i=0;i<Scheduler.topologicalCoflows.length;++i)
					if(Scheduler.dependencies[Scheduler.currentCoflow][i])
						Scheduler.topologicalCoflows[i].beginTime = Math.max(Scheduler.topologicalCoflows[i].beginTime, Scheduler.currentTime + coflow.cd);
				
				Job job = coflow.job;
				if(isJobFinished(job)){ 
					job.completionTime = Scheduler.currentTime+size;
					job.finished = true;
					Scheduler.jcts.add(new Scheduler.Item(Scheduler.trafficId+"@"+coflow.job.index, job.totalSize, Scheduler.currentTime, job.weight));
				}
				
				if(preemption)
					return;

				do{
					++Scheduler.currentCoflow;
				}while(Scheduler.currentCoflow<Scheduler.topologicalCoflows.length && Scheduler.topologicalCoflows[Scheduler.currentCoflow].finished);
				if(Scheduler.currentCoflow<Scheduler.topologicalCoflows.length){
					updateBvnSlice();
					EBvnFinished event = new EBvnFinished();
					Scheduler.putEvent(Scheduler.currentTime+size, event);
				}
			}else{
				if(preemption)
					return;
				EBvnFinished event = new EBvnFinished();
				Scheduler.putEvent(Scheduler.currentTime+size, event);
			}
		}else{
			if(preemption)
				return;
			EBvnFinished event = new EBvnFinished();
			Scheduler.putEvent(Scheduler.currentTime+size, event);
		}
		
	}
	
	private boolean isJobFinished(Job job){
		for(Coflow c:job.coflows)
			if(!c.finished) 
				return false;
		return true;
	}
}
