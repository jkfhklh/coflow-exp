package coflow.events;

import java.util.TreeSet;

import coflow.Scheduler;
import coflow.model.Coflow;
import coflow.model.Job;

public class EJobReleased extends PreemptiveEvent {

	private Job job = null;
	
	public EJobReleased(Job job){
		this.job = job;
	}
	
	@Override
	public void run() {
		// update active coflows and dependencies
		++Scheduler.numOfReleasedJob;
		job.released = true;
		updateActiveCoflowList();
		boolean[][] G = GetDirectDepencencies();
		Scheduler.dependencies = TransitiveClosure(G);
		
		// update topological list of coflows
		Coflow[] list = topologicalSorting(G);
		stableSorting(list,0,list.length-1);
		Scheduler.topologicalCoflows = list;
		Scheduler.currentCoflow = 0;
		
		// update bvn slice of current coflow
		updateBvnSlice();
		backfillingBvnSlice();
		transmit();
	}
	
	private void updateActiveCoflowList(){ // O(n^2)
		/*for(Coflow c:Scheduler.activeCoflows){ // remove finished coflows
			if(c.finished)
				Scheduler.activeCoflows.remove(c);
		}*/
		Scheduler.activeCoflows.removeIf(c -> c.finished);
		for(Coflow c:job.coflows){ // add new coflows
			c.released = true;
			Scheduler.activeCoflows.add(c);
		}
	}
	
	private boolean[][] GetDirectDepencencies(){ // O(n^2)
		int n = Scheduler.activeCoflows.size();
		boolean[][] G = new boolean[n][n];
		for(int i=0;i<n;++i){ // get initial dependency matrix
			for(int j=0;j<n;++j){
				Coflow ci = Scheduler.activeCoflows.get(i);
				Coflow cj = Scheduler.activeCoflows.get(j);
				if(ci.job != cj.job) G[i][j] = false;
				else G[i][j] = i!=j && ci.job.d[ci.index][cj.index];
			}
		}
		return G;
	}
	
	private boolean[][] TransitiveClosure(boolean[][] H){ // O(n^3)
		int n = H.length;
		boolean[][] G = H.clone();
		for(int k=0;k<n;++k) // solve the transitive closure
			for(int i=0;i<n;++i)
				for(int j=0;j<n;++j)
					G[i][j] = G[i][j] || ( G[i][k] && G[k][j] );
		for(int k=0;k<n;++k){ // check whether input job is a DAG
			if(G[k][k]){
				System.err.println("Job "+ Scheduler.activeCoflows.get(k).job.index + " is not a DAG!");
				System.exit(0);
			}
		}
		return G;
	}
	
	private Coflow[] topologicalSorting(boolean[][] G){ // O(n^2)
		int n = G.length;
		Coflow[] list = new Coflow[n]; // reset existing list
		int[] indegree = new int[n];
		for(int i=0;i<n;++i)
			for(int j=0;j<n;++j)
				if(G[i][j])
					++indegree[j];
		TreeSet<Integer> temp = new TreeSet<Integer>();
		for(int i=0;i<n;++i)
			if(indegree[i]==0)
				temp.add(i);
		int counter = 0;
		while(!temp.isEmpty()){
			int u = temp.pollFirst(); // get and remove
			for(int i=0;i<n;++i)
				if(G[u][i])
					if(--indegree[i] == 0)
						temp.add(i);
			list[counter++] = Scheduler.activeCoflows.get(u);
		}
		return list;
	}
	
	private void stableSorting(Coflow[] list, int l,int r){ // O(n log n)
		if(l>=r) return;
		int c = (l+r)/2;
		stableSorting(list,l,c);
		stableSorting(list,c+1,r);
		Coflow[] temp = new Coflow[list.length];
		int pl = l, pr = c+1, ptemp = l;
		while(pl<=c && pr<=r){
			if(list[pl].compare(list[pl], list[pr])<=0) // pl<=pr
				temp[ptemp++] = list[pl++];
			else 
				temp[ptemp++] = list[pr++];
		}
		while(pl<=c)
			temp[ptemp++] = list[pl++];
		while(pr<=r)
			temp[ptemp++] = list[pr++];
		for(int i=l;i<=r;++i)
			list[i] = temp[i];
	}

}
