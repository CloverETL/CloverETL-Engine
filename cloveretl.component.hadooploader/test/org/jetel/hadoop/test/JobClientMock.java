package org.jetel.hadoop.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

class JobClientMock extends JobClient{
	
	private JobConf subbmitedJob;
	private int closeCallsCount = 0;
	private boolean closeThrows = false;
	
	public void setCloseThrows(boolean closeThrows) {
		this.closeThrows = closeThrows;
	}
	
	public int getCloseCallsCount() {
		return closeCallsCount;
	}
	
	public JobConf getSubbmitedJob() {
		return subbmitedJob;
	}
	
	@Override
	public RunningJob submitJob(JobConf job) throws FileNotFoundException, IOException {
		subbmitedJob = job;
		return new RunningJobMock();
	}
	
	@Override
	public synchronized void close() throws IOException {
		closeCallsCount++;
		if (closeThrows) {
			throw new IOException("some");
		}
	}
}
