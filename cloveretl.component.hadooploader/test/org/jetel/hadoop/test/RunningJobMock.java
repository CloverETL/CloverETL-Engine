package org.jetel.hadoop.test;

import java.io.IOException;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;

class RunningJobMock implements RunningJob{

	@Override
	public JobID getID() {
		return null;
	}

	@Override
	public String getJobID() {
		return null;
	}

	@Override
	public String getJobName() {
		return null;
	}

	@Override
	public String getJobFile() {
		return null;
	}

	@Override
	public String getTrackingURL() {
		return null;
	}

	@Override
	public float mapProgress() throws IOException {
		return 0;
	}

	@Override
	public float reduceProgress() throws IOException {
		return 0;
	}

	@Override
	public float cleanupProgress() throws IOException {
		return 0;
	}

	@Override
	public float setupProgress() throws IOException {
		return 0;
	}

	@Override
	public boolean isComplete() throws IOException {
		return false;
	}

	@Override
	public boolean isSuccessful() throws IOException {
		return false;
	}

	@Override
	public void waitForCompletion() throws IOException {
	}

	@Override
	public int getJobState() throws IOException {
		return 0;
	}

	@Override
	public void killJob() throws IOException {
	}

	@Override
	public void setJobPriority(String priority) throws IOException {
	}

	@Override
	public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom) throws IOException {
		return null;
	}

	@Override
	public void killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
	}

	@Override
	public void killTask(String taskId, boolean shouldFail) throws IOException {
	}

	@Override
	public Counters getCounters() throws IOException {
		return null;
	}

	@Override
	public String getFailureInfo() throws IOException {
		return null;
	}

	@Override
	public String[] getTaskDiagnostics(TaskAttemptID taskid) throws IOException {
		return null;
	}
	
}