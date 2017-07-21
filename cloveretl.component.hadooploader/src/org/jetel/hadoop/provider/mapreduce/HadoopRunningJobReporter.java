/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.hadoop.provider.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.jetel.hadoop.service.mapreduce.HadoopCounterKey;
import org.jetel.hadoop.service.mapreduce.HadoopCounterKeyValuePair;
import org.jetel.hadoop.service.mapreduce.HadoopJobReporter;

/**
 * Implementation of {@link HadoopJobReporter} that uses Hadoop API version 0.20.2. Suitable for Hadoop API versions up
 * to 0.22.0. This is just a wrapper around {@link RunningJob} with some additional map/reduce counters logic. 
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 18.12.2012
 * @see RunningJob
 */
public class HadoopRunningJobReporter implements HadoopJobReporter {

	private RunningJob job;

	public HadoopRunningJobReporter(RunningJob status) {
		if (status == null) {
			throw new NullPointerException("status");
		}
		this.job = status;
	}

	@Override
	public String getID() {
		return job.getID().toString();
	}

	@Override
	public String getName() {
		return job.getJobName();
	}

	@Override
	public float setupProgress() throws IOException {
		return job.setupProgress();
	}

	@Override
	public float mapProgress() throws IOException {
		return job.mapProgress();
	}

	@Override
	public float reduceProgress() throws IOException {
		return job.reduceProgress();
	}

	@Override
	public float cleanUpProgress() throws IOException {
		return job.cleanupProgress();
	}

	@Override
	public String getJobState() throws IOException {
		return JobStatus.getJobRunState(job.getJobState());
	}

	@Override
	public boolean isComplete() throws IOException {
		return job.isComplete();
	}

	@Override
	public boolean isSuccessful() throws IOException {
		return job.isSuccessful();
	}

	@Override
	public String getFailureInfo() throws IOException {
		return job.getFailureInfo();
	}

	@Override
	public void kill() throws IOException {
		job.killJob();
	}

	@Override
	public long getCounterValue(HadoopCounterKey counterKey) throws IOException {
		if (counterKey == null) {
			throw new NullPointerException("counterKey");
		}
		return job.getCounters().findCounter(counterKey.getGroupName(), counterKey.getName()).getValue();
	}

	@Override
	public List<HadoopCounterKeyValuePair> getAllCounters() throws IOException {
		List<HadoopCounterKeyValuePair> result = new ArrayList<HadoopCounterKeyValuePair>();
		for (Counters.Group group : job.getCounters()) {
			for (Counter counter : group) {
				result.add(new HadoopCounterKeyValuePair(counter.getName(), group.getName(), counter.getValue()));
			}
		}
		return result;
	}

}
