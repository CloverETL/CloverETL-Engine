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
package org.jetel.component.hadooploader;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.jetel.hadoop.connection.IHadoopMapReduceJobSender;
import org.jetel.hadoop.connection.HadoopMapReduceJobDetails;

/**
 * 
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         &lt;<a href="http://www.javlin.eu">http://www.javlin.eu</a>&gt
 * @version $Revision$, $Date$
 * @created 9.11.2012
 */
public class HadoopMapReduceJobSender implements IHadoopMapReduceJobSender {

	private static Log logger = LogFactory.getLog(HadoopMapReduceJobSender.class);
	public static final String NAME_NODE_ADDRESS_KEY = "fs.default.name";
	public static final String JOB_TRACKER_ADDRESS_KEY = "mapred.job.tracker";

	private Configuration conf;
	private boolean closed = false;

	@Override
	public boolean connect(URI jobTrackerHost, URI nameNodeHost) throws IOException {
		return connect(jobTrackerHost, nameNodeHost, null);
	}

	@Override
	public boolean connect(URI jobTrackerHost, URI nameNodeHost, Properties properties) throws IOException {
		if (jobTrackerHost == null) {
			throw new NullPointerException("jobTrackerHost");
		}
		if (nameNodeHost == null) {
			throw new NullPointerException("nameNodeHost");
		}
		conf = HadoopConfigurationUtil.property2Configuration(properties);
		conf.set(JOB_TRACKER_ADDRESS_KEY, jobTrackerHost.toString());
		conf.set(NAME_NODE_ADDRESS_KEY, nameNodeHost.toString());

		return true;
	}

	@Override
	public String validateConnection() throws IOException {
		if (closed) {
			throw new IllegalStateException("The connection of this instance is closed.");
		}
		if (conf == null) {
			throw new IllegalStateException("Not connected. Call connect first.");
		}
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public void close() throws IOException {
		closed = true;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void sendJob(HadoopMapReduceJobDetails jobDetails) throws IOException {
		if (jobDetails == null) {
			throw new NullPointerException("jobDetails");
		}
		if (closed) {
			throw new IllegalStateException("The connection of this instance is closed.");
		}
		if (conf == null) {
			throw new IllegalStateException("Not connected. Call connect first.");
		}
		JobConf job = new JobConf(conf);
		job.setJar(jobDetails.getJobJarFile().toString());

		if (jobDetails.getJobName() != null) {
			job.setJobName(jobDetails.getJobName());
		}
		if (jobDetails.getUser() != null) {
			job.setUser(jobDetails.getUser());
		}
		try {
			job.setMapperClass((Class<? extends Mapper<?, ?, ?, ?>>) jobDetails.getMapper());
		} catch (ClassCastException ex) {
			throw new IllegalArgumentException("jobDetails.getMapper() must return instance of Mapper.", ex);
		}
		if (jobDetails.getCombiner() == null) {
			logger.info("No combiner set for map/reduce job " + job.getJobName() + ". Using null combiner.");
		} else {
			try {
				job.setCombinerClass((Class<? extends Reducer<?, ?, ?, ?>>) jobDetails.getCombiner());
			} catch (ClassCastException ex) {
				throw new IllegalArgumentException("jobDetails.getCombiner() must return instance of Reducer.", ex);
			}
		}
		if (jobDetails.getReducer() == null) {
			logger.warn("No reducer set for map/reduce job " + job.getJobName()
					+ ". Using default reducer, that does nothing.");
		} else {
			try {
				job.setReducerClass((Class<? extends Reducer<?, ?, ?, ?>>) jobDetails.getReducer());
			} catch (ClassCastException ex) {
				throw new IllegalArgumentException("jobDetails.getReducer() must return instance of Reducer.", ex);
			}
		}
		if (jobDetails.getInputFormat() == null) {
			logger.info("No input format set for map/reduce job " + job.getJobName()
					+ ". Using default TextInputFormat.");
		} else {
			try {
				job.setInputFormat((Class<? extends InputFormat<?, ?>>) jobDetails.getInputFormat());
			} catch (ClassCastException ex) {
				throw new IllegalArgumentException("jobDetails.getInputFormat() must return instance of InputFormat.",
						ex);
			}
		}
		if (jobDetails.getOutputFormat() == null) {
			logger.info("No output format set for map/reduce job " + job.getJobName()
					+ ". Using default TextOutputFormat.");
		} else {
			try {
				job.setOutputFormat((Class<? extends OutputFormat<?, ?>>) jobDetails.getOutputFormat());
			} catch (ClassCastException ex) {
				throw new IllegalArgumentException(
						"jobDetails.getOutputFormat() must return instance of OutputFormat.", ex);
			}
		}

		job.setOutputKeyClass(jobDetails.getOutputKey());
		job.setOutputValueClass(jobDetails.getOutputValue());

		if (jobDetails.getWorkingDirectory() != null) {
			job.setWorkingDirectory(new Path(jobDetails.getWorkingDirectory()));
		}
		for (URI uri : jobDetails.getInputFiles()) {
			FileInputFormat.addInputPath(job, new Path(uri));
		}
		FileOutputFormat.setOutputPath(job, new Path(jobDetails.getOutputFile()));

		JobClient.runJob(job);
	}
}
