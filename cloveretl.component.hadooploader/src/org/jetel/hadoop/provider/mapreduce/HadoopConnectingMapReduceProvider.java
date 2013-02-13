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
import java.net.URI;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.jetel.hadoop.provider.HadoopConfigurationUtils;
import org.jetel.hadoop.service.mapreduce.HadoopConnectingMapReduceService;
import org.jetel.hadoop.service.mapreduce.HadoopJobReporter;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceConnectionData;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceJob;

/**
 * 
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 9.11.2012
 * @see JobClient
 */
public class HadoopConnectingMapReduceProvider implements HadoopConnectingMapReduceService {

	public static final Log LOGGER = LogFactory.getLog(HadoopConnectingMapReduceProvider.class);
	public static final String JOB_TRACKER_NOT_INIT_MESSAGE = "JobTracker is not initialized.";
	public static final String NO_REDUCER_USED_DEBUG_MESSAGE = "No reducer class set for map/reduce job '%s'. Using default reducer, that does nothing.";
	public static final String NO_COMBINER_USED_DEBUG_MESSAGE = "No combiner class set for map/reduce job '%s'. Not using combiners.";
	public static final String NO_INPUT_FORMAT_USED_DEBUG_MESSAGE = "No input format class set for map/reduce job '%s'. Using default TextInputFormat.";
	public static final String NO_OUTPUT_FORMAT_USED_DEBUG_MESSAGE = "No output format class set for map/reduce job '%s'. Using default TextOutputFormat.";
	public static final String CAST_MESSAGE = "Specified class '%s' does not inherit from %s as required.";
	public static final String CLASS_NOT_FOUND_MESSAGE = "Specified class '%s' could not be found in given JAR file.";
	public static final String OUTPUT_DIR_CLEANED = "Cleaning output directory for Hadoop job %s: %s '%s' has been deleted.";

	public static final String NAMENODE_URL_KEY = "fs.default.name";
	public static final String JOBTRACKER_URL_KEY = "mapred.job.tracker";
	
	private static final String REDUCER_CLASS_PROPERTY = "mapred.reducer.class";
	private static final String MAPPER_CLASS_PROPERTY = "mapred.mapper.class";
	private static final String COMBINER_CLASS_PROPERTY = "mapred.combiner.class";
	private static final String INPUT_FORMAT_CLASS_PROPERTY = "mapred.input.format.class";
	private static final String OUTPUT_FORMAT_CLASS_PROPERTY = "mapred.output.format.class";
	private static final String OUTPUT_KEY_CLASS_PROPERTY = "mapred.output.key.class";
	private static final String OUTPUT_VALUE_CLASS_PROPERTY = "mapred.output.value.class";

	private JobClient client;

	@Override
	public final void connect(HadoopMapReduceConnectionData connData, Properties additionalSettings) throws IOException {
		if (isConnected()) {
			throw new IllegalStateException(DUPLICATED_CONNECT_CALL_MESSAGE);
		}
		if (connData == null) {
			throw new NullPointerException("connData");
		}

		JobConf config = new JobConf(HadoopConfigurationUtils.property2Configuration(additionalSettings));
		if (connData.getUser() != null) {
			config.setUser(connData.getUser());
		}
		config.set(NAMENODE_URL_KEY, String.format(connData.getFsUrlTemplate(), connData.getFsMasterHost(), connData
				.getFsMasterPort()));
		config.set(JOBTRACKER_URL_KEY, connData.getJobtrackerHost() + ":" + connData.getJobtrackerPort());
		client = new JobClient(config);
	}

	@Override
	public boolean isConnected() {
		return client != null;
	}

	@Override
	public String validateConnection() throws IOException {
		if (!isConnected()) {
			throw new IllegalStateException(NOT_CONNECTED_MESSAGE);
		}
		client.getClusterStatus(false).getTaskTrackers();
		client.getFs().getStatus();
		return null;
	}

	@Override
	public void close() throws IOException {
		try {
			if (client != null) {
				client.close();
			}
		} finally {
			client = null;
		}
	}

	@Override
	public HadoopJobReporter sendJob(HadoopMapReduceJob jobDetails, Properties additionalJobSettings) throws IOException {
		if (jobDetails == null) {
			throw new NullPointerException("jobDetails");
		}
		if (client == null) {
			throw new IllegalStateException(NOT_CONNECTED_MESSAGE);
		}

		JobConf job = new JobConf(client.getConf());
		for (String key : additionalJobSettings.stringPropertyNames()) {
			job.set(key, additionalJobSettings.getProperty(key));
		}
		job.setJar(getJarFileURL(jobDetails.getJobJarFile()));
		
		// Don't use classloader, at least for now; Potential memory leak - loaded classes may sustain in memory
		// ClassLoader loader = new URLClassLoader(new URL[] { jobDetails.getJobJarFile() }, getClass().getClassLoader());
		
		if (jobDetails.getJobName() != null) {
			job.setJobName(jobDetails.getJobName());
		}

		job.set(MAPPER_CLASS_PROPERTY, jobDetails.getMapper());
		if (jobDetails.getCombiner() == null) {
			LOGGER.debug(String.format(NO_COMBINER_USED_DEBUG_MESSAGE, jobDetails.getJobName()));
		} else {
			job.set(COMBINER_CLASS_PROPERTY, jobDetails.getCombiner());
		}
		if (jobDetails.getReducer() == null) {
			LOGGER.debug(String.format(NO_REDUCER_USED_DEBUG_MESSAGE, jobDetails.getJobName()));
		} else {
			job.set(REDUCER_CLASS_PROPERTY, jobDetails.getReducer());
		}
		if (jobDetails.getInputFormat() == null) {
			LOGGER.debug(String.format(NO_INPUT_FORMAT_USED_DEBUG_MESSAGE, jobDetails.getJobName()));
		} else {
			job.set(INPUT_FORMAT_CLASS_PROPERTY, jobDetails.getInputFormat());
		}
		if (jobDetails.getOutputFormat() == null) {
			LOGGER.debug(String.format(NO_OUTPUT_FORMAT_USED_DEBUG_MESSAGE, jobDetails.getJobName()));
		} else {
			job.set(OUTPUT_FORMAT_CLASS_PROPERTY, jobDetails.getOutputFormat());
		}
		job.set(OUTPUT_KEY_CLASS_PROPERTY, jobDetails.getOutputKey());
		job.set(OUTPUT_VALUE_CLASS_PROPERTY, jobDetails.getOutputValue());
		if (jobDetails.getNumMappers() != null) {
			job.setNumMapTasks(jobDetails.getNumMappers());
		}
		if (jobDetails.getNumReducers() != null) {
			job.setNumReduceTasks(jobDetails.getNumReducers());
		}
		if (jobDetails.getWorkingDirectory() != null) {
			job.setWorkingDirectory(new Path(jobDetails.getWorkingDirectory()));
		}
		Path outputDirectory = new Path(jobDetails.getOutputDir());
		for (URI inputFile : jobDetails.getInputFiles()) {
			FileInputFormat.addInputPath(job, new Path(inputFile));
		}
		FileOutputFormat.setOutputPath(job, outputDirectory);

		if (jobDetails.isClearOutputPath() && client.getFs().exists(outputDirectory)) {
			boolean isFile = client.getFs().isFile(outputDirectory);
			client.getFs().delete(outputDirectory, true);
			LOGGER.info(String.format(OUTPUT_DIR_CLEANED, job.getJobName(), isFile ? "File" : "Directory",
					outputDirectory.toString()));
		}

		return new HadoopRunningJobReporter(client.submitJob(job));
	}

	private String getJarFileURL(URL jobJarFile) {
		String result = jobJarFile.toString();
		
		// Relative path in absolute URL check taken from java.net.URI.checkPath(URI)
		String protocol = jobJarFile.getProtocol();
		if (protocol != null) {
			String path = jobJarFile.getPath();
			if ((path != null) && ((path.length() > 0) && (path.charAt(0) != '/'))) {
				// Here we really have the relative path in absolute URL ->
				// Remove the protocol with ':' prefix which leaves only the relative path to avoid the
				// java.net.URISyntaxException: Relative path in absolute URI: file:./path/to/mapred.jar
				result = result.substring(protocol.length() + 1);
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	protected static <T> Class<? extends T> loadSubclass(ClassLoader loader, String className, Class<T> supperClass) {
		try {
			return (Class<? extends T>) loader.loadClass(className);
		} catch (ClassCastException ex) {
			throw new IllegalArgumentException(String.format(CAST_MESSAGE, className, supperClass.getName()), ex);
		} catch (ClassNotFoundException ex) {
			throw new IllegalArgumentException(String.format(CLASS_NOT_FOUND_MESSAGE, className), ex);
		}
	}
}