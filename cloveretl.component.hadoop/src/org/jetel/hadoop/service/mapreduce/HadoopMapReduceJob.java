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
package org.jetel.hadoop.service.mapreduce;

import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.jetel.hadoop.mapreduce.HadoopJobRunner;

/**
 * <p>Represents basic configuration of Hadoop map/reduce job. Holds all the information that mandatory for the job to
 * be submitted to a jobtracker and executed on Hadoop cluster. In addition, it can store some common job settings like
 * name of the map/reduce job.</p>
 * 
 * <p> This is a bean class that does not contain any considerable logic nor is it intended to. All attributes are
 * initialized in constructor and are made available by call to standard getters methods. All getters are expected to
 * return non-null values unless otherwise specified by their documentation. This class is immutable. </p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopMapReduceService#sendJob(HadoopMapReduceJob, Properties)
 * @see HadoopJobRunner#runJobLogProgress(HadoopMapReduceService, HadoopMapReduceJob, Properties, boolean)
 */
public class HadoopMapReduceJob {
	private String jobName;

	private URL jobJarFile;
	private List<URI> inputFiles;
	private URI outputDir;
	private URI workingDirectory;
	private boolean clearOutputDir;

	private String mapper;
	private String combiner;
	private String reducer;
	private String inputFormat;
	private String outputFormat;
	private String outputKey;
	private String outputValue;
	private Integer numMappers;
	private Integer numReducers;
	private long timeout;

	/**
	 * Initializes new {@code HadoopMapReduceJob} that contains at least the mandatory configuration required to execute
	 * the job. If not stated otherwise parameters can be <code>null</code> (not specified).
	 * 
	 * @param jobName A name of the job typically specified by user as an identification of the job. It is not needed to
	 *        be unique.
	 * @param jobJarFile Absolute URL to locally stored JAR file containing definition of job classes (for example
	 *        mapper class). May not be <code>null</code>.
	 * @param inputFiles List containing one or more location(s) of job input file(s) on Hadoop cluster file system.
	 *        Location(s) can be absolute (in terms of Hadoop file system) or relative to working directory. May not be
	 *        <code>null</code> not empty.
	 * @param outputDir URI of directory on Hadoop cluster file system where the output of the job should be stored. If
	 *        this directory does not exist it is created but the parent directory must exist. Can be absolute or
	 *        relative to specified working directory. May not be <code>null</code>.
	 * @param workingDirectory Hadoop file system URI to working directory of the job. Might be absolute or relative to
	 *        previous working directory.
	 * @param clearOutputDir If {@code true} then in case that output directory of the job already exists it should be
	 *        deleted before execution of the job.
	 * @param mapper Fully qualified name of class that is to be used as mapper class of the map/reduce job. This class
	 *        is typically contained in job JAR file. May not be <code>null</code> not empty.
	 * @param combiner Fully qualified name of the class that is to be used as combiner of map/reduce job. This class is
	 *        typically contained in job JAR file.
	 * @param reducer Fully qualified name of the class that is to be used as reducer of map/reduce job. This class is
	 *        typically contained in job JAR file.
	 * @param inputFormat Fully qualified name of input format class for this map/reduce job.
	 * @param outputFormat Fully qualified name of output format class for this map/reduce job.
	 * @param outputKey Fully qualified name of output records key class for this map/reduce job. May not be
	 *        <code>null</code>.
	 * @param outputValue Fully qualified name of output records value class for this map/reduce job. May not be
	 *        <code>null</code>.
	 * @param numMappers Number of mappers to be used for job execution or <code>null</code> if not specified. If
	 *        specified must be greater then 0.
	 * @param numReducers Number of reducers to be used for job execution or <code>null</code> if not specified. If
	 *        specified must be greater then 0.
	 * @param timeout Time limit for job execution in milliseconds. May not be less then 0. Value of 0 specifies no limit.
	 */
	public HadoopMapReduceJob(String jobName, URL jobJarFile, List<URI> inputFiles, URI outputDir,
			URI workingDirectory, boolean clearOutputDir, String mapper, String combiner, String reducer,
			String inputFormat, String outputFormat, String outputKey, String outputValue, Integer numMappers,
			Integer numReducers, long timeout) {
		if (jobJarFile == null) {
			throw new NullPointerException("jobJarFile");
		}
		if (inputFiles == null) {
			throw new NullPointerException("inputFile");
		}
		if (inputFiles.isEmpty()) {
			throw new IllegalArgumentException("inputFiles is empty. No job input file specified.");
		}
		if (outputDir == null) {
			throw new NullPointerException("outputFile");
		}
		if (mapper == null) {
			throw new NullPointerException("mapper");
		}
		if (mapper.isEmpty()){
			throw new IllegalArgumentException("mapper cannot be empty string.");
		}
		if (outputKey == null) {
			throw new NullPointerException("outputKey");
		}
		if (outputValue == null) {
			throw new NullPointerException("outputValue");
		}
		if (numMappers != null && numMappers <= 0) {
			throw new IllegalArgumentException("maxMappers must be greater then 0.");
		}
		if (numReducers != null && numReducers <= 0) {
			throw new IllegalArgumentException("maxReducers must be greater then 0");
		}
		if ( timeout < 0) {
			throw new IllegalArgumentException("Value of timeout cannot be less then 0.");
		}

		this.jobJarFile = jobJarFile;
		this.inputFiles = inputFiles;
		this.outputDir = outputDir;
		this.workingDirectory = workingDirectory;
		this.clearOutputDir = clearOutputDir;
		this.jobName = jobName;
		this.mapper = mapper;
		this.combiner = combiner;
		this.reducer = reducer;
		this.inputFormat = inputFormat;
		this.outputFormat = outputFormat;
		this.outputKey = outputKey;
		this.outputValue = outputValue;
		this.numMappers = numMappers;
		this.numReducers = numReducers;
		this.timeout = timeout;
	}

	/**
	 * Gets job name.
	 * @return A name of the job typically specified by user as an identification of the job or <code>null</code>>. It
	 *         is not required to be unique.
	 */
	public String getJobName() {
		return jobName;
	}

	/**
	 * Gets URL of job jar file.
	 * @return Absolute URL to locally stored JAR file containing definition of job classes (for example mapper class)
	 */
	public URL getJobJarFile() {
		return jobJarFile;
	}

	/**
	 * Gets list of job input files on Hadoop cluster file system.
	 * @return List containing one or more URI(s) of job input file(s). URI(s) can be absolute (in terms of that Hadoop
	 *         file system) or relative to working directory.
	 */
	public List<URI> getInputFiles() {
		return Collections.unmodifiableList(inputFiles);
	}

	/**
	 * Gets job output directory on Hadoop cluster file system.
	 * @return URI of directory where the output of the job should be stored. Can be absolute or relative to specified
	 *         working directory.
	 */
	public URI getOutputDir() {
		return outputDir;
	}

	/**
	 * Gets URI of job working directory on Hadoop file system.
	 * @return URI to working directory of the job or <code>null</code> if not specified. Might be absolute or relative
	 *         to previous working directory.
	 */
	public URI getWorkingDirectory() {
		return workingDirectory;
	}

	/**
	 * Indicates whether job output directory is to be deleted (if existing) on Hadoop cluster file system before job
	 * execution.
	 * @return {@code true} if and only if the output directory of the job is be deleted before job execution in case it
	 *         exists.
	 */
	public boolean isClearOutputPath() {
		return clearOutputDir;
	}

	/**
	 * Gets name of mapper class of the job.
	 * @return Fully qualified name of class that is to be used as mapper class of the map/reduce job. This class is
	 *         typically contained in job JAR file.
	 */
	public String getMapper() {
		return mapper;
	}

	/**
	 * Gets name of combiner class of the job.
	 * @return Fully qualified name of the class that is to be used as combiner of map/reduce job or <code>null</code>
	 *         if not specified. This class is typically contained in job JAR file.
	 */
	public String getCombiner() {
		return combiner;
	}

	/**
	 * Gets name of reducer class of the job.
	 * @return Fully qualified name of the class that is to be used as reducer of map/reduce job or <code>null</code> if
	 *         not specified. This class is typically contained in job JAR file.
	 */
	public String getReducer() {
		return reducer;
	}

	/**
	 * Gets input format class name of the job.
	 * @return Fully qualified name of input format class for this map/reduce job or <code>null</code> if not specified.
	 */
	public String getInputFormat() {
		return inputFormat;
	}

	/**
	 * Gets output format class name of the job.
	 * @return Fully qualified name of output format class for this map/reduce job or <code>null</code> if not
	 *         specified.
	 */
	public String getOutputFormat() {
		return outputFormat;
	}

	/**
	 * Gets name of output key class of the job.
	 * @return Fully qualified name of output records key class for this map/reduce job.
	 */
	public String getOutputKey() {
		return outputKey;
	}

	/**
	 * Gets name of output value class of the job.
	 * @return Fully qualified name of output records value class for this map/reduce job.
	 */
	public String getOutputValue() {
		return outputValue;
	}

	/**
	 * Gets number of mappers.
	 * @return Number of mappers to be used for job execution or <code>null</code> if not specified. If
	 *        non-null it is greater then 0.
	 */
	public Integer getNumMappers() {
		return numMappers;
	}

	/**
	 * Gets number of reducers.
	 * @return Number of reducers to be used for job execution or <code>null</code> if not specified. If
	 *        non-null it is greater then 0.
	 */
	public Integer getNumReducers() {
		return numReducers;
	}

	/**
	 * Gets job timeout limit.
	 * @return Time limit for job execution in milliseconds. Value returned is equal or greater then 0. Value of 0
	 *         specifies no limit.
	 */
	public long getTimeout() {
		return timeout;
	}

	@Override
	public String toString() {
		return "HadoopMapReduceJob [jobName=" + jobName + ", jobJarFile=" + jobJarFile + ", inputFiles=" + inputFiles
				+ ", outputDir=" + outputDir + ", workingDirectory=" + workingDirectory + ", clearOutputDir="
				+ clearOutputDir + ", mapper=" + mapper + ", combiner=" + combiner + ", reducer=" + reducer
				+ ", inputFormat=" + inputFormat + ", outputFormat=" + outputFormat + ", outputKey=" + outputKey
				+ ", outputValue=" + outputValue + ", numMappers=" + numMappers + ", numReducers=" + numReducers
				+ ", timeout=" + timeout + "]";
	}
}
