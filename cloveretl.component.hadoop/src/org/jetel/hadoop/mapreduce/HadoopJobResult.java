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
package org.jetel.hadoop.mapreduce;

import org.jetel.hadoop.service.mapreduce.HadoopJobReporter;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceService;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceJob;
import java.util.Properties;

/**
 * A bean class intended to store various information and results of execution of map/reduce job sent to (and executed
 * on) Hadoop cluster. All the job statistics and results are initialized in constructor and available through getters.
 * This class is immutable.
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopJobRunner#runJobLogProgress(HadoopMapReduceService, HadoopMapReduceJob, Properties, boolean)
 */
public final class HadoopJobResult {
	private final HadoopJobReporter reporter;
	private final String jobName;
	private final String jobId;
	private final long startTime; // milliseconds since start of year 1970 (Unix time)
	private final long stopTime; // milliseconds since start of year 1970 (Unix time)
	private final boolean timeouted;
	private final Exception error;
	private final boolean jobSuccessfull;
	private final boolean jobComplete;
	private final String lastPhase;
	private final float lastPhaseProgress; // in range <0, 1>

	/**
	 * Creates new {@code HadoopJobResult} instance that stores information about run of map/reduce job. The
	 * {@code null} value is accepted value for all non primitive parameters of this constructor.
	 * 
	 * @param reporter Utility allowing to obtain job information from Hadoop cluster or null if there was exception
	 *        while starting the job.
	 * @param jobName A name of the job that was given to it by a user. Non unique identification of the job.
	 * @param jobId Unique identification of the job assigned by cluster.
	 * @param startTime Start time of the job in milliseconds since start of year 1970 (Unix or POSIX time). If measured
	 *        correctly it should be greater or equal to current time.
	 * @param stopTime End time of the job in milliseconds since start of year 1970 (Unix or POSIX time). If measured
	 *        correctly it should be greater or equal to current time. Must be equal or greater then start time.
	 * @param timeouted {@code true} if and only if the job has stopped due to its duration was longer then set timeout.
	 * @param error {@code Exception} that occurred during job execution and caused the job to be prematurely
	 *        terminated.
	 * @param jobSuccessfull {@code true} if and only if the job finished successfully.
	 * @param jobComplete {@code false} if and only if the job was still running when this constructor was called.
	 * @param lastPhase Last phase of the job that was in progress when the job was stopped. If the job was successful
	 *        this must be last phase in job life.
	 * @param lastPhaseProgress Progress of last phase of the job. Must be in interval <0, 1> inclusive. Must be equal
	 *        to 1 if the job was successful.
	 * @see HadoopJobRunner#runJobLogProgress(HadoopMapReduceService, HadoopMapReduceJob, Properties, boolean)
	 */
	public HadoopJobResult(HadoopJobReporter reporter, String jobName, String jobId, long startTime, long stopTime,
			boolean timeouted, Exception error, boolean jobSuccessfull, boolean jobComplete, String lastPhase,
			float lastPhaseProgress) {
		if (startTime > stopTime) {
			throw new IllegalArgumentException(
					"stopTime of the job cannot be greater then its start time. Start time was: " + startTime
							+ "; stop time was: " + stopTime + ".");
		}
		if (lastPhaseProgress < 0 || lastPhaseProgress > 1) {
			throw new IllegalArgumentException("Value of lastPhaseProgress must be in interval <0,1>. Value was:"
					+ lastPhaseProgress);
		}
		if (jobSuccessfull && lastPhaseProgress != 1) {
			throw new IllegalArgumentException("Value of lastPhaseProgress must be 1 if the job was successful.");
		}
		this.reporter = reporter;
		this.jobName = jobName;
		this.jobId = jobId;
		this.startTime = startTime;
		this.stopTime = stopTime;
		this.timeouted = timeouted;
		this.error = error;
		this.jobSuccessfull = jobSuccessfull;
		this.jobComplete = jobComplete;
		this.lastPhase = lastPhase;
		this.lastPhaseProgress = lastPhaseProgress;
	}

	/**
	 * Gets reporter for the job.
	 * @return Utility allowing to obtain job information from Hadoop cluster or null if there was exception while
	 *         starting the job.
	 */
	public HadoopJobReporter getReporter() {
		return reporter;
	}

	/**
	 * Gets name of the job.
	 * @return A name of the job that was given to it by a user. It is non unique identification of the job. Might be
	 *         {@code null}.
	 */
	public String getJobName() {
		return jobName;
	}

	/**
	 * Gets job identificator.
	 * @return Unique identification of the job assigned by cluster. Can be {@code null} if job did not start
	 *         successfully.
	 */
	public String getJobId() {
		return jobId;
	}

	/**
	 * Gets job start time.
	 * @return Start time of the job in milliseconds since start of year 1970 (Unix or POSIX time).
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * Gets end time of the job.
	 * @return the stopTime End time of the job in milliseconds since start of year 1970 (Unix or POSIX time).
	 */
	public long getStopTime() {
		return stopTime;
	}

	/**
	 * Indicates whether the job has stopped due timeout.
	 * @return {@code true} if and only if the job has stopped due to its duration was longer then set timeout.
	 */
	public boolean isTimeouted() {
		return timeouted;
	}

	/**
	 * Gets {@code Exception} that caused the job to stop.
	 * @return {@code Exception} that occurred during job execution and caused the job to be prematurely
	 *        terminated.
	 */
	public Exception getError() {
		return error;
	}

	/**
	 * Indicates whether the job is successful.
	 * @return {@code true} if and only if the job finished successfully.
	 */
	public boolean isJobSuccessfull() {
		return jobSuccessfull;
	}

	/**
	 * Indicates whether the job is complete.
	 * @return {@code false} if and only if the job was still running when this instance of {@code HadoopJobResult} was created.
	 */
	public boolean isJobComplete() {
		return jobComplete;
	}

	/**
	 * Gets last phase of the job.
	 * @return Last phase of the job that was in progress when the job was stopped. If the job was successful
	 *        always returns last phase in job life (see {@link #isJobSuccessfull()}). 
	 */
	public String getLastPhase() {
		return lastPhase;
	}

	/**
	 * Gets progress of the last phase of the job.
	 * @return Progress of last phase of the job which is a value from interval <0, 1> inclusive. It is always equal
	 *        to 1 if the job was successful (see {@link #isJobSuccessfull()}).
	 */
	public float getLastPhaseProgress() {
		return lastPhaseProgress;
	}
}
