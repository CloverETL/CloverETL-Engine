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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jetel.hadoop.service.mapreduce.HadoopJobReporter;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceJob;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceService;

/**
 * Utility that can run Hadoop map/reduce job, monitor and log its progress in synchronous manner.
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 */
public final class HadoopJobRunner {

	public static final String JOB_STARTING_MESSAGE_FORMAT = "Starting execution of Hadoop map/reduce job "
			+ "'%s' (id=%s).";
	public static final String PROGRESS_MESSAGE_FORMAT_PART_1 = "Progress report of Hadoop map/reduce job "
			+ "'%s' (id=%s):";
	public static final String PROGRESS_MESSAGE_FORMAT_PART_2 = "Job setup: %1.2f%%   Mappers: %1.2f%%   "
			+ "Reducers: %1.2f%%   Job cleanup: %1.2f%%";
	public static final String JOB_SUCCESS_MESSAGE_FORMAT = "Hadoop map/reduce job '%s' (id=%s) finished "
			+ "successfully.";
	public static final String JOB_ERROR_MESSAGE_FORMAT = "Hadoop map/reduce job '%s' (id=%s) failed with error!"
			+ " Hadoop failure info: %s";
	public static final String JOB_ABORT_MESSAGE_FORMAT = "Hadoop map/reduce job '%s' (id=%s) was "
			+ "killed due to graph execution abort.";
	public static final String JOB_TIMEOUT_MESSAGE_FORMAT = "Hadoop map/reduce job '%s' (id=%s) was "
			+ "killed due to timeout.";
	public static final String JOB_IOEXCEPTION_MESSAGE_FORMAT = "Hadoop map/reduce job '%s' (id=%s) failed "
			+ "due to external exception. The exception was: %s";
	public static final String CANNOT_KILL_JOB_MESSAGE_FORMAT = "Exception occured while killing Hadoop"
			+ " map/reduce job '%s' (id=%s).";
	public static final String JOB_KILLED_MESSAGE_FORMAT = "Hadoop map/reduce job '%s' (id=%s) has been killed by CloverETL.";
	public static final char MARGIN_CHARACTER = '=';

	private final int progressLookupInterval; // in miliseconds
	private final Running client;
	private final Logger log;
	private String jobName;// name of currently executing job/last job executed. Null iff job failed/none was executed
	private String jobId;// id of currently executing job/last job executed. Null iff job failed/none was executed
	private volatile HadoopJobReporter reporter; // reporter of last run job or null if there was not job yet,
	// reporter was not obtained due to exception or the job was already killed. It is kept as attribute so that job can
	// be killed
	private volatile boolean runAsDaemon = false;

	/**
	 * Creates {@code HadoopJobRunner} instance with given client, log and progress lookup interval.
	 * 
	 * @param client {@code Running} that tells if currently running job should continue to run.
	 * @param log A log used to log progress of running jobs.
	 * @param progressLookupInterval Interval in which progress of jobs run using this instance should be monitored and
	 *        logged. Can be zero but not negative.
	 */
	public HadoopJobRunner(Running client, Logger log, int progressLookupInterval) {
		if (client == null) {
			throw new NullPointerException("client");
		}
		if (log == null) {
			throw new NullPointerException("log");
		}
		if (progressLookupInterval < 0) {
			throw new IllegalArgumentException("progressLookupInterval must be at least zero.");
		}
		this.client = client;
		this.log = log;
		this.progressLookupInterval = progressLookupInterval;
	}

	/**
	 * Runs Hadoop map/reduce job specified by parameter using given {@code HadoopMapReduceService}. This method
	 * blocs while the job is not complete ({@link HadoopJobReporter#isComplete()}), method {@link Running#runIt()} of
	 * the client given in constructor returns true and the job did not timeout. While the job is running its progress
	 * is logged.
	 * TODO Add support for asynchronous jobs. UPDATE very basic support added
	 * 
	 * @param mapRedService Non <code>null</code> map/reduce service used to send job to Hadoop cluster.
	 * @param job Non <code>null</code> specification of the job to be send and monitored.
	 * @param additionalJobSettings Non <code>null</code> additional settings for the job to be sent provided by user.
	 * @param runAsDaemon <code>true</code> if the job should not be killed after {@link Running#runIt()} returns false,
	 *        false otherwise.
	 * @return Result containing locally measured info about the job and instance of {@link HadoopJobReporter} that
	 *         provides access to on the cluster information about the job.
	 */
	public final HadoopJobResult runJobLogProgress(HadoopMapReduceService mapRedService, HadoopMapReduceJob job,
			Properties additionalJobSettings, boolean runAsDaemon) {
		if (mapRedService == null) {
			throw new NullPointerException("mapRedService");
		}
		if (job == null) {
			throw new NullPointerException("job");
		}
		float setupPrs = 0;
		float mapPrs = 0;
		float reducePrs = 0;
		float cleanupPrs = 0;
		boolean timeouted = false;
		boolean successfull = false;
		boolean complete = false;
		Exception exception = null;
		jobName = job.getJobName();
		jobId = null;
		long startTime = System.currentTimeMillis();
		long timeLimit = job.getTimeout() <= 0 ? Long.MAX_VALUE : startTime + job.getTimeout();
		long endTime = Long.MIN_VALUE;
		try {
			this.runAsDaemon = runAsDaemon;
			reporter = mapRedService.sendJob(job, additionalJobSettings);
			jobName = reporter.getName();
			jobId = reporter.getID();
			boolean change = true;
			logFormatedWithMargin(Level.INFO, JOB_STARTING_MESSAGE_FORMAT);
			if (!runAsDaemon) {
				while (client.runIt() && !(timeouted = System.currentTimeMillis() >= timeLimit) && !reporter.isComplete()) {
					try {
						Thread.sleep(Math.min(progressLookupInterval, Math.max(0, timeLimit - System.currentTimeMillis())));
					} catch (InterruptedException ex) {
						log.debug("Hadoop job thread sleep interupted. Might be caused by graph abort.", ex);
					}
					change |= setupPrs < 1 && setupPrs != (setupPrs = reporter.setupProgress());
					change |= setupPrs == 1 && mapPrs < 1 && mapPrs != (mapPrs = reporter.mapProgress());
					change |= mapPrs == 1 && reducePrs < 1 && reducePrs != (reducePrs = reporter.reduceProgress());
					change |= reducePrs == 1 && cleanupPrs < 1 && cleanupPrs != (cleanupPrs = reporter.cleanUpProgress());
					if (change) {
						formatProgress(setupPrs, mapPrs, reducePrs, cleanupPrs);
						change = false;
					}
				}
				if (!(complete = reporter.isComplete()) && (!runAsDaemon || timeouted)) {
					reporter.kill(); // Note: After client.runIt() becomes false, it is very likely that any (RPC) call on reporter will end up with InterruptedException
					log.debug(String.format(JOB_KILLED_MESSAGE_FORMAT, jobName, jobId));
				}
				endTime = Math.min(System.currentTimeMillis(), timeLimit);
				if ((successfull = reporter.isSuccessful()) || complete) {
					if (cleanupPrs < 1) {
						formatProgress(setupPrs = 1, mapPrs = 1, reducePrs = 1, cleanupPrs = 1);
					}
					if (successfull){
						logFormatedWithMargin(Level.INFO, JOB_SUCCESS_MESSAGE_FORMAT);
					} else {
						logFormatedWithMargin(Level.ERROR, JOB_ERROR_MESSAGE_FORMAT, reporter.getFailureInfo());
					}
				} else if (!client.runIt()) {
					if (!runAsDaemon) {
						logFormatedWithMargin(Level.INFO, JOB_ABORT_MESSAGE_FORMAT);
					}
				} else if (timeouted) {
					logFormatedWithMargin(Level.INFO, JOB_TIMEOUT_MESSAGE_FORMAT);
				} else {
					throw new IllegalStateException("Unknown reason for stopping Hadoop job execution. Possible bug.");
				}
			}
		} catch (IOException ex) {
			logFormatedWithMargin(Level.ERROR, JOB_IOEXCEPTION_MESSAGE_FORMAT, ex.toString());
			log.error(String.format(
					"External exception occred during execution of Hadoop map/reduce job '%s' (id=%s). "
							+ "The job failed.", jobName, jobId), ex);
			exception = ex;
		}
		if (endTime == Long.MIN_VALUE) {
			endTime = Math.min(System.currentTimeMillis(), timeLimit);
		}

		return new HadoopJobResult(reporter, jobName, jobId, startTime, endTime, timeouted, exception, successfull,
				complete, getLastMapredPhase(setupPrs, mapPrs, reducePrs, cleanupPrs), getLastMapredPhaseProgress(
						setupPrs, mapPrs, reducePrs, cleanupPrs));
	}

	public void killRunningJobs() {
		if (reporter != null && !runAsDaemon) {
			try {
				reporter.kill();
				log.debug(String.format(JOB_KILLED_MESSAGE_FORMAT, jobName, jobId));
			} catch (IOException ex) {
				if (log == null) {
					LogFactory.getLog(HadoopJobRunner.class).error(
							String.format(CANNOT_KILL_JOB_MESSAGE_FORMAT, jobName, jobId), ex);
				} else {
					log.error(String.format(CANNOT_KILL_JOB_MESSAGE_FORMAT, reporter.getName(), reporter.getID()), ex);
				}
			} finally {
				reporter = null;
			}
		}
	}

	private static String getLastMapredPhase(float setupPrs, float mapPrs, float reducePrs, float cleanupPrs) {
		return setupPrs < 1 ? "Setup" : (mapPrs < 1 ? "Map" : (reducePrs < 1 ? "Reduce" : "Cleanup"));
	}

	private static float getLastMapredPhaseProgress(float setupPrs, float mapPrs, float reducePrs, float cleanupPrs) {
		return setupPrs < 1 ? setupPrs : (mapPrs < 1 ? mapPrs : (reducePrs < 1 ? reducePrs : cleanupPrs));
	}

	private final void formatProgress(float setupPrs, float mapPrs, float reducePrs, float cleanupPrs) {
		logWithMargin(Level.INFO, String.format(PROGRESS_MESSAGE_FORMAT_PART_1, jobName, jobId), String.format(
				PROGRESS_MESSAGE_FORMAT_PART_2, setupPrs * 100, mapPrs * 100, reducePrs * 100, cleanupPrs * 100));
	}

	private final void logFormatedWithMargin(Level level, String formatedMessageWithNameAndMargin,
			String... moreFormatArguments) {
		List<String> allArgs = moreFormatArguments == null ? new ArrayList<String>() : new ArrayList<String>(
				Arrays.asList(moreFormatArguments));
		allArgs.add(0, jobId);
		allArgs.add(0, jobName);
		logWithMargin(level, String.format(formatedMessageWithNameAndMargin, allArgs.toArray()));
	}

	private final void logWithMargin(Level level, String... messageLines) {
		StringBuilder margin = new StringBuilder();
		int maxLineLength = 0;
		for (String line : messageLines) {
			if (line.length() > maxLineLength) {
				maxLineLength = line.length();
			}
		}
		for (int i = 0; i < maxLineLength; i++) {
			margin.append(MARGIN_CHARACTER);
		}

		log.log(level, margin);
		for (String line : messageLines) {
			log.log(level, line);
		}
		log.log(level, margin);
	}
}
