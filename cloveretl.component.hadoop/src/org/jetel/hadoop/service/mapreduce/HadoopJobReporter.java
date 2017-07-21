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

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * <p> Defines a service for obtaining information about Hadoop map/reduce job that is/was executed on Hadoop cluster.
 * Information is typically obtained by communication with the Hadoop cluster (specifically it is obtained from
 * jobtracker). The job that is to be reported about might be still in progress (in which case tracking information are
 * obtained) or execution of the job might already be completed (see method {@link #isComplete()}). In later case result
 * information are obtained. </p>
 * 
 * <p> This interface specifies methods for obtaining information about the job provided by Hadoop API. Therefore,
 * implementors are typically wrappers around Hadoop API class. This interface provides abstraction from Hadoop specific
 * API that may differ between Hadoop versions. </p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopMapReduceService#sendJob(HadoopMapReduceJob, Properties)
 */
public interface HadoopJobReporter {

	/**
	 * Gets identification of the job.
	 * @return Unique identification of the job assigned by Hadoop cluster.
	 */
	String getID();

	/**
	 * Gets job name.
	 * @return Identification (label) of the job assigned by user before sending the job to cluster. This might not be
	 *         unique.
	 */
	String getName();

	/**
	 * Gets progress of setup phase of the job.
	 * @return Progress of the setup phase of map/reduce job as number belonging to interval 0 (inclusive) to 1
	 *         (inclusive). Returns 0 if setup phase did not start yet and 1 if the phase has completed.
	 * @throws IOException if communication with jobtracker of the cluster fails or the job itself fails.
	 */
	float setupProgress() throws IOException;

	/**
	 * Gets progress of map phase of the job.
	 * @return Progress of the map phase of map/reduce job as number belonging to interval 0 (inclusive) to 1
	 *         (inclusive). Returns 0 if map phase did not start yet and 1 if the phase has completed.
	 * @throws IOException if communication with jobtracker of the cluster fails or the job itself fails.
	 */
	float mapProgress() throws IOException;

	/**
	 * Gets progress of reduce phase of the job.
	 * @return Progress of the reduce phase of map/reduce job as number belonging to interval 0 (inclusive) to 1
	 *         (inclusive). Returns 0 if reduce phase did not start yet and 1 if the phase has completed.
	 * @throws IOException if communication with jobtracker of the cluster fails or the job itself fails.
	 */
	float reduceProgress() throws IOException;

	/**
	 * Gets progress of cleanup phase of the job.
	 * @return Progress of the cleanup phase of map/reduce job as number belonging to interval 0 (inclusive) to 1
	 *         (inclusive). Returns 0 if cleanup phase did not start yet and 1 if the phase has completed.
	 * @throws IOException if communication with jobtracker of the cluster fails or the job itself fails.
	 */
	float cleanUpProgress() throws IOException;

	/**
	 * Gets state of the job as provided by jobtracker.
	 * @return
	 * @throws IOException if communication with the jobtracker of the cluster fails.
	 */
	String getJobState() throws IOException;

	/**
	 * Indicates that the job has completed.
	 * @return {@code true} if and only if the job is has finished. It might or might not be successful.
	 * @throws IOException if communication with jobtracker of the cluster fails or the job itself fails during call to
	 *         this method.
	 * @see #isSuccessful()
	 */
	boolean isComplete() throws IOException;

	/**
	 * Indicates whether the job was successful.
	 * @return {@code true} if and only if the map/reduce job is completed and it finished successfully.
	 * @throws IOException if communication with the jobtracker of the cluster fails or the job itself fails during call
	 *         to this method.
	 * @see #isComplete()
	 */
	boolean isSuccessful() throws IOException;

	/**
	 * In case that map/reduce job has finished but it was not successful this method can be used to obtain gailure
	 * info. It can be used to debug definition of mapper, reducer or other parts of the job definition.
	 * @return In case of job failure information about the failure or {@code null}.
	 * @throws IOException if communication with jobtracker of the cluster fails.
	 */
	String getFailureInfo() throws IOException;

	/**
	 * Attempts to kill Hadoop map/reduce job. If call to this method succeeds callers should assume that the job has
	 * either finished or it was stopped by that call. In both cases the job is not being executed any more.
	 * @throws IOException IOException if communication with jobtracker of the cluster fails or there is an exception
	 *         while killing the job.
	 */
	void kill() throws IOException;

	/**
	 * For given Hadoop map/reduce counter key representation gets value of counter represented for this job.
	 * 
	 * @param counterKey Hadoop API independent representation of Hadoop map/reduce counter key.
	 * @return Value of specified counter.
	 * @throws IOException if communication with jobtracker of the cluster fails or counter value cannot be obtained by
	 *         jobtracker.
	 * @see HadoopMapReduceInfoService#getCounterGroups()
	 * @see HadoopCounterGroup
	 */
	long getCounterValue(HadoopCounterKey counterKey) throws IOException;

	/**
	 * Gets all counters as list of keys and respective values.
	 * @return A list of all map/reduce counters (including user defined and predefined) as key-value pairs.
	 * @throws IOException if communication with the jobtracker fails of it fails to obtain value of counters.
	 */
	List<HadoopCounterKeyValuePair> getAllCounters() throws IOException;
}
