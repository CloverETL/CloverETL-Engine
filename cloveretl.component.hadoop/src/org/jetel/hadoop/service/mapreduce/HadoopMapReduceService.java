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
import java.util.Properties;
import org.jetel.hadoop.service.HadoopConnectingService;

/**
 * <p> Service that allows to send map/reduce job specifications to the Hadoop cluster (specifically to jobtracker of
 * the cluster) where the jobs specified are executed. </p>
 * 
 * <p> This service is only intended to be used by end clients and it abstracts from details of connecting to the
 * cluster. Clients using instances of this interface are expected not to possess nor need information regarding which
 * cluster are the job sent to. Clients assume that this service is always able to send jobs specifications (it is
 * connected). </p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 9.11.2012
 * @see HadoopConnectingService
 * @see HadoopConnectingMapReduceService
 */
public interface HadoopMapReduceService {

	/**
	 * <p> Sends map/reduce job specified by parameters of this method to Hadoop cluster where it is executed. This
	 * method does not block. Job executed on cluster asynchronously but can be monitored using returned
	 * {@link HadoopJobReporter}. </p>
	 * 
	 * <p> Implementors should ensure that additional job settings specified by parameter do <strong>not</strong> take
	 * priority to connection settings contained in {@code job} parameter. That is by default if the same setting is
	 * specified by both parameters then value specified by {@code job} overrides this that of
	 * {@code additionalJobSettings}. However this behaviour can be changed by implementors if desired. </p>
	 * 
	 * @param job Non-null specification of the job to be executed.
	 * @param additionalJobSettings Non-null additional job settings represented as pairs of Hadoop API specific keys
	 *        and corresponding values.
	 * @return A utility that allows to monitor and track sent job. May never return <code>null</code>.
	 * @throws IOException If communication with the cluster (specifically the jobtracker) fails while sending
	 *         specification of the job or if the job configuration specified causes the job to fail.
	 */
	HadoopJobReporter sendJob(HadoopMapReduceJob job, Properties additionalJobSettings) throws IOException;
}
