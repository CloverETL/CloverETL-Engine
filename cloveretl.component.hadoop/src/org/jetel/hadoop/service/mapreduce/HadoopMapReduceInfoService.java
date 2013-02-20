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

import java.util.List;
import org.jetel.hadoop.service.HadoopConnectingService;

/**
 * <p> A service provided in addition to {@link HadoopConnectingMapReduceService}. Together they form a complete
 * map/reduce API that is not Hadoop API and Hadoop version specific. </p>
 * 
 * <p>This service provides access to general map/reduce information that do not require connection to Hadoop cluster to
 * be obtained, yet can differ along various versions of Hadoop API. Therefore this service does not extend
 * {@link HadoopConnectingService} and is suitable for use in designer because its operations may not fail due to
 * external factors. </p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopConnectingMapReduceService
 */
public interface HadoopMapReduceInfoService {

	/**
	 * Gets predefined counter groups that are available for all map/reduce jobs. These groups contain all default
	 * counters that have their values computed automatically by Hadoop for all map/reduce job. Does not include any
	 * custom, user defined counters.
	 * @return List of all default, predefined counter groups which are available for all map/reduce jobs,
	 * @see HadoopCounterKey
	 * @see HadoopJobReporter#getCounterValue(HadoopCounterKey)
	 * @see HadoopJobReporter#getCustomCounters()
	 */
	List<HadoopCounterGroup> getCounterGroups();
}
