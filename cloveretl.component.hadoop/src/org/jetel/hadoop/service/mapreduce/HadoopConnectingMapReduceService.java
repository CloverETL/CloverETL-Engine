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

import org.jetel.hadoop.service.HadoopConnectingService;

/**
 * <p> A union of {@link HadoopConnectingService} and {@link HadoopMapReduceService} that specifies all operations
 * needed to: <ol> <li>Connect to Hadoop cluster and validate that connection.</li> <li>Send map/reduce jobs to be
 * executed on connected Hadoop cluster.</li> <li>Close the connection.</li> </ol> This interface is intended to be
 * implemented by map/reduce job sending service providers.</p>
 * 
 * <p> This service must be connected before it can provide any useful operation. As that connection may fail due to
 * external factors instances of this interface should not be used if such a failure is not expected. Specifically,
 * instances of this interface are only meant to be used from engine. They are not be used in designer. Use
 * {@link HadoopMapReduceInfoService} in designer instead. </p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopMapReduceInfoService
 */
public interface HadoopConnectingMapReduceService extends HadoopConnectingService<HadoopMapReduceConnectionData>,
		HadoopMapReduceService {
}
