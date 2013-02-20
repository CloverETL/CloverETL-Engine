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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Task;
import org.jetel.hadoop.service.mapreduce.HadoopCounterGroup;
import org.jetel.hadoop.service.mapreduce.HadoopCounterKey;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceInfoService;

/**
 * Provider of the service definition {@link HadoopMapReduceInfoService} that uses Hadoop API version 0.20.2. Suitable
 * for Hadoop API versions up to 0.22.0.
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 18.12.2012
 */
public class HadoopMapReduceInfoProvider implements HadoopMapReduceInfoService {

	@Override
	public List<HadoopCounterGroup> getCounterGroups() {
		List<HadoopCounterGroup> result = new ArrayList<HadoopCounterGroup>();
		result.add(new HadoopCounterGroup(JobInProgress.Counter.values(), "JobInProgressCounters", null));
		result.add(new HadoopCounterGroup(Task.Counter.values(), "TaskCounters", null));
		//TODO this is temporary solution for adding group for FileSystemCounters enum as the enum class itself cannot be found
		String fsCountersGroupName = "FileSystemCounters";
		List<HadoopCounterKey> fsCounters = new ArrayList<HadoopCounterKey>();
		fsCounters.add(new HadoopCounterKey("FILE_BYTES_READ", fsCountersGroupName));
		fsCounters.add(new HadoopCounterKey("HDFS_BYTES_READ", fsCountersGroupName));
		fsCounters.add(new HadoopCounterKey("FILE_BYTES_WRITTEN", fsCountersGroupName));
		fsCounters.add(new HadoopCounterKey("HDFS_BYTES_WRITTEN", fsCountersGroupName));
		result.add(new HadoopCounterGroup(fsCounters, fsCountersGroupName, null));
		return result;
	}
}
