/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
 *   
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *   
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU   
 *    Lesser General Public License for more details.
 *   
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 */
package org.jetel.util;


/**
 * Helper class specifying an inner interval of a file.
 * It servers for cluster data processing as interoperability object 
 * for definition of an inner file interval, which will be processed by a cluster node.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.12.2009
 */
public class FileConstrains {
	
	private String fileURL;
	
	private long intervalStart;

	private long intervalEnd;

	public FileConstrains(String fileURL, long intervalStart, long intervalEnd) {
		//Assert.assertTrue("Interval start has to be smaller than interval end.", intervalStart <= intervalEnd);
		this.fileURL = fileURL;
		this.intervalStart = intervalStart;
		this.intervalEnd = intervalEnd;
	}

	public String getFileURL() {
		return fileURL;
	}

	public long getIntervalStart() {
		return intervalStart;
	}

	public long getIntervalEnd() {
		return intervalEnd;
	}
	
	public long length() {
		return intervalEnd - intervalStart;
	}
	
}
