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
package org.jetel.hadoop.service.filesystem;

import org.jetel.hadoop.service.AbstractHadoopConnectionData;

public class HadoopFileSystemConnectionData extends AbstractHadoopConnectionData {
	private String masterHost;
	private int masterPort;

	public HadoopFileSystemConnectionData(String masterHost, int masterPort, String user) {
		super(user);
		if (masterHost == null) {
			throw new IllegalArgumentException("Host not specified");
		}
		if (masterPort < 0) {
			throw new IllegalArgumentException("Port cannot be less then 0.");
		}
		this.masterHost = masterHost;
		this.masterPort = masterPort;
	}

	public String getHost() {
		return masterHost;
	}

	public int getPort() {
		return masterPort;
	}
}