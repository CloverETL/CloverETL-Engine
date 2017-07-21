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

import java.util.Properties;

import org.jetel.hadoop.service.AbstractHadoopConnectionData;
import org.jetel.hadoop.service.filesystem.HadoopFileSystemService;

/**
 * Represents set settings that are required to create connection to jobtracter of Hadoop cluser. This is bean class
 * that just the wraps required configuration data. This class is immutable.
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopConnectingMapReduceService#connect(HadoopMapReduceConnectionData, Properties)
 */
public class HadoopMapReduceConnectionData extends AbstractHadoopConnectionData {
	private String fsMasterHost;
	private int fsMasterPort;
	private String jobtrackerHost;
	private int jobtrackerPort;
	private String fsUrlTemplate;

	/**
	 * Constructs new instance initializing information needed for connection.
	 * 
	 * @param fsMasterHost Host address for communicating with Hadoop cluster file system. For example, if the file
	 *        system is HDFS, this is host address of cluster namenode. May not be <code>null</code> nor empty.
	 * @param fsMasterPort Port for communication with Hadoop cluster file system. For example, if the file system is
	 *        HDFS, this is port used by namenode. May not be negative.
	 * @param fsUrlTemplate A template string that is provided by method
	 *        {@link HadoopFileSystemService#getFSMasterURLTemplate()}. This is formating template to be used as first
	 *        argument of {@link String#format(String, Object...)}. When following 2 attributes are file system host and
	 *        port the returned value of {@link String#format(String, Object...)} must be url for connecting to cluster
	 *        file system.
	 * @param jobtrackerHost Host address of jobtracker of the cluster. May not be <code>null</code> nor empty.
	 * @param jobtrackerPort Port for communication with jobtracker of the Hadoop cluster. May not be negative.
	 * @param user User that is connecting to Hadoop cluster.
	 */
	public HadoopMapReduceConnectionData(String fsMasterHost, int fsMasterPort, String fsUrlTemplate,
			String jobtrackerHost, int jobtrackerPort, String user) {
		super(user);
		if (fsMasterHost == null) {
			throw new NullPointerException("fsMasterHost");
		}
		if (jobtrackerHost == null) {
			throw new NullPointerException("jobtrackerHost");
		}
		if (fsUrlTemplate == null) {
			throw new NullPointerException("fsUrlTemplate");
		}
		if (!isCorrectFsUrlTemplate(fsUrlTemplate)) {
			throw new IllegalArgumentException("fsUrlTemplate is not correct template "
					+ "(is empty or does not contain % characters for host name and port)");
		}
		if (fsMasterPort < 0) {
			throw new IllegalArgumentException("fsMasterPort cannot be negative.");
		}
		if (jobtrackerPort < 0) {
			throw new IllegalArgumentException("jobtrackerPort cannot be negative.");
		}
		this.fsMasterHost = fsMasterHost;
		this.fsMasterPort = fsMasterPort;
		this.jobtrackerHost = jobtrackerHost;
		this.jobtrackerPort = jobtrackerPort;
		this.fsUrlTemplate = fsUrlTemplate;
	}

	/**
	 * Indicates if given string is correct {@link String#format(String, Object...)} template for Hadoop file system
	 * url.
	 * @param fsUrlTemplate The template in question.
	 * @return <code>true</code> iff the template string contains substring <code>"%s"</code> at least 2 times.
	 */
	protected boolean isCorrectFsUrlTemplate(String fsUrlTemplate) {
		return fsUrlTemplate.contains("%s") && fsUrlTemplate.replaceFirst("%s", "").contains("%");
	}

	/**
	 * Gets file system host address.
	 * @return Host address for communicating with Hadoop cluster file system. For example, if the file system is HDFS,
	 *         this is host address of cluster namenode. Granted to be non-null nor empty.
	 */
	public String getFsMasterHost() {
		return fsMasterHost;
	}

	/**
	 * Gets port for communicating with Hadoop file system.
	 * @return Port for communication with Hadoop cluster file system. For example, if the file system is HDFS, this is
	 *         port used by namenode.
	 */
	public int getFsMasterPort() {
		return fsMasterPort;
	}

	/**
	 * Gets jobtracter's host address.
	 * @return Host address of jobtracker of the cluster. Granted to be non-null nor empty string.
	 */
	public String getJobtrackerHost() {
		return jobtrackerHost;
	}

	/**
	 * Gets jobtracter's port.
	 * @return Port for communication with jobtracker of the Hadoop cluster.
	 */
	public int getJobtrackerPort() {
		return jobtrackerPort;
	}

	/**
	 * Gets template for creating Hadoop files system url from host address and port.
	 * @return Formating template to be used as first argument of {@link String#format(String, Object...)}. When
	 *         following 2 attributes are file system host and port the returned value of
	 *         {@link String#format(String, Object...)} must be url for connecting to cluster file system. Granted to be
	 *         non-null nor empty string.
	 */
	public String getFsUrlTemplate() {
		return fsUrlTemplate;
	}
}