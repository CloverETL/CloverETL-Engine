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
package org.jetel.hadoop.connection;

import java.net.URI;
import java.net.URL;

public class HadoopURLUtils {

	/** The name of HADOOP protocol. */
	public static final String HDFS_PROTOCOL = "hdfs";
	/** The URL prefix of HDFS URLs. */
	public static final String HDFS_PROTOCOL_URL_PREFIX = HDFS_PROTOCOL + "://";

	/**
	 * Checks whether or not a given URL string is a Hadoop/HDFS URL.
	 * 
	 * @param url
	 *            a string URL to be checked
	 * 
	 * @return <code>true</code> if the given URL is a Hadoop/HDFS URL, <code>false</code> otherwise
	 */
	public static boolean isHDFSUrl(String url) {
		return (url != null && url.startsWith(HDFS_PROTOCOL_URL_PREFIX));
	}

	/**
	 * Checks whether or not an URL is a Hadoop/HDFS URL.
	 * 
	 * @param url
	 *            an URL to be checked
	 * 
	 * @return <code>true</code> if the given URL is a Hadoop/HDFS URL, <code>false</code> otherwise
	 */
	public static boolean isHDFSUrl(URL url) {
		return url != null && HDFS_PROTOCOL.equals(url.getProtocol());
	}

	/**
	 * Checks whether or not an URI is a Hadoop URI.
	 * 
	 * @param uri
	 *            an URI to be checked
	 * 
	 * @return <code>true</code> if the given URL is a Hadoop/HDFS URL, <code>false</code> otherwise
	 */
	public static boolean isHDFSUri(URI uri) {
		return uri != null && HDFS_PROTOCOL.equals(uri.getScheme());
	}

	/**
	 * Extracts a connection name from a given HDFS URL.
	 * 
	 * @param hdfsUrl
	 *            a HDFS URL
	 * 
	 * @return a HDFS connection name extracted from the given HDFS URL
	 * 
	 * @throws IllegalArgumentException
	 *             if the given url is not a HDFS URL
	 */
	public static String getConnectionName(String hdfsUrl) {
		if (!isHDFSUrl(hdfsUrl)) {
			throw new IllegalArgumentException("HDFS_Url");
		}

		int slashIndex = hdfsUrl.indexOf('/', HDFS_PROTOCOL_URL_PREFIX.length());

		if (slashIndex < 0) {
			slashIndex = hdfsUrl.length();
		}

		return hdfsUrl.substring(HDFS_PROTOCOL_URL_PREFIX.length(), slashIndex);
	}

	/**
	 * Extracts path portion of given HDFS URL.
	 * 
	 * @param hdfsUrl
	 * @return
	 */
	public static String getPath(String hdfsUrl) {
		try {
			final URI inputURI = URI.create(hdfsUrl);
			return inputURI.getPath();

		} catch (Exception ex) {
			return "/";
		}
	}

	/**
	 * Extracts a relative URL from a given sandbox URL.
	 * 
	 * @param hdfsUrl
	 *            a Hadoop/HDFS URL
	 * 
	 * @return a relative URL extracted from the given sandbox URL
	 * 
	 * @throws IllegalArgumentException
	 *             if the given url is not a Hadoop/HDFS URL
	 */
	public static String getRelativeUrl(String hdfsUrl) {
		if (!isHDFSUrl(hdfsUrl)) {
			throw new IllegalArgumentException("HDFS_Url");
		}

		int slashIndex = hdfsUrl.indexOf('/', HDFS_PROTOCOL_URL_PREFIX.length());

		if (slashIndex < 0 || slashIndex == hdfsUrl.length() - 1) {
			// there is just a HDFS connection name (and possibly a slash) present within the HDFS URL
			return ".";
		}

		return hdfsUrl.substring(slashIndex + 1);
	}

}
