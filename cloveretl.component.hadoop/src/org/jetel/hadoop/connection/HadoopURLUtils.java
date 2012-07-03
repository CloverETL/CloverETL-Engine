package org.jetel.hadoop.connection;

import java.net.URI;
import java.net.URL;

public class HadoopURLUtils {

	/** The name of HADOOP protocol. */
		public static final String HDFS_PROTOCOL = "hdfs";
		/** The URL prefix of HDFS URLs. */
		public static final String HDFS_PROTOCOL_URL_PREFIX = HDFS_PROTOCOL + "://";

		/**
		 * Checks whether or not a given URL string is a HDFS URL.
		 *
		 * @param url a string URL to be checked
		 *
		 * @return <code>true</code> if the given URL is a sandbox URL, <code>false</code> otherwise 
		 */
		public static boolean isHDFSUrl(String url) {
			return (url != null && url.startsWith(HDFS_PROTOCOL_URL_PREFIX));
		}

		/**
		 * Checks whether or not an URL is a sandbox URL.
		 *
		 * @param url an URL to be checked
		 *
		 * @return <code>true</code> if the given URL is a HDFS URL, <code>false</code> otherwise 
		 */
		public static boolean isHDFSUrl(URL url) {
			return (url != null && url.getProtocol().equals(HDFS_PROTOCOL));
		}

		/**
		 * Extracts a connection name from a given HDFS URL.
		 *
		 * @param hdfsUrl a HDFS URL
		 *
		 * @return a HDFS connection name extracted from the given HDFS URL
		 *
		 * @throws IllegalArgumentException if the given url is not a HDFS URL  
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
		public static String getPath(String hdfsUrl){
			try{
				final URI inputURI = URI.create(hdfsUrl);
				return inputURI.getPath();
				
			}catch(Exception ex){
				return "/";
			}
		}
		
		/**
		 * Extracts a relative URL from a given sandbox URL.
		 *
		 * @param hdfsUrl a sandbox URL
		 *
		 * @return a relative URL extracted from the given sandbox URL
		 *
		 * @throws IllegalArgumentException if the given url is not a sanbox URL  
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
