package org.jetel.hadoop.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.ContextProvider;
import org.jetel.logger.SafeLog;
import org.jetel.logger.SafeLogFactory;
import org.jetel.util.file.CustomPathResolver;
import org.jetel.util.file.FileUtils;

public class HadoopPathResolver implements CustomPathResolver {

	public static String URL_HDFS_PROTOCOL_STRING = "hdfs";
	private static String URL_HDFS_PROTOCOL_STRING_UPPER = URL_HDFS_PROTOCOL_STRING.toUpperCase();
	private static final SafeLog log = SafeLogFactory.getSafeLog(FileUtils.class);
	
	@Override
	public InputStream getInputStream(URL contextURL, String input)
			throws IOException {
		
		if (input!=null && (input.startsWith(URL_HDFS_PROTOCOL_STRING) || input.startsWith(URL_HDFS_PROTOCOL_STRING_UPPER))){

			try{
				final URI inputURI = URI.create(input);

				final String hadoopConnName = inputURI.getHost();

				IConnection  conn = ContextProvider.getGraph().getConnection(hadoopConnName);
				if (conn==null)
					throw new IOException(String.format("Cannot find HDFS connection [%s] referenced in fileURL \"%s\".",hadoopConnName,input));
				if (!(conn instanceof HadoopConnection)){
					throw new IOException(String.format("Connection [%s:%s] is not of HDFS type.",conn.getId(),conn.getName()));
				}else{
					try {
						if(log.isDebugEnabled()) log.debug(String.format("Connecting to HDFS through [%s:%s] for reading.",conn.getId(),conn.getName()));
						return ((HadoopConnection)conn).getConnection().open(new URI(inputURI.getPath())).getDataInputStream();
					} catch (ComponentNotReadyException e) {
						throw new IOException("Cannot connect to HDFS - "+e.getMessage(),e);
					}
				}
			} catch (URISyntaxException e) {
				throw new IOException(String.format("Invalid file path: \"%s\"",input));
			}
		}else{
			return null;
		}
	}

	@Override
	public OutputStream getOutputStream(URL contextURL, String input,
			boolean appendData, int compressLevel) throws IOException {
		
		if (input!=null && (input.startsWith(URL_HDFS_PROTOCOL_STRING) || input.startsWith(URL_HDFS_PROTOCOL_STRING_UPPER))){

			try{
				final URI inputURI = URI.create(input);

				final String hadoopConnName = inputURI.getHost();
				
				IConnection  conn = ContextProvider.getGraph().getConnection(hadoopConnName);
				if (conn==null)
					throw new IOException(String.format("Cannot find HDFS connection [%s] referenced in fileURL \"%s\".",hadoopConnName,input));
				if (!(conn instanceof HadoopConnection)){
					throw new IOException(String.format("Connection [%s:%s] is not of HDFS type.",conn.getId(),conn.getName()));
				}else{
					try {
						if(log.isDebugEnabled()) log.debug(String.format("Connecting to HDFS through [%s:%s] for writing.",conn.getId(),conn.getName()));
						return ((HadoopConnection)conn).getConnection().create(new URI(inputURI.getPath()), !appendData).getDataOutputStream();
					} catch (ComponentNotReadyException e) {
						throw new IOException("Cannot connect to HDFS - "+e.getMessage(),e);
					} 
				}
			}catch (URISyntaxException e) {
				throw new IOException(String.format("Invalid file path: \"%s\"",input));
			}
		}else{
			return null;
		}
	}
}
