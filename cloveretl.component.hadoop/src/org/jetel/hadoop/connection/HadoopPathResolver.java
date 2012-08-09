package org.jetel.hadoop.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.ArrayList;
import java.util.List;

import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.TransformationGraph;
import org.jetel.logger.SafeLog;
import org.jetel.logger.SafeLogFactory;
import org.jetel.util.file.CustomPathResolver;
import org.jetel.util.file.FileUtils;

public class HadoopPathResolver implements CustomPathResolver {

	private static final SafeLog log = SafeLogFactory.getSafeLog(FileUtils.class);
	
	@Override
	public InputStream getInputStream(URL contextURL, String input)
			throws IOException {
		
		if (HadoopURLUtils.isHDFSUrl(input)){

			try{
				final URI inputURI = URI.create(input);

				final String hadoopConnName = inputURI.getHost();

				TransformationGraph graph=ContextProvider.getGraph();
				if (graph==null){
					throw new IOException(String.format("Internal error: Cannot find HDFS connection [%s] referenced in fileURL \"%s\". Missing TransformationGraph instance.",hadoopConnName,input));
				}
				
				IConnection  conn = graph.getConnection(hadoopConnName);
				if (conn==null)
					throw new IOException(String.format("Cannot find HDFS connection [%s] referenced in fileURL \"%s\".",hadoopConnName,input));
				if (!(conn instanceof HadoopConnection)){
					throw new IOException(String.format("Connection [%s:%s] is not of HDFS type.",conn.getId(),conn.getName()));
				}else{
					try {
						if(log.isDebugEnabled()) log.debug(String.format("Connecting to HDFS through [%s:%s] for reading.",conn.getId(),conn.getName()));
						IHadoopConnection hconn=((HadoopConnection)conn).getConnection();
						IHadoopInputStream istream=hconn.open(new URI(inputURI.getPath()));
						return istream.getDataInputStream();
						
					} catch (ComponentNotReadyException e) {
						log.warn(String.format("Cannot connect to HDFS - [%s:%s] - %s",e.getGraphElement().getId(),e.getGraphElement().getName(),e.getMessage()));
						throw new IOException("Cannot connect to HDFS - "+e.getMessage(),e);
					}
				}
			} catch (URISyntaxException e) {
				throw new IOException(String.format("Invalid file path: \"%s\"",input),e);
			} catch (IOException e){
				throw e;
			} catch (Exception e){
				throw new IOException(String.format("Unexpected error during processing file path: \"%s\"",input),e);
			}
		}else{
			return null;
		}
	}

	@Override
	public OutputStream getOutputStream(URL contextURL, String input,
			boolean appendData, int compressLevel) throws IOException {
		
		if (HadoopURLUtils.isHDFSUrl(input)){

			try{
				final URI inputURI = URI.create(input);

				final String hadoopConnName = inputURI.getHost();
				
				TransformationGraph graph=ContextProvider.getGraph();
				if (graph==null){
					throw new IOException(String.format("Internal error: Cannot find HDFS connection [%s] referenced in fileURL \"%s\". Missing TransformationGraph instance.",hadoopConnName,input));
				}
				
				IConnection  conn = graph.getConnection(hadoopConnName);
				if (conn==null)
					throw new IOException(String.format("Cannot find HDFS connection [%s] referenced in fileURL \"%s\".",hadoopConnName,input));
				if (!(conn instanceof HadoopConnection)){
					throw new IOException(String.format("Connection [%s:%s] is not of HDFS type.",conn.getId(),conn.getName()));
				}else{
					try {
						if(log.isDebugEnabled()) log.debug(String.format("Connecting to HDFS through [%s:%s] for writing.",conn.getId(),conn.getName()));
						return ((HadoopConnection)conn).getConnection().create(new URI(inputURI.getPath()), !appendData).getDataOutputStream();
					} catch (ComponentNotReadyException e) {
						log.warn(String.format("Cannot connect to HDFS - [%s:%s] - %s",e.getGraphElement().getId(),e.getGraphElement().getName(),e.getMessage()));
						throw new IOException("Cannot connect to HDFS - "+e.getMessage(),e);
					} 
				}
			}catch (URISyntaxException e) {
				throw new IOException(String.format("Invalid file path: \"%s\"",input));
			}catch (Exception e){
				throw new IOException(String.format("Unexpected error during processing file path: \"%s\"",input),e);
			}
		}else{
			return null;
		}
	}

	@Override
	public URL getURL(URL contextURL, String fileURL) throws MalformedURLException {
		if (HadoopURLUtils.isHDFSUrl(fileURL))
			return new URL(contextURL,fileURL,new HadoopStreamHandler());
		else
			throw new MalformedURLException("Not a Hadoop/HDFS URL: "+fileURL);
	}

	 protected class HadoopStreamHandler extends URLStreamHandler{
		
		@Override
		public URLConnection openConnection(URL url) throws IOException {
			return new HadoopStreamConnection(url);
		}
		
	}
	 
	 protected class HadoopStreamConnection extends URLConnection{

		protected HadoopStreamConnection(URL arg0) {
			super(arg0);
		}

		@Override
		public void connect() throws IOException {
			TransformationGraph graph=ContextProvider.getGraph();
			if (graph==null){
				throw new IOException(String.format("Internal error: Cannot find HDFS connection [%s] referenced in fileURL \"%s\". Missing TransformationGraph instance.",this.url.getProtocol(),this.url.toString()));
			}
			
			IConnection  conn = graph.getConnection(this.url.getProtocol());
			if (conn==null)
				throw new IOException(String.format("Cannot find HDFS connection [%s] referenced in fileURL \"%s\".",this.url.getProtocol(),this.url.toString()));
			if (!(conn instanceof HadoopConnection)){
				throw new IOException(String.format("Connection [%s:%s] is not of HDFS type.",conn.getId(),conn.getName()));
			}else{
				try {
					conn.init();
					if(log.isDebugEnabled()) log.debug(String.format("Connecting to HDFS through [%s:%s] for reading.",conn.getId(),conn.getName()));
					((HadoopConnection)conn).getConnection(); // just testing that we can connect, we don't store the connection
				} catch (ComponentNotReadyException e) {
					log.warn(String.format("Cannot connect to HDFS - [%s:%s] - %s",e.getGraphElement().getId(),e.getGraphElement().getName(),e.getMessage()));
					throw new IOException("Cannot connect to HDFS - "+e.getMessage(),e);
				}
				// release connection
				conn.free();
			}
		}
		
		@Override
		public InputStream getInputStream() throws IOException{
			return HadoopPathResolver.this.getInputStream(null, this.url.toString());		
		}
		
		public OutputStream getOutputStream() throws IOException{
			return HadoopPathResolver.this.getOutputStream(null, this.url.toString(), false, 0);
		}
		 
	 }

	@Override
	public boolean handlesURL(URL contextURL, String fileURL) {
		return HadoopURLUtils.isHDFSUrl(fileURL);
	}

	@Override
	public List<String> resolveWildcardURL(URL contextURL, String fileURL)
			throws MalformedURLException {
		if (HadoopURLUtils.isHDFSUrl(fileURL)){

			try{
				final URI inputURI = URI.create(fileURL);

				final String hadoopConnName = inputURI.getHost();

				TransformationGraph graph=ContextProvider.getGraph();
				if (graph==null){
					throw new MalformedURLException();
				}

				IConnection  conn = graph.getConnection(hadoopConnName);
				if (conn==null)
					throw new MalformedURLException();
				if (!(conn instanceof HadoopConnection)){
					throw new MalformedURLException();
				}else{
					conn.init();
					String uriPath=inputURI.getPath();
					String matchPattern= uriPath.replaceAll("\\?", ".?").replaceAll("\\*", ".*");
					
					HadoopFileStatus[] statuses=((HadoopConnection)conn).getConnection().listStatus(new URI(uriPath.substring(0,uriPath.lastIndexOf("/"))));
					List<String> filenames=new ArrayList<String>(statuses.length);

					for(HadoopFileStatus status: statuses){
						if (status.getFile().getPath().matches(matchPattern)){
							filenames.add(new StringBuilder(HadoopURLUtils.HDFS_PROTOCOL_URL_PREFIX).append(hadoopConnName).append(status.getFile().getPath()).toString());
						}
					}
					return filenames;
				}
			}catch(Exception ex){
				throw new MalformedURLException();
			}
		}
		throw new MalformedURLException();
	}

}
