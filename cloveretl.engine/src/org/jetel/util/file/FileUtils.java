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
package org.jetel.util.file;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.URLStreamHandler;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.jetel.component.fileoperation.CloverURI;
import org.jetel.component.fileoperation.FileManager;
import org.jetel.data.Defaults;
import org.jetel.enums.ArchiveType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.TransformationGraph;
import org.jetel.logger.SafeLog;
import org.jetel.logger.SafeLogFactory;
import org.jetel.util.MultiOutFile;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.protocols.amazon.S3InputStream;
import org.jetel.util.protocols.amazon.S3OutputStream;
import org.jetel.util.protocols.ftp.FTPStreamHandler;
import org.jetel.util.protocols.proxy.ProxyHandler;
import org.jetel.util.protocols.proxy.ProxyProtocolEnum;
import org.jetel.util.protocols.sandbox.SandboxStreamHandler;
import org.jetel.util.protocols.sftp.SFTPConnection;
import org.jetel.util.protocols.sftp.SFTPStreamHandler;
import org.jetel.util.protocols.webdav.WebdavOutputStream;
import org.jetel.util.stream.StreamUtils;

import com.ice.tar.TarEntry;
import com.ice.tar.TarInputStream;
import com.jcraft.jsch.ChannelSftp;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
/**
 *  Helper class with some useful methods regarding file manipulation
 *
 * @author      dpavlis
 * @since       May 24, 2002
 * @revision    $Revision$
 */
public class FileUtils {

	private final static String DEFAULT_ZIP_FILE = "default_output";
	
	// for embedded source
	//     "something   :       (         something   )       [#something]?
	//      ([^:]*)     (:)     (\\()     (.*)        (\\))   (((#)(.*))|($))
	private final static Pattern INNER_SOURCE = Pattern.compile("(([^:]*)([:])([\\(]))(.*)(\\))(((#)(.*))|($))");

	// standard input/output source
	public final static String STD_CONSOLE = "-";
	
	// sftp protocol handler
	public static final SFTPStreamHandler sFtpStreamHandler = new SFTPStreamHandler();

	// ftp protocol handler
	public static final FTPStreamHandler ftpStreamHandler = new FTPStreamHandler();

	// proxy protocol handler
	public static final ProxyHandler proxyHandler = new ProxyHandler();

	// file protocol name
	private static final String FILE_PROTOCOL = "file";
	private static final String FILE_PROTOCOL_ABSOLUTE_MARK = "file:./";
	
	// archive protocol names
	private static final String TAR_PROTOCOL = "tar";
	private static final String GZIP_PROTOCOL = "gzip";
	private static final String ZIP_PROTOCOL = "zip";
	
    private static final ArchiveURLStreamHandler ARCHIVE_URL_STREAM_HANDLER = new ArchiveURLStreamHandler();
    
	private static final URLStreamHandler HTTP_HANDLER = new CredentialsSerializingHandler() {

		@Override
		protected int getDefaultPort() {
			return 80;
		}
	};
    
	private static final URLStreamHandler HTTPS_HANDLER = new CredentialsSerializingHandler() {

		@Override
		protected int getDefaultPort() {
			return 443;
		}
	};

	private static final SafeLog log = SafeLogFactory.getSafeLog(FileUtils.class);

	public static final Map<String, URLStreamHandler> handlers;

	private static final String FTP_PROTOCOL = "ftp";

	private static final String SFTP_PROTOCOL = "sftp";

	private static final String SCP_PROTOCOL = "scp";
	
	private static final String HTTP_PROTOCOL = "http";
	private static final String HTTPS_PROTOCOL = "https";
	
	static {
		Map<String, URLStreamHandler> h = new HashMap<String, URLStreamHandler>();
		h.put(GZIP_PROTOCOL, ARCHIVE_URL_STREAM_HANDLER);
		h.put(ZIP_PROTOCOL, ARCHIVE_URL_STREAM_HANDLER);
		h.put(TAR_PROTOCOL, ARCHIVE_URL_STREAM_HANDLER);
		h.put(FTP_PROTOCOL, ftpStreamHandler);
		h.put(SFTP_PROTOCOL, sFtpStreamHandler);
		h.put(SCP_PROTOCOL, sFtpStreamHandler);
		h.put(HTTP_PROTOCOL, HTTP_HANDLER);
		h.put(HTTPS_PROTOCOL, HTTPS_HANDLER);
		for (ProxyProtocolEnum p: ProxyProtocolEnum.values()) {
			h.put(p.toString(), proxyHandler);
		}
		h.put(SandboxUrlUtils.SANDBOX_PROTOCOL, new SandboxStreamHandler());
		handlers = Collections.unmodifiableMap(h);
	}
	
	/**
	 * Third-party implementation of path resolving - useful to make possible to run the graph inside of war file.
	 */
    private static final List<CustomPathResolver> customPathResolvers = new ArrayList<CustomPathResolver>();
    private static final String PLUS_CHAR_ENCODED = URLEncoder.encode("+");

    /**
     * Used only to extract the protocol name in a generic manner
     */
	private static final URLStreamHandler GENERIC_HANDLER = new URLStreamHandler() {
		@Override
		protected URLConnection openConnection(URL u) throws IOException {
			return null;
		}
	};

	/**
	 * If the given string is an valid absolute URL
	 * with a known protocol, returns the URL.
	 * 
	 * Throws MalformedURLException otherwise.
	 * 
	 * @param url
	 * @return URL constructed from <code>url</code>
	 * @throws MalformedURLException
	 */
	public static final URL getUrl(String url) throws MalformedURLException {
		try {
			return new URL(url);
		} catch (MalformedURLException e) {
			String protocol = new URL(null, url, GENERIC_HANDLER).getProtocol();
			URLStreamHandler handler = FileUtils.handlers.get(protocol.toLowerCase());
			if (handler != null) {
				return new URL(null, url, handler);
			} else {
				throw e;
			}
		}
	}

    public static URL getFileURL(String fileURL) throws MalformedURLException {
    	return getFileURL((URL) null, fileURL);
    }

    public static URL getFileURL(String contextURL, String fileURL) throws MalformedURLException {
    	return getFileURL(getFileURL((URL) null, contextURL), fileURL);
    }
    /**
    * Creates URL object based on specified fileURL string. Handles
    * situations when <code>fileURL</code> contains only path to file
    * <i>(without "file:" string)</i>. 
    * 
    * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
    * @param fileURL   string containing file URL
    * @return  URL object or NULL if object can't be created (due to Malformed URL)
    * @throws MalformedURLException  
    */
    public static URL getFileURL(URL contextURL, String fileURL) throws MalformedURLException {
    	//FIX CLD-2895
    	//default value for addStrokePrefix was changed to true
    	//right now I am not sure if this change can have an impact to other part of project,
    	//but it seems this changed fix relatively important issue:
    	//
    	//contextURL "file:/c:/project/"
    	//fileURL "c:/otherProject/data.txt"
    	//leads to --> "file:/c:/project/c:/otherProject/data.txt"
    	return getFileURL(contextURL, fileURL, true);
    }
    
    /**
     * Creates URL object based on specified fileURL string. Handles
     * situations when <code>fileURL</code> contains only path to file
     * <i>(without "file:" string)</i>. 
     * 
     * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
     * @param fileURL   string containing file URL
     * @return  URL object or NULL if object can't be created (due to Malformed URL)
     * @throws MalformedURLException  
     */
    public static URL getFileURL(URL contextURL, String fileURL, boolean addStrokePrefix) throws MalformedURLException {
    	// remove mark for absolute path
    	if (contextURL != null && fileURL.startsWith(FILE_PROTOCOL_ABSOLUTE_MARK)) {
    		fileURL = fileURL.substring((FILE_PROTOCOL+":").length());
    	}
    	
    	// standard url
        try {
        	if( fileURL.startsWith("/") ){
                return new URL(fileURL);
        	} else {
        		return getStandardUrlWeblogicHack(contextURL, fileURL);
        	}
        } catch(MalformedURLException ex) {}

        // sftp url
    	try {
        	// ftp connection is connected via sftp handler, 22 port is ok but 21 is somehow blocked for the same connection
    		return new URL(contextURL, fileURL, sFtpStreamHandler);
        } catch(MalformedURLException e) {}

        // proxy url
    	try {
    		return new URL(contextURL, fileURL, proxyHandler);
        } catch(MalformedURLException e) {}

        // sandbox url
		if (SandboxUrlUtils.isSandboxUrl(fileURL)){
        	try {
        		return new URL(contextURL, fileURL, new SandboxStreamHandler());
            } catch(MalformedURLException e) {}
		}
        
		// archive url
		if (isArchive(fileURL)) {
			StringBuilder innerInput = new StringBuilder();
			StringBuilder anchor = new StringBuilder();
			ArchiveType type = getArchiveType(fileURL, innerInput, anchor);
			URL archiveFileUrl = getFileURL(contextURL, innerInput.toString());
			return new URL(null, type.getId() + ":(" + archiveFileUrl.toString() + ")#" + anchor, new ArchiveURLStreamHandler(contextURL));
		}
		
        // file url
		String prefix = FILE_PROTOCOL + ":";
		if (addStrokePrefix && new File(fileURL).isAbsolute() && !fileURL.startsWith("/")) {
			prefix += "/";
		}
        return new URL(contextURL, prefix + fileURL);
    }
    
    /**
     * method created as workaround to issue https://bug.javlin.eu/browse/CLS-886
     * 
     * weblogic implementation of java.net.URLStreamHandler - weblogic.net.http.Handler
     * does not print credentials in its toExternalForm(URL u) method.
     * 
     * On any non-weblogic platform this method can be replaced by 
     * new URL(URL context, String spec);
     * 
     * Method enforce using the Sun handler implementation for http and https protocols
     * @throws MalformedURLException 
     */
    private static URL getStandardUrlWeblogicHack(URL contextUrl, String fileUrl) throws MalformedURLException {
    	if (contextUrl != null || fileUrl != null) {
    		final URL resolvedInContextUrl = new URL(contextUrl, fileUrl);
    		String protocol = resolvedInContextUrl.getProtocol();
    		if (protocol != null) {
    			protocol = protocol.toLowerCase();
    			if (protocol.equals(HTTP_PROTOCOL)) {
        	    	return new URL(contextUrl, fileUrl, FileUtils.HTTP_HANDLER);
    			} else if (protocol.equals(HTTPS_PROTOCOL)) {
        	    	return new URL(contextUrl, fileUrl, FileUtils.HTTPS_HANDLER);
    			}
    		}
    	}
    	return new URL(contextUrl, fileUrl);
    }
    
    /**
     * Converts a list of file URLs to URL objects by calling {@link #getFileURL(URL, String)}.
     *
     * @param contextUrl URL context for converting relative paths to absolute ones
     * @param fileUrls array of string file URLs
     *
     * @return an array of file URL objects
     *
     * @throws MalformedURLException if any of the given file URLs is malformed
     */
	public static URL[] getFileUrls(URL contextUrl, String[] fileUrls) throws MalformedURLException {
		if (fileUrls == null) {
			return null;
		}

		URL[] resultFileUrls = new URL[fileUrls.length];

		for (int i = 0; i < fileUrls.length; i++) {
			resultFileUrls[i] = getFileURL(contextUrl, fileUrls[i]);
		}

		return resultFileUrls;
	}

	/**
	 *  Calculates checksum of specified file.<br>Is suitable for short files (not very much buffered).
	 *
	 * @param  filename  Filename (full path) of file to calculate checksum for
	 * @return           Calculated checksum or -1 if some error (IO) occured
	 */
	public static long calculateFileCheckSum(String filename) {
		byte[] buffer = new byte[1024];
		Checksum checksum = new Adler32(); // we use Adler - should be faster
		int length = 0;
		try {
			FileInputStream dataFile = new FileInputStream(filename);
			try {
				while (length != -1) {
					length = dataFile.read(buffer);
					if (length > 0) {
						checksum.update(buffer, 0, length);
					}
				}
			} finally {
				dataFile.close();
			}

		} catch (IOException ex) {
			return -1;
		}
		return checksum.getValue();
	}

	/**
	 * Reads file specified by URL. The content is returned as String
     * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
	 * @param fileURL URL specifying the location of file from which to read
	 * @return
	 */
	public static String getStringFromURL(URL contextURL, String fileURL, String charset){
        InputStream source;
        String chSet = charset != null ? charset : Defaults.DataParser.DEFAULT_CHARSET_DECODER;
		StringBuffer sb = new StringBuffer(2048);
        char[] charBuf=new char[256];
		try {
			source = FileUtils.getInputStream(contextURL, fileURL);
			BufferedReader in = new BufferedReader(new InputStreamReader(source, chSet));
			try {
				int readNum;

				while ((readNum = in.read(charBuf, 0, charBuf.length)) != -1) {
					sb.append(charBuf, 0, readNum);
				}
			} finally {
				in.close();
			}
		} catch (IOException ex) {
			throw new RuntimeException("Can't get string from file " + fileURL + " : " + ex.getMessage());
		}
        return sb.toString();
	}
	
    /**
	 * Creates ReadableByteChannel from the url definition.
	 * <p>
	 * All standard url format are acceptable plus extended form of url by zip & gzip construction:
	 * </p>
	 * Examples:
	 * <dl>
	 * <dd>zip:&lt;url_to_zip_file&gt;#&lt;inzip_path_to_file&gt;</dd>
	 * <dd>gzip:&lt;url_to_gzip_file&gt;</dd>
	 * </dl>
	 * 
	 * @param contextURL
	 *            context URL for converting relative to absolute path (see TransformationGraph.getProjectURL())
	 * @param input
	 *            URL of file to read
	 * @return
	 * @throws IOException
	 */
	public static ReadableByteChannel getReadableChannel(URL contextURL, String input) throws IOException {
    	InputStream in = getInputStream(contextURL, input);
    	//incremental reader needs FileChannel:
    	return in instanceof FileInputStream ? ((FileInputStream)in).getChannel() : Channels.newChannel(in);
    }	

	/**
	 * Creates InputStream from the url definition.
	 * <p>
	 * All standard url format are acceptable plus extended form of url by zip & gzip construction:
	 * </p>
	 * Examples:
	 * <dl>
	 * <dd>zip:&lt;url_to_zip_file&gt;#&lt;inzip_path_to_file&gt;</dd>
	 * <dd>gzip:&lt;url_to_gzip_file&gt;</dd>
	 * </dl>
	 * For zip file, if anchor is not set there is return first zip entry.
	 * 
	 * @param contextURL
	 *            context URL for converting relative to absolute path (see TransformationGraph.getProjectURL())
	 * @param input
	 *            URL of file to read
	 * @return
	 * @throws IOException 
	 * @throws IOException
	 */
    public static InputStream getInputStream(URL contextURL, String input) throws IOException {
        InputStream innerStream = null;
        
        // It is important to use the same mechanism (TrueZip) for both reading and writing
        // of the local archives!
        //
        // The TrueZip library has it's own virtual file system. Even after a stream from TrueZip
        // gets closed, the contents are not immediately written to the disc.
        // Therefore reading local archive with different library (e.g. java.util.zip) might lead to
        // inconsistency if the archive has not been flushed by TrueZip yet.
        StringBuilder localArchivePath = new StringBuilder();
        if (getLocalArchiveInputPath(contextURL, input, localArchivePath)) {
        	// apply the contextURL
        	URL url = FileUtils.getFileURL(contextURL, localArchivePath.toString());
			String absolutePath = getUrlFile(url);
			registerTrueZipVSFEntry(new de.schlichtherle.io.File(localArchivePath.toString()));
			return new de.schlichtherle.io.FileInputStream(absolutePath);
        }

        //first we try the custom path resolvers
    	for (CustomPathResolver customPathResolver : customPathResolvers) {
    		innerStream = customPathResolver.getInputStream(contextURL, input);
    		if (innerStream != null) {
    			log.debug("Input stream '" + input + "[" + contextURL + "] was opened by custom path resolver.");
    			return innerStream; //we found the desired input stream using external resolver method
    		}
    	}
    	
		// std input (console)
		if (input.equals(STD_CONSOLE)) {
			return System.in;
		}

        // get inner source
		Matcher matcher = FileURLParser.getURLMatcher(input);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			// get and set proxy and go to inner source
			Proxy proxy = getProxy(innerSource);
			String proxyUserInfo = null;
			input = matcher.group(2) + matcher.group(3) + matcher.group(7);
			if (proxy != null) {
				try {
					proxyUserInfo = new URI(innerSource).getUserInfo();
				} catch (URISyntaxException ex) {
				}
			}

			innerStream = proxy == null ? getInputStream(contextURL, innerSource) 
					: getAuthorizedConnection(getFileURL(contextURL, input), proxy, proxyUserInfo).getInputStream();
		}
		
		// get archive type
		StringBuilder sbAnchor = new StringBuilder();
		StringBuilder sbInnerInput = new StringBuilder();
		ArchiveType archiveType = getArchiveType(input, sbInnerInput, sbAnchor);
		input = sbInnerInput.toString();

        //open channel
        URL url = null;
        if (innerStream == null) {
        	url = FileUtils.getFileURL(contextURL, input);
        	
        	// creates file input stream for incremental reading (random access file)
        	if (archiveType == null && url.getProtocol().equals(FILE_PROTOCOL)) {
            	return new FileInputStream(url.getRef() != null ? getUrlFile(url) + "#" + url.getRef() : getUrlFile(url));
        	} else if (archiveType == null && SandboxUrlUtils.isSandboxUrl(url)) {
        		TransformationGraph graph = ContextProvider.getGraph();
        		if (graph == null) {
					throw new NullPointerException("Graph reference cannot be null when \"" + SandboxUrlUtils.SANDBOX_PROTOCOL + "\" protocol is used.");
        		}
        		return graph.getAuthorityProxy().getSandboxResourceInput(url.getHost(), getUrlFile(url));
        	}
        	
        	try {
        		if (S3InputStream.isS3File(url)) {
        			return new S3InputStream(url);
        		}
        		try {
        			innerStream = getAuthorizedConnection(url).getInputStream();
        		}
        		catch (Exception e) {
        			throw new IOException("Cannot obtain connection input stream for URL '" + url + "'. Make sure the URL is valid.", e);
        		}
        	} catch (IOException e) {
				log.debug("IOException occured for URL - host: '" + url.getHost() + "', path: '" + url.getPath() + "' (user info not shown)",
						  "IOException occured for URL - host: '" + url.getHost() + "', userinfo: '" + url.getUserInfo() + "', path: '" + url.getPath() + "'");
				throw e;
        	}
        }

        // create archive streams
        if (archiveType == ArchiveType.ZIP) {
        	List<InputStream> lIs = getZipInputStreamsInner(innerStream, sbAnchor.toString(), 0, null);
        	return lIs.size() > 0 ? lIs.get(0) : null;
        } else if (archiveType == ArchiveType.GZIP) {
            return new GZIPInputStream(innerStream, Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
        } else if (archiveType == ArchiveType.TAR) {
        	List<InputStream> lIs = getTarInputStreamsInner(innerStream, sbAnchor.toString(), 0, null);
        	return lIs.size() > 0 ? lIs.get(0) : null;
        }
        
        return innerStream;
    }

    /**
     * Gets archive type.
     * @param input - input file
     * @param innerInput - output parameter
     * @param anchor - output parameter
     * @return
     */
    public static ArchiveType getArchiveType(String input, StringBuilder innerInput, StringBuilder anchor) {
    	// result value
    	ArchiveType archiveType = null;
    	
        //resolve url format for zip files
    	if (input.startsWith("zip:")) archiveType = ArchiveType.ZIP;
    	else if (input.startsWith("tar:")) archiveType = ArchiveType.TAR;
    	else if (input.startsWith("gzip:")) archiveType = ArchiveType.GZIP;
    	
    	// parse the archive
        if((archiveType == ArchiveType.ZIP) || (archiveType == ArchiveType.TAR)) {
        	if (input.contains(")#")) {
                anchor.append(input.substring(input.lastIndexOf(")#") + 2));
                innerInput.append(input.substring(input.indexOf(":(") + 2, input.lastIndexOf(")#")));
        	}
        	else if (input.contains("#")) {
                anchor.append(input.substring(input.lastIndexOf('#') + 1));
                innerInput.append(input.substring(input.indexOf(':') + 1, input.lastIndexOf('#')));
        	}
        	else {
                anchor = null;
                innerInput.append(input.substring(input.indexOf(':') + 1));
        	}
        }
        else if (archiveType == ArchiveType.GZIP) {
        	innerInput.append(input.substring(input.indexOf(':') + 1));
        }
        
        // if doesn't exist inner input, inner input is input
        if (innerInput.length() == 0) innerInput.append(input);
        
        // remove parentheses - fixes incorrect URL resolution
        if (innerInput.length() >= 2 && innerInput.charAt(0) == '(' && innerInput.charAt(innerInput.length()-1) == ')') {
        	innerInput.deleteCharAt(innerInput.length()-1);
        	innerInput.deleteCharAt(0);
        }
        
        return archiveType;
    }
    
    /**
     * Creates a zip input stream.
     * @param innerStream
     * @param anchor
     * @param resolvedAnchors - output parameter
     * @return
     * @throws IOException
     */
    public static List<InputStream> getZipInputStreams(InputStream innerStream, String anchor, List<String> resolvedAnchors) throws IOException {
    	return getZipInputStreamsInner(innerStream, anchor, 0, resolvedAnchors);
    }

    /**
     * Creates list of zip input streams.
     * @param innerStream
     * @param anchor
     * @param matchFilesFrom
     * @param resolvedAnchors
     * @return
     * @throws IOException
     */
    private static List<InputStream> getZipInputStreamsInner(InputStream innerStream, String anchor, 
    		int matchFilesFrom, List<String> resolvedAnchors) throws IOException {
    	// result list of input streams
    	List<InputStream> streams = new ArrayList<InputStream>();

    	// check and prepare support for wild card matching
        Matcher matcher;
        Pattern WILDCARD_PATTERS = null;
        boolean bWildsCardedAnchor = anchor.contains("?") || anchor.contains("*");
        if (bWildsCardedAnchor) 
        	WILDCARD_PATTERS = Pattern.compile(anchor.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\.", "\\\\.").replaceAll("\\?", "\\.").replaceAll("\\*", ".*"));

    	// the input stream must support a buffer for wild cards.
        if (bWildsCardedAnchor) {
        	if (!innerStream.markSupported()) {
        		innerStream = new BufferedInputStream(innerStream);
        	}
    		innerStream.mark(Integer.MAX_VALUE);
        }
    	
        //resolve url format for zip files
        ZipInputStream zin = new ZipInputStream(innerStream) ;     
        ZipEntry entry;

        // find entries
        int iMatched = 0;
        while((entry = zin.getNextEntry()) != null) {	// zin is changing -> recursion !!!
            // wild cards
            if (bWildsCardedAnchor) {
           		matcher = WILDCARD_PATTERS.matcher(entry.getName());
           		if (matcher.find() && iMatched++ == matchFilesFrom) {
                	streams.add(zin);
                	if (resolvedAnchors != null) resolvedAnchors.add(entry.getName());
                	innerStream.reset();
                	streams.addAll(getZipInputStreamsInner(innerStream, anchor, ++matchFilesFrom, resolvedAnchors));
                	innerStream.reset();
                	return streams;
                }
            
        	// without wild cards
            } else if(anchor == null || anchor.equals("") || entry.getName().equals(anchor)) { //url is given without anchor; first entry in zip file is used
               	streams.add(zin);
               	if (resolvedAnchors != null) resolvedAnchors.add(anchor);
               	return streams;
            }
            
            //finish up with entry
            zin.closeEntry();
        }
        if (matchFilesFrom > 0 || streams.size() > 0) return streams;
        
        // if no wild carded entry found, it is ok, return null
        if (bWildsCardedAnchor) return null;
        
        //close the archive
        zin.close();
        
        //no channel found report
        if(anchor == null) {
            throw new IOException("Zip file is empty.");
        } else {
            throw new IOException("Wrong anchor (" + anchor + ") to zip file.");
        }
    }

    /**
     * Creates a tar input stream.
     * @param innerStream
     * @param anchor
     * @param resolvedAnchors - output parameter
     * @return
     * @throws IOException
     */
    public static List<InputStream> getTarInputStreams(InputStream innerStream, String anchor, List<String> resolvedAnchors) throws IOException {
    	return getTarInputStreamsInner(innerStream, anchor, 0, resolvedAnchors);
    }

    /**
     * Creates list of tar input streams.
     * @param innerStream
     * @param anchor
     * @param matchFilesFrom
     * @param resolvedAnchors
     * @return
     * @throws IOException
     */
    private static List<InputStream> getTarInputStreamsInner(InputStream innerStream, String anchor, 
    		int matchFilesFrom, List<String> resolvedAnchors) throws IOException {
    	// result list of input streams
    	List<InputStream> streams = new ArrayList<InputStream>();

    	// check and prepare support for wild card matching
        Matcher matcher;
        Pattern WILDCARD_PATTERS = null;
        boolean bWildsCardedAnchor = anchor.contains("?") || anchor.contains("*");
        if (bWildsCardedAnchor) 
        	WILDCARD_PATTERS = Pattern.compile(anchor.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\.", "\\\\.").replaceAll("\\?", "\\.").replaceAll("\\*", ".*"));

    	// the input stream must support a buffer for wild cards.
        if (bWildsCardedAnchor) {
        	if (!innerStream.markSupported()) {
        		innerStream = new BufferedInputStream(innerStream);
        	}
    		innerStream.mark(Integer.MAX_VALUE);
        }
    	
        //resolve url format for zip files
    	TarInputStream tin = new TarInputStream(innerStream);
        TarEntry entry;

        // find entries
        int iMatched = 0;
        while((entry = tin.getNextEntry()) != null) {	// tin is changing -> recursion !!!
            // wild cards
            if (bWildsCardedAnchor) {
           		matcher = WILDCARD_PATTERS.matcher(entry.getName());
           		if (matcher.find() && iMatched++ == matchFilesFrom) {
                	streams.add(tin);
                	if (resolvedAnchors != null) resolvedAnchors.add(entry.getName());
                	innerStream.reset();
                	streams.addAll(getTarInputStreamsInner(innerStream, anchor, ++matchFilesFrom, resolvedAnchors));
                	innerStream.reset();
                	return streams;
                }
            
        	// without wild cards
            } else if(anchor == null || anchor.equals("") || entry.getName().equals(anchor)) { //url is given without anchor; first entry in zip file is used
               	streams.add(tin);
               	if (resolvedAnchors != null) resolvedAnchors.add(anchor);
               	return streams;
            }
        }
        if (matchFilesFrom > 0 || streams.size() > 0) return streams;
        
        // if no wild carded entry found, it is ok, return null
        if (bWildsCardedAnchor) return null;
        
        //close the archive
        tin.close();
        
        //no channel found report
        if(anchor == null) {
            throw new IOException("Tar file is empty.");
        } else {
            throw new IOException("Wrong anchor (" + anchor + ") to tar file.");
        }
    }

	/**
     * Creates authorized url connection.
     * @param url
     * @return
     * @throws IOException
     */
    public static URLConnection getAuthorizedConnection(URL url) throws IOException {
        return URLConnectionRequest.getAuthorizedConnection(
        		url.openConnection(), 
        		url.getUserInfo(), 
        		URLConnectionRequest.URL_CONNECTION_AUTHORIZATION);
    }

    /**
     * Creates an authorized stream.
     * @param url
     * @param proxy
     * @param proxyUserInfo username and password to access the proxy
     * 
     * @return
     * @throws IOException
     */
    public static URLConnection getAuthorizedConnection(URL url, Proxy proxy, String proxyUserInfo) throws IOException {
        return URLConnectionRequest.getAuthorizedConnection(
        		url.openConnection(proxy),
        		proxyUserInfo, 
        		URLConnectionRequest.URL_CONNECTION_PROXY_AUTHORIZATION);
    }

    /**
     * Creates an authorized stream.
     * @param url
     * @return
     * @throws IOException
     * 
     * @deprecated Use {@link #getAuthorizedConnection(URL, Proxy, String)} instead
     */
    @Deprecated
    public static URLConnection getAuthorizedConnection(URL url, Proxy proxy) throws IOException {
    	// user info is obtained from the URL, most likely different from proxy user info 
        return getAuthorizedConnection(url, proxy, url.getUserInfo());
    }

    /**
     * Creates an proxy from the file url string.
     * @param fileURL
     * @return
     */
    public static Proxy getProxy(String fileURL) {
    	// create an url
    	URL url;
    	try {
			url = getFileURL(fileURL);
		} catch (MalformedURLException e) {
			return null;
		}
		// get proxy type
		ProxyProtocolEnum proxyProtocolEnum;
    	if ((proxyProtocolEnum = ProxyProtocolEnum.fromString(url.getProtocol())) == null) {
    		return null;
    	}
		// no proxy
    	if (proxyProtocolEnum == ProxyProtocolEnum.NO_PROXY) {
    		return Proxy.NO_PROXY;
    	}
    	// create a proxy
    	SocketAddress addr = new InetSocketAddress(url.getHost(), url.getPort() < 0 ? 8080 : url.getPort());
    	Proxy proxy = new Proxy(Proxy.Type.valueOf(proxyProtocolEnum.getProxyString()), addr);
		return proxy;
	}

	/**
     * Creates WritableByteChannel from the url definition.
     * <p>All standard url format are acceptable (including ftp://) plus extended form of url by zip & gzip construction:</p>
     * Examples:
     * <dl>
     *  <dd>zip:&lt;url_to_zip_file&gt;#&lt;inzip_path_to_file&gt;</dd>
     *  <dd>gzip:&lt;url_to_gzip_file&gt;</dd>
     * </dl>
     * @param contextURL context URL for converting relative to absolute path (see TransformationGraph.getProjectURL()) 
	 * @param input
	 * @param appendData - for file and ftp
	 * @param compressLevel - for zip 
	 * @return
	 * @throws IOException
	 */
	public static WritableByteChannel getWritableChannel(URL contextURL, String input, boolean appendData, int compressLevel) throws IOException {
		return Channels.newChannel(getOutputStream(contextURL, input, appendData, compressLevel));
	}

	public static WritableByteChannel getWritableChannel(URL contextURL, String input, boolean appendData) throws IOException {
		return getWritableChannel(contextURL, input, appendData, -1);
	}

	public static boolean isArchive(String input) {
		return input.startsWith("zip:") || input.startsWith("tar:") || input.startsWith("gzip:");
	}

	private static boolean isZipArchive(String input) {
		return input.startsWith("zip:");
	}
	
	public static boolean isRemoteFile(String input) {
		return input.startsWith("http:")
			|| input.startsWith("https:")
			|| input.startsWith("ftp:") || input.startsWith("sftp:") || input.startsWith("scp:");
	}
	
	private static boolean isConsole(String input) {
		return input.equals(STD_CONSOLE);
	}
	
	private static boolean isSandbox(String input) {
		return SandboxUrlUtils.isSandboxUrl(input);
	}
	
	public static boolean isLocalFile(URL contextUrl, String input) {
		if (input.startsWith("file:")) {
			return true;
		} else if (isRemoteFile(input) || isConsole(input) || isSandbox(input) || isArchive(input)) {
			return false;
		} else {
			try {
				URL url = getFileURL(contextUrl, input);
				return !isSandbox(url.toString());
			} catch (MalformedURLException e) {}
		}

		return false;
	}
	
	private static boolean isHttp(String input) {
		return input.startsWith("http:") || input.startsWith("https:");
	}
	
	private static boolean hasCustomPathOutputResolver(URL contextURL, String input, boolean appendData,
			int compressLevel)
		throws IOException {
		OutputStream os;
    	for (CustomPathResolver customPathResolver : customPathResolvers) {
    		os = customPathResolver.getOutputStream(contextURL, input, appendData, compressLevel);
    		if (os != null) {
    			os.close();
    			return true;
    		}
    	}
    	
    	return false;
	}
	
	private static boolean hasCustomPathInputResolver(URL contextURL, String input) throws IOException {	
		InputStream innerStream;
		for (CustomPathResolver customPathResolver : customPathResolvers) {
			innerStream = customPathResolver.getInputStream(contextURL, input);
			if (innerStream != null) {
				innerStream.close();
				return true;
			}
		}
		
		return false;
	}
	
	@java.lang.SuppressWarnings("unchecked")
	private static String getFirstFileInZipArchive(URL context, String filePath) throws NullPointerException, FileNotFoundException, ZipException, IOException {
		File file = getJavaFile(context, filePath); // CLS-537
		de.schlichtherle.util.zip.ZipFile zipFile = new de.schlichtherle.util.zip.ZipFile(file);
		Enumeration<de.schlichtherle.util.zip.ZipEntry> zipEnmr;
		de.schlichtherle.util.zip.ZipEntry entry;
		
		for (zipEnmr = zipFile.entries(); zipEnmr.hasMoreElements() ;) {
			entry = zipEnmr.nextElement();
			if (!entry.isDirectory()) {
				return entry.getName();
			}
		}
		
		return null;
	}
	
	/**
	 * Descends through the URL to figure out whether this is a local archive.
	 * 
	 * If the whole URL represents a local archive, standard FS path (archive.zip/dir1/another_archive.zip/dir2/file)
	 * will be returned inside of the path string builder, and true will be returned.
	 * 
	 * @param input URL to inspect
	 * @param path output buffer for full file-system path
	 * @return true if the URL represents a file inside a local archive
	 * @throws IOException
	 */
	private static boolean getLocalArchivePath(URL contextURL, String input, boolean appendData, int compressLevel,
			StringBuilder path, int nestLevel, boolean output) throws IOException {
		
		if (output) {
			if (hasCustomPathOutputResolver(contextURL, input, appendData, compressLevel)) {
				return false;
			}
		}
		else {
			if (hasCustomPathInputResolver(contextURL, input)) {
				return false;
			}
		}
		
		if (isZipArchive(input)) {
			Matcher matcher = getInnerInput(input);
			// URL-centered convention, zip:(inner.zip)/outer.txt
			// this might be misleading, as what you will see on the filesystem first is inner.zip
			// and inside of that there will be a file named outer.txt
			String innerSource; // inner part of the URL (outer file on the FS)
			String outer; // outer part of the URL (inner file file on the FS)
			boolean ret;
			
			if (matcher != null && (innerSource = matcher.group(5)) != null) {
				outer = matcher.group(10);
			}
			else {
				return false;
			}
			
			ret = getLocalArchivePath(contextURL, innerSource, appendData, compressLevel, path, nestLevel + 1, output);
			if (ret) {
				if (outer == null && isZipArchive(input)) {
					outer = output ? DEFAULT_ZIP_FILE : getFirstFileInZipArchive(contextURL, path.toString());
					if (outer == null) {
						throw new IOException("The archive does not contain any files");
					}
				}
				if (path.length() != 0) {
					path.append(File.separator);
				}
				path.append(outer);
			}
			
			return ret;
		}
		else if (isLocalFile(contextURL, input) && nestLevel > 0) {
			assert(path.length() == 0);			
			path.append(input);
			return true;
		}
		else {
			return false;
		}
	}
	
	private static boolean getLocalArchiveOutputPath(URL contextURL, String input, boolean appendData,
			int compressLevel, StringBuilder path) throws IOException {
		return getLocalArchivePath(contextURL, input, appendData, compressLevel, path, 0, true);
	}

	private static boolean getLocalArchiveInputPath(URL contextURL, String input, StringBuilder path)
		throws IOException {
		return getLocalArchivePath(contextURL, input, false, 0, path, 0, false);
	}
	
	private static void registerTrueZipVSFEntry(de.schlichtherle.io.File entry) {
		TransformationGraph graph = ContextProvider.getGraph();
		if (graph != null) {
			graph.getVfsEntries().addVFSEntry(entry);
		}
	}
	
	/**
     * Creates OutputStream from the url definition.
     * <p>All standard url format are acceptable (including ftp://) plus extended form of url by zip & gzip construction:</p>
     * Examples:
     * <dl>
     *  <dd>zip:&lt;url_to_zip_file&gt;#&lt;inzip_path_to_file&gt;</dd>
     *  <dd>gzip:&lt;url_to_gzip_file&gt;</dd>
     * </dl>
	 * @param contextURL
	 * @param input
	 * @param appendData
	 * @param compressLevel
	 * @return
	 * @throws IOException
	 */
	public static OutputStream getOutputStream(URL contextURL, String input, boolean appendData, int compressLevel)	throws IOException {
        OutputStream os = null;
        
        StringBuilder localArchivePath = new StringBuilder();
        if (getLocalArchiveOutputPath(contextURL, input, appendData, compressLevel, localArchivePath)) {
        	String archPath = localArchivePath.toString();
			
        	// apply the contextURL
        	URL url = FileUtils.getFileURL(contextURL, archPath);
			String absolutePath = getUrlFile(url);
			
        	de.schlichtherle.io.File archive = new de.schlichtherle.io.File(absolutePath);
        	boolean mkdirsResult = archive.getParentFile().mkdirs();
			log.debug("Opening local archive entry " + archive.getAbsolutePath()
        			+ " (mkdirs: " + mkdirsResult
        			+ ", exists:" + archive.exists() + ")");
			registerTrueZipVSFEntry(archive);
        	return new de.schlichtherle.io.FileOutputStream(absolutePath, appendData);
        }
        
        //first we try the custom path resolvers
    	for (CustomPathResolver customPathResolver : customPathResolvers) {
    		os = customPathResolver.getOutputStream(contextURL, input, appendData, compressLevel);
    		if (os != null) {
    			log.debug("Output stream '" + input + "[" + contextURL + "] was opened by custom path resolver.");
    			return os; //we found the desired output stream using external resolver method
    		}
    	}
    	
		// std input (console)
		if (isConsole(input)) {
			return Channels.newOutputStream(new SystemOutByteChannel());
		}
		
		// get inner source
		Matcher matcher = getInnerInput(input);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			// get and set proxy and go to inner source
			Proxy proxy = getProxy(innerSource);
			String proxyUserInfo = null;
			input = matcher.group(2) + matcher.group(3) + matcher.group(7);
			if (proxy != null) {
				try {
					proxyUserInfo = new URI(innerSource).getUserInfo();
				} catch (URISyntaxException ex) {
				}
			}
			os = proxy == null ? getOutputStream(contextURL, innerSource, appendData, compressLevel) : getAuthorizedConnection(getFileURL(contextURL, input), proxy, proxyUserInfo).getOutputStream();
		}
		
		// get archive type
		StringBuilder sbAnchor = new StringBuilder();
		StringBuilder sbInnerInput = new StringBuilder();
		ArchiveType archiveType = getArchiveType(input, sbInnerInput, sbAnchor);
		input = sbInnerInput.toString();
		
        //open channel
        if (os == null) {
    		// create output stream
    		if (isRemoteFile(input) && !isHttp(input)) {
    			// ftp output stream
    			URL url = FileUtils.getFileURL(contextURL, input);
    			URLConnection urlConnection = url.openConnection();
    			if (urlConnection instanceof SFTPConnection) {
    				((SFTPConnection)urlConnection).setMode(appendData? ChannelSftp.APPEND : ChannelSftp.OVERWRITE);
    			}
    			try {
        			os = urlConnection.getOutputStream();
    			} catch (IOException e) {
    				log.debug("IOException occured for URL - host: '" + url.getHost() + "', path: '" + url.getPath() + "' (user info not shown)",
    						  "IOException occured for URL - host: '" + url.getHost() + "', userinfo: '" + url.getUserInfo() + "', path: '" + url.getPath() + "'");
    				throw e;
    			}
    		} else if (S3InputStream.isS3File(input)) {
    			// must be done before isHttp() check
    			return new S3OutputStream(new URL(input));
    		} else if (isHttp(input)) {
    			return new WebdavOutputStream(input);
    		} else if (isSandbox(input)) {
    			TransformationGraph graph = ContextProvider.getGraph();
        		if (graph == null) {
					throw new NullPointerException("Graph reference cannot be null when \"" + SandboxUrlUtils.SANDBOX_PROTOCOL + "\" protocol is used.");
        		}
    			URL url = FileUtils.getFileURL(contextURL, input);
            	return graph.getAuthorityProxy().getSandboxResourceOutput(url.getHost(), SandboxUrlUtils.getRelativeUrl(url.toString()), appendData);
    		} else {
    			// file path or relative URL
    			URL url = FileUtils.getFileURL(contextURL, input);
    			if (isSandbox(url.toString())) {
        			TransformationGraph graph = ContextProvider.getGraph();
            		if (graph == null) {
    					throw new NullPointerException("Graph reference cannot be null when \"" + SandboxUrlUtils.SANDBOX_PROTOCOL + "\" protocol is used.");
            		}
                	return graph.getAuthorityProxy().getSandboxResourceOutput(url.getHost(), getUrlFile(url), appendData);
    			}
    			// file input stream 
    			String filePath = url.getRef() != null ? getUrlFile(url) + "#" + url.getRef() : getUrlFile(url);
    			os = new FileOutputStream(filePath, appendData);
    		}
    	}
		
		// create writable channel
		// zip channel
		if(archiveType == ArchiveType.ZIP) {
			// resolve url format for zip files
			if (appendData) {
				throw new IOException("Appending to remote archives is not supported");
			}
			de.schlichtherle.util.zip.ZipOutputStream zout = new de.schlichtherle.util.zip.ZipOutputStream(os);
			if (compressLevel != -1) {
				zout.setLevel(compressLevel);
			}
			String anchor = sbAnchor.toString();
			de.schlichtherle.util.zip.ZipEntry entry =
				new de.schlichtherle.util.zip.ZipEntry(anchor.equals("") ? DEFAULT_ZIP_FILE : anchor);
			zout.putNextEntry(entry);
			return zout;
        } 
		
		// gzip channel
		else if (archiveType == ArchiveType.GZIP) {
			if (appendData) {
				throw new IOException("Appending to remote archives is not supported");
			}
            GZIPOutputStream gzos = new GZIPOutputStream(os, Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
            return gzos;
        } 
		
		// other channel
       	return os;
	}
    
	/**
	 * This method checks whether it is possible to write to given file.
	 * 
	 * @param contextURL
	 * @param fileURL
	 * @param mkDirs - tries to make directories if it is necessary
	 * @return true if can write, false otherwise
	 * @throws ComponentNotReadyException
	 */
	@SuppressWarnings(value = "RV_ABSOLUTE_VALUE_OF_HASHCODE")
	public static boolean canWrite(URL contextURL, String fileURL, boolean mkDirs) throws ComponentNotReadyException {
		// get inner source
		Matcher matcher = getInnerInput(fileURL);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			return canWrite(contextURL, innerSource, mkDirs);
		}
		
		String fileName;
		if (fileURL.startsWith("zip:") || fileURL.startsWith("tar:")){
			int pos;
			fileName = fileURL.substring(fileURL.indexOf(':') + 1, 
					(pos = fileURL.indexOf('#')) == -1 ? fileURL.length() : pos);
		}else if (fileURL.startsWith("gzip:")){
			fileName = fileURL.substring(fileURL.indexOf(':') + 1);
		}else{
			fileName = fileURL;
		}
		MultiOutFile multiOut = new MultiOutFile(fileName);
		File file;
        URL url;
		boolean tmp = false;
		//create file on given URL
		try {
			String filename = multiOut.next();
			if(filename.startsWith("port:")) return true;
			if(filename.startsWith("dict:")) return true;
			url = getFileURL(contextURL, filename);
            if(!url.getProtocol().equalsIgnoreCase("file")) return true;
            
            // if the url is a path, make a fake file
            String sUrl = getUrlFile(url);
            boolean isFile = !sUrl.endsWith("/") && !sUrl.endsWith("\\");
            if (!isFile) {
            	if (fileURL.indexOf("#") < 0) {
					sUrl = sUrl + "tmpfile" + Math.abs(sUrl.hashCode());
				} else {
					sUrl = sUrl + "tmpfile" + UUID.randomUUID();
				}
            }
			file = new File(sUrl);
		} catch (Exception e) {
			throw new ComponentNotReadyException(e + ": " + fileURL);
		}
		//check if can write to this file
		tmp = file.exists() ? file.canWrite() : createFile(file, mkDirs);
		
		if (!tmp) {
			throw new ComponentNotReadyException("Can't write to: " + fileURL);
		}
		return true;
	}
	
	/**
	 * Creates directory for fileURL if it is necessary.
	 * @param contextURL
	 * @param fileURL
	 * @return created parent directory
	 * @throws ComponentNotReadyException 
	 */
	public static File makeDirs(URL contextURL, String fileURL) throws ComponentNotReadyException {
		if (contextURL == null && fileURL == null) return null;
		URL url;
		try {
			url = FileUtils.getFileURL(contextURL, FileURLParser.getMostInnerAddress(fileURL));
		} catch (MalformedURLException e) {
			return null;
		}

		if (SandboxUrlUtils.isSandboxUrl(url)) {
			TransformationGraph graph = ContextProvider.getGraph();

			if (graph == null) {
				throw new NullPointerException("Graph reference cannot be null when \"" + SandboxUrlUtils.SANDBOX_PROTOCOL + "\" protocol is used.");
			}

			try {
				graph.getAuthorityProxy().makeDirectories(url.getHost(), url.getPath());
			} catch (IOException exception) {
				throw new ComponentNotReadyException("Making of sandbox directories failed!", exception);
			}

			return null;
		}

		if (!url.getProtocol().equals(FILE_PROTOCOL)) return null;
		
		//find the first non-existing directory
		File file = new File(url.getPath());
		File fileTmp = file;
		File createDir = null;
		while (fileTmp != null && !fileTmp.exists()) {
			createDir = fileTmp;
			fileTmp = fileTmp.getParentFile();
		}
		if (createDir != null && !file.mkdirs()) {
			if (!file.exists()) {
				// dir still doesn't exist - throw ex.
				throw new ComponentNotReadyException("Cannot create directory: " + file);
			}
		}
		return createDir;
	}
	
        /**
         * Gets file path from URL, properly handles special URL characters (like %20)
         * @param url
         * @return
         * @throws UnsupportedEncodingException
         *
         * @see #handleSpecialCharacters(java.net.URL)
         */
        private static String getUrlFile(URL url) {
            try {
                final String fixedFileUrl = handleSpecialCharacters(url);
                return URLDecoder.decode(fixedFileUrl, "UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException("Encoding not supported!", ex);
            }
        }

    /**
     * Fix problems with special characters that occurs while running on Mac OS X enviroment and using path
     * which contains '+' character, e.g. /var/folders/t6/t6VjEdukHKWHBtJyNy8wEU+++TI/-Tmp-/.
     * Without special handling, method #getUrlFile will return /var/folders/t6/t6VjEdukHKWHBtJyNy8wEU   TI/-Tmp-/,
     * handling '+' characeter as ' ' (space).
     *
     * @param url
     * @return
     */
    private static String handleSpecialCharacters(URL url) {
        return url.getFile().replace("+", PLUS_CHAR_ENCODED);
    }

    /**
	 * Tries to create file and directories. 
	 * @param fileURL
	 * @return
	 * @throws ComponentNotReadyException 
	 */
	private static boolean createFile(File file, boolean mkDirs) throws ComponentNotReadyException {
		boolean tmp = false;
		boolean fails = false;
		try {
			tmp = file.createNewFile();
		} catch (IOException e) {
			if (!mkDirs) throw new ComponentNotReadyException(e + ": " + file);
			fails = true;
		}
		File createdDir = null;
		if (fails) {
			createdDir = file.getParentFile();
			createdDir = makeDirs(null, createdDir.getAbsolutePath());
			try {
				tmp = file.createNewFile();
			} catch (IOException e) {
				if (createdDir != null) {
					if (!deleteFile(createdDir)) {
						log.error("error delete " + createdDir);
					}
				}
				throw new ComponentNotReadyException(e + ": " + file);
			}
		}
		if (tmp) {
			if (!file.delete()) {
				log.error("error delete " + file.getAbsolutePath());
			}
		}
		if (createdDir != null) {
			if (!deleteFile(createdDir)) {
				log.error("error delete " + createdDir);
			}
		}
		return tmp;
	}
	
	/**
	 * Deletes file.
	 * @param file
	 * @return true if the file is deleted
	 */
	private static boolean deleteFile(File file) {
		if (!file.exists()) return true;

		//list and delete all sub-directories
		for (File child: file.listFiles()) {
			if (!deleteFile(child)) {
				return false;
			}
		}
		return file.delete();
	}
	
	
	/**
	 * This method checks whether is is possible to write to given file
	 * 
	 * @param contextURL
	 * @param fileURL
	 * @return true if can write, false otherwise
	 * @throws ComponentNotReadyException
	 */
	public static boolean canWrite(URL contextURL, String fileURL) throws ComponentNotReadyException {
		return canWrite(contextURL, fileURL, false);
	}
	
	/**
	 * This method checks whether is is possible to write to given directory
	 * 
	 * @param dest
	 * @return true if can write, false otherwise
	 */
	public static boolean canWrite(File dest){
		if (dest.exists()) return dest.canWrite();
		
		List<File> dirs = getDirs(dest);
		
		boolean result = dest.mkdirs();
				
		//clean up
		for (File dir : dirs) {
			if (dir.exists()) {
				boolean deleted = dir.delete();
				if( !deleted ){
					log.error("error delete " + dir.getAbsolutePath());
				}
			}
		}
		
		return result;
	}
	
	/**
	 * @param file
	 * @return list of directories, which have to be created to create this directory (don't exist),
	 * 	  empty list if requested directory exists
	 */
	private static List<File> getDirs(File file){
		ArrayList<File> dirs = new ArrayList<File>();
		File dir = file;
		if (file.exists()) return dirs;
		dirs.add(file);
		while (!(dir = dir.getParentFile()).exists()) {
			dirs.add(dir);
		}
		return dirs;
	}

	/**
	 * Finds embedded source.
	 * 
	 * Example: 
	 * 		source:      zip:(http://linuxweb/~jausperger/employees.dat.zip)#employees0.dat
	 *      result: (g1) zip:
	 *              (g2) http://linuxweb/~jausperger/employees.dat.zip
	 *              (g3) #employees0.dat
	 * 
	 * @param source - input/output source
	 * @return matcher or null
	 */
	public static Matcher getInnerInput(String source) {
		Matcher matcher = INNER_SOURCE.matcher(source);
		return matcher.find() ? matcher : null;
	}
	
	/**
	 * Returns file name of input (URL).
	 * 
	 * @param input - url file name
	 * @return file name
	 * @throws MalformedURLException
	 * 
	 * @see java.net.URL#getFile()
	 */
	public static String getFile(URL contextURL, String input) throws MalformedURLException {
		Matcher matcher = getInnerInput(input);
		String innerSource2, innerSource3;
		if (matcher != null && (innerSource2 = matcher.group(5)) != null) {
			if (!(innerSource3 = matcher.group(7)).equals("")) {
				return innerSource3.substring(1);
			} else {
				input = getFile(null, innerSource2);
			}
		}
		URL url = getFileURL(contextURL, input);
		if (SandboxUrlUtils.isSandboxUrl(url)) { // CLS-754
			try {
				CloverURI cloverUri = CloverURI.createURI(url.toURI());
				File file = FileManager.getInstance().getFile(cloverUri); 
				if (file != null) {
					return file.toString();
				}
			} catch (Exception e) {
				log.debug("Failed to resolve sandbox URL", e);
			}
		}
		if (url.getRef() != null) return url.getRef();
		else {
			input = getUrlFile(url);
			if (input.startsWith("zip:") || input.startsWith("tar:")) {
				input = input.contains("#") ? 
					input.substring(input.lastIndexOf('#') + 1) : 
					input.substring(input.indexOf(':') + 1);
			} else if (input.startsWith("gzip:")) {
				input = input.substring(input.indexOf(':') + 1);
			}
			return normalizeFilePath(input);
		}
	}

	/**
	 * Adds final slash to the directory path, if it is necessary.
	 * @param directoryPath
	 * @return
	 */
	public static String appendSlash(String directoryPath) {
		if(directoryPath.length() == 0 || directoryPath.endsWith("/") || directoryPath.endsWith("\\")) {
			return directoryPath;
		} else {
			return directoryPath + "/";
		}
	}
	
	/**
	 * Parses address and returns true if the address contains a server.
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static boolean isServerURL(URL url) throws IOException {
		
		return url != null && !url.getProtocol().equals(FILE_PROTOCOL);
	}
	
	public static boolean isArchiveURL(URL url) {
		String protocol = null;
		if (url != null) {
			protocol = url.getProtocol();
		}
		return protocol.equals(TAR_PROTOCOL) || protocol.equals(GZIP_PROTOCOL) || protocol.equals(ZIP_PROTOCOL);
	}
	
	/**
	 * Creates URL connection and connect the server.
	 * 
	 * @param url
	 * @throws IOException
	 */
	public static void checkServer(URL url) throws IOException {
		if (url == null) {
			return;
		}
		URLConnection connection = url.openConnection();
		connection.connect();
		if (connection instanceof SFTPConnection) {
			((SFTPConnection) connection).disconnect();
		}
	}

	/**
	 * Gets the most inner url address.
	 * @param contextURL 
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static URL getInnerAddress(URL contextURL, String input) throws IOException {
        URL url = null;
        
		// get inner source
		Matcher matcher = getInnerInput(input);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			url = getInnerAddress(contextURL, innerSource);
			input = matcher.group(2) + matcher.group(3) + matcher.group(7);
		}
		
		// std input (console)
		if (input.equals(STD_CONSOLE)) {
			return null;
		}
		
        //resolve url format for zip files
        if(input.startsWith("zip:") || input.startsWith("tar:")) {
            if(!input.contains("#")) { //url is given without anchor - later is returned channel from first zip entry 
            	input = input.substring(input.indexOf(':') + 1);
            } else {
                input = input.substring(input.indexOf(':') + 1, input.lastIndexOf('#'));
            }
        }
        else if (input.startsWith("gzip:")) {
        	input = input.substring(input.indexOf(':') + 1);
        }
        
        return url == null ? FileUtils.getFileURL(contextURL, input) : url;
	}
	
	/**
	 * Adds new custom path resolver. Useful for third-party implementation of path resolving.
	 * @param customPathResolver
	 */
	public static void addCustomPathResolver(CustomPathResolver customPathResolver) {
		customPathResolvers.add(customPathResolver);
	}


	/**
	 * Converts the given URL to File.
	 * @param url
	 * @return
	 * @throws MalformedURLException is thrown if the given URL does not have 'file' protocol.
	 * @see http://weblogs.java.net/blog/kohsuke/archive/2007/04/how_to_convert.html
	 */
	public static File convertUrlToFile(URL url) throws MalformedURLException {
		String protocol = url.getProtocol();
		if (protocol.equals(FILE_PROTOCOL)) {
			try {
				return new File(url.toURI());
			} catch(URISyntaxException e) {
				return new File(url.getPath());
			} catch(IllegalArgumentException e2) {
				return new File(url.getPath());
			}
		} else if (protocol.equals(SandboxUrlUtils.SANDBOX_PROTOCOL)) {
			try {
				CloverURI cloverUri = CloverURI.createURI(url.toURI());
				File file = FileManager.getInstance().getFile(cloverUri); 
				if (file != null) {
					return file;
				}
			} catch (Exception e) {
				log.debug("Failed to resolve sandbox URL", e);
			}
		}
		throw new MalformedURLException("URL '" + url.toString() + "' cannot be converted to File.");
	}
	
	/**
	 * Tries to convert the given string to {@link File}. 
	 * @param contextUrl context URL which is used for relative path
	 * @param file string representing the file
	 * @return {@link File} representation
	 */
	public static File getJavaFile(URL contextUrl, String file) {
		URL fileURL;
		try {
			fileURL = FileUtils.getFileURL(contextUrl, file);
		} catch (MalformedURLException e) {
			throw new JetelRuntimeException("Invalid file URL definition.", e);
		}
		try {
			return FileUtils.convertUrlToFile(fileURL);
		} catch (MalformedURLException e) {
			throw new JetelRuntimeException("File URL does not have 'file' protocol.", e);
		}
	}

	/**
	 * <p>This method affects only windows platform following way:
	 * <ul><li> backslashes are replaced by slashes
	 * <li>if first character is slash and device specification follows, the starting slash is removed
	 * </ul>
	 * 
	 * <p>For example:
	 * 
	 * <p><tt>c:\project\data.txt -> c:/project/data.txt<br>
	 * /c:/project/data.txt -> c:/project/data.txt
	 * 
	 * <p>A path reached from method anUrl.getPath() (or anUrl.getFile()) should be cleaned by this method. 
	 * 
	 * @param path
	 * @return
	 */
	public static String normalizeFilePath(String path) {
		if (path == null) {
			return "";
		} else {
			if (PlatformUtils.isWindowsPlatform()) {
				//convert backslash to forward slash
				path = path.indexOf('\\') == -1 ? path : path.replace('\\', '/');
				//if device is present
				int i = path.indexOf(':');
				if (i != -1) {
					//remove leading slash from device part to handle output of URL.getFile()
					path = path.charAt(0) == '/' ? path.substring(1, path.length()) : path;
				}
			}
			return path;
		}
	}
	
	/**
	 * Standardized way how to convert an URL to string.
	 * For URLs with 'file' protocol only simple path is returned (for example file:/c:/project/data.txt ->c:/project/data.txt)
	 * The URLs with other protocols are converted by URL.toString() method. 
	 * 
	 * @param url
	 * @return
	 */
	public static String convertUrlToString(URL url) {
		if (url == null) {
			return null;
		}
		
        if (url.getProtocol().equals(FILE_PROTOCOL)) {
			return normalizeFilePath(url.getPath());
		} else {
			return url.toString();
		}
	}
	
    public static class ArchiveURLStreamHandler extends URLStreamHandler {
    	
    	private URL context;
    	
		public ArchiveURLStreamHandler() {
		}
		
    	private ArchiveURLStreamHandler(URL context) {
			this.context = context;
		}

		@Override
    	protected URLConnection openConnection(URL u) throws IOException {
			return new ArchiveURLConnection(context, u);
    	}
    }
    
    private static class ArchiveURLConnection extends URLConnection {
    	
    	private URL context;

		public ArchiveURLConnection(URL context, URL url) {
			super(url);
			this.context = context;
		}
    	
		@Override
		public void connect() throws IOException {
		}
		
		@Override
		public InputStream getInputStream() throws IOException {
			String urlString;
			try {
				// Try to decode %-encoded URL
				// Fix of CLD-2872 if scheme is in a zip file and the URL contains spaces (or other URL-invalid chars)
				URI uri = url.toURI();
				if (uri.isOpaque()) {
					// This is intended to handle archive (e.g. zip:...) URLs
					// Example of expected URI format: zip:(sandbox://sanboxName/path%20with%20spaces.zip)#path%20inside%20zip.xsd
					// Decode only fragmet part to get zip:(sandbox://sanboxName/path%20with%20spaces.zip)#path inside zip.xsd
					urlString = decodeFragment(uri);
				} else {
					urlString = uri.toString();
				}
			} catch (URISyntaxException e) {
				urlString = url.toString();
			}
			return FileUtils.getInputStream(context, urlString);
		}
		
		private static String decodeFragment(URI uri) {
			StringBuilder sb = new StringBuilder();
			if (uri.getScheme() != null) {
				sb.append(uri.getScheme()).append(':');
			}
			sb.append(uri.getRawSchemeSpecificPart());
			if (uri.getFragment() != null) {
				sb.append('#').append(uri.getFragment());
			}
			return sb.toString();
		}
    }
    

    /**
     * Quietly closes the passed IO object.
     * Does nothing if the argument is <code>null</code>.
     * Ignores any exceptions throw when closing the object.
     * 
     * @param closeable an IO object to be closed
     */
	public static void closeQuietly(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException ex) {
				// ignore
			}
		}
	}

	/**
	 * Closes all objects passed as the argument.
	 * If any of them throws an exception, the first exception is thrown.
	 * 
	 * @param closeables
	 * @throws IOException
	 */
	public static void closeAll(Closeable... closeables) throws IOException {
		if (closeables != null) {
			IOException firstException = null;
			
			for (Closeable closeable: closeables) {
				if (closeable != null) {
					try {
						closeable.close();
					} catch (IOException ex) {
						if (firstException == null) {
							firstException = ex;
						}
					}
				}
			}
			
			if (firstException != null) {
				throw firstException;
			}
		}
	}

	/**
     * Efficiently copies file contents from source to target.
     * Creates the target file, if it does not exist.
     * The parent directories of the target file are not created.
     * 
     * @param source source file
     * @param target target file
     * @return <code>true</code> if the whole content of the source file was copied.
     * 
     * @throws IOException
     */
	public static boolean copyFile(File source, File target) throws IOException {
		FileInputStream inputStream = null;
		FileOutputStream outputStream = null;
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		try {
			inputStream = new FileInputStream(source);
			outputStream = new FileOutputStream(target);
			inputChannel = inputStream.getChannel();
			outputChannel = outputStream.getChannel();
	        StreamUtils.copy(inputChannel, outputChannel);
			return true;
		} finally {
			closeAll(outputChannel, outputStream, inputChannel, inputStream);
		}
	}

	/**
	 * Delete the supplied {@link File} - for directories,
	 * recursively delete any nested directories or files as well.
	 * @param root the root <code>File</code> to delete
	 * @return <code>true</code> if the <code>File</code> was deleted,
	 * otherwise <code>false</code>
	 */
	public static boolean deleteRecursively(File root) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException("Interrupted");
		}
		if (root != null && root.exists()) {
			if (root.isDirectory()) {
				File[] children = root.listFiles();
				if (children != null) {
					for (int i = 0; i < children.length; i++) {
						deleteRecursively(children[i]);
					}
				}
			}
			return root.delete();
		}
		return false;
	}

	/**
	 * Checks the given string whether represents multiple URL.
	 * This is only simple test, whether the given string contains
	 * one of these three characters <b>;*?</b>.
	 * @param fileURL
	 * @return true if and only if the given string represents multiple URL
	 */
	public static boolean isMultiURL(String fileURL) {
		return fileURL != null && (fileURL.contains(";") || fileURL.contains("*") || fileURL.contains("?"));
	}
	
}

/*
 *  End class FileUtils
 */

