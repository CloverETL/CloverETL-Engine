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

import static org.jetel.util.file.FileUtils.DICTIONARY_PROTOCOL;
import static org.jetel.util.file.FileUtils.PORT_PROTOCOL;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.http.HttpStatus;
import org.jetel.enums.ArchiveType;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.util.Pair;
import org.jetel.util.protocols.ProxyConfiguration;
import org.jetel.util.protocols.UserInfo;
import org.jetel.util.protocols.amazon.S3InputStream;
import org.jetel.util.protocols.ftp.FTPConnection;
import org.jetel.util.file.HttpPartUrlUtils;
import org.jetel.util.protocols.proxy.ProxyHandler;
import org.jetel.util.protocols.proxy.ProxyProtocolEnum;
import org.jetel.util.protocols.sftp.SFTPConnection;
import org.jetel.util.protocols.webdav.WebdavClient;
import org.jetel.util.protocols.webdav.WebdavClientImpl;
import org.jetel.util.string.StringUtils;

import com.github.sardine.DavResource;
import com.github.sardine.impl.SardineException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import de.schlichtherle.truezip.file.TFile;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/05/06  
 * Class generating collection of filenames which match given wildcard patterns.
 * The pattern may describe either relative or absolute filenames.
 * '*' represents any count of characters, '?' represents one character.
 * Wildcards cannot be followed by directory separators. 
 */
public class WcardPattern {

	// logger
    private static Log logger = LogFactory.getLog(WcardPattern.class);

    // for embedded source
	//     "[zip|gzip|tar]     :       ^(       something   )          [[#|$]something]?
	//      ((zip|gzip|tar)    :       ([^\\(]  .*          [^\\)])    #(.*))|((zip|gzip|tar):([^\\(].*[^\\)])$)
	private final static Pattern ARCHIVE_SOURCE = Pattern.compile("((zip|gzip|tar|tgz):([^\\(].*[^\\)])#(.*))|((zip|gzip|tar|tgz):([^\\(].*[^\\)])$)");
	
	// Wildcard characters.
	@SuppressFBWarnings("MS")
	public final static char[] WCARD_CHAR = {'*', '?'};

	// Regex substitutions for wildcards. 
	private final static String[] REGEX_SUBST = {".*", "."};

	// Start sequence for regex quoting.
	private final static String REGEX_START_QUOTE = "\\Q";

	// End sequence for regex quoting
	private final static String REGEX_END_QUOTE = "\\E";

	// Regex start anchor.
	private final static char REGEX_START_ANCHOR = '^';

	// Regex end anchor.
	private final static char REGEX_END_ANCHOR = '$';

	// file protocol
	private final static String FILE = "file";
	
	// sftp protocol
	private final static String SFTP = "sftp";
	
	// ftp protocol
	private final static String FTP = "ftp";
	
	// http protocol
	private final static String HTTP = "http";

	// https protocol
	private final static String HTTPS = "https";

	// Collection of filename patterns.
	private List<String> patterns;
	
	// Context URL.
	private URL parent;

	// default - resolve server names
	private boolean resolveAllNames = true;
	
	/**
	 * Constructor. Creates instance with empty set of patterns. It doesn't match any filenames initially.  
	 */
	public WcardPattern() {
		patterns = new ArrayList<String>(1);
	}
	
	/**
	 * Adds filename pattern.
	 * @param pat Pattern to be added.
	 */
	public void addPattern(String pat) {
		patterns.add(pat);
	}

	/**
	 * Add filename patterns.
	 * @param pat Array of patterns
	 */
	public void addPattern(String[] pat) {
		for (int i = 0; i < pat.length; i++) {
			addPattern(pat[i]);
		}
	}

	/**
	 * Add filename patterns.
	 * @param pat Pattern list
	 * @param sep Pattern separator
	 */
	public void addPattern(String pat, String sep) {
		addPattern(pat.split(sep));
	}

	/**
	 * Splits specified pattern in two parts - directory which cannot contain any wildcard
	 * and filename pattern containing wildcards. When specified pattern doesn't contain
	 * any wildcards, doesn't do anything. 
	 * @param pat Pattern to be split.
	 * @param dir Directory name.
	 * @param filePat Filename pattern. 
	 * @return false for pattern without wildcards, true otherwise. 
	 */
	public static void splitFilePattern(String pat, StringBuffer dir, StringBuffer filePat) {
		dir.setLength(0);
		filePat.setLength(0);

		File f = new File(pat);
		String parent = FileUtils.normalizeFilePath(f.getParent());
		dir.append(StringUtils.isEmpty(parent) ? "." : parent); // CLD-4114: When parent == null, dir would be "null"
		filePat.append(f.getName());
	}
	
	/**
	 * Returns index of the first wildcard.
	 * @param filePat
	 * @return
	 */
	private int getWildCardIndex(String filePat) {
		int resIdx = -1;
		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			int i;
			if ((i = filePat.indexOf("" + WCARD_CHAR[wcardIdx])) >= 0) { // wildcard found
				resIdx = resIdx == -1 || resIdx > i ? i : resIdx;
			}
		}
		// no wildcard in pattern
		return resIdx;
	}
	
	/**
	 * Sets if the method filenames resolves server paths. 
	 * @param resolveAllNames
	 * @throws IOException
	 */
	public void resolveAllNames(boolean resolveAllNames) {
		this.resolveAllNames = resolveAllNames;
	}

	/**
	 * Generates filenames matching current set of patterns.
	 * 
	 * @return Set of matching filenames. Absolute path is returned for URL with wildcard. Relative path is returned 
	 * for URL without wildcard.
	 * @throws IOException 
	 */
	public List<String> filenames() throws IOException {
		List<String> mfiles = new ArrayList<String>();
		String fileName;
		
		// goes through filenames separated by ';'
		for (int i = 0; i < patterns.size(); i++) {
			fileName = patterns.get(i);
			mfiles.addAll(innerFileNames(fileName));
		}
		return mfiles;
	}
	
	private static List<String> getNestedZipAnchors(String url) {
		LinkedList<String> result = new LinkedList<String>();
		StringBuilder innerInput = new StringBuilder();
		StringBuilder anchor = new StringBuilder();
		ArchiveType archiveType = FileUtils.getArchiveType(url, innerInput, anchor);
		while (archiveType != null) {
			result.push(anchor.toString());
			url = innerInput.toString();
			innerInput.setLength(0);
			anchor.setLength(0);
			archiveType = FileUtils.getArchiveType(url, innerInput, anchor);
		}
		
		return result;
	}
	
	private boolean isProxy(String url) {
		int i = url.indexOf(':');
		if (i >= 0) {
			String protocol = url.substring(0, i);
			return ProxyProtocolEnum.fromString(protocol) != null;
		}
		
		return false;
	}

	/**
	 * Gets names from file system and sub-archives.
	 * @param fileName
	 * @param outherPathNeedsInputStream
	 * @return
	 * @throws IOException
	 */
    private List<String> innerFileNames(String fileName) throws IOException {
    	if (isProxy(fileName)) {
    		return null; // the string is in fact a proxy definition, do not try to expand it
    	}

    	// result list for non-archive files
        List<String> fileStreamNames = null;
        
		// get inner source
		String originalFileName = fileName;
		Matcher matcher = FileURLParser.getURLMatcher(fileName);
		String innerSource;
		int iPreName = 0;
		int iPostName = 0;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			iPreName = (matcher.group(2) + matcher.group(3)).length()+1;
			iPostName = iPreName + innerSource.length();
			fileStreamNames = innerFileNames(innerSource);
		} else {
			// for archives without ...:(......), just ...:......
			Matcher archMatcher = getArchiveURLMatcher(fileName);
			if (archMatcher != null && (innerSource = archMatcher.group(3)) != null) {
				iPreName = archMatcher.group(2).length()+1;
				iPostName = iPreName + innerSource.length();
				fileStreamNames = innerFileNames(innerSource);
			} else if (archMatcher != null && (innerSource = archMatcher.group(7)) != null) {
				iPreName = archMatcher.group(6).length()+1;
				iPostName = iPreName + innerSource.length();
				fileStreamNames = innerFileNames(innerSource);
			}
		}
		
		// get archive type
		StringBuilder sbAnchor = new StringBuilder();
		StringBuilder sbInnerInput = new StringBuilder();
		ArchiveType archiveType = FileUtils.getArchiveType(fileName, sbInnerInput, sbAnchor);
		String anchor = sbAnchor.toString();
		if (archiveType != null && iPreName == 0) {
			iPreName = archiveType.name().length()+1;
			iPostName = fileName.length() - (anchor.length() == 0 ? 0 : anchor.length()+1);
		}
		fileName = sbInnerInput.toString();

        if (fileStreamNames == null) { // fileName is NOT a nested URL
        	fileStreamNames = resolveAndSetFileNames(fileName); // resolves simple file names
        }
        
        if (archiveType != null) {
        	if (!resolveAllNames) {
        		// CLO-9154:
        		// skip wildcard resolution in archives in checkConfig
        		return Collections.emptyList();
        	}
            // check sub-archives
            List<String> newFileStreamNames = new ArrayList<String>();
            
    		boolean anchorContainsWildcards = (anchor.indexOf(WCARD_CHAR[0]) >= 0) || (anchor.indexOf(WCARD_CHAR[1]) >= 0);
    		
            for (String resolvedName: fileStreamNames) {
            	InputStream is = null;
            	FileStreamName fileStreamName = new FileStreamName(resolvedName);
            	try {
            		
            		// no need to open input stream if there are no wildcards
            		if (anchorContainsWildcards) {
            			
                		if (archiveType == ArchiveType.ZIP) {
                			String localArchivePath = FileUtils.getLocalArchiveInputPath(parent, resolvedName);
                			if (localArchivePath != null) {
                				// for local ZIP archives, FileUtils.getInputStream must not be called
                				// - it treats archives as folders, therefore it is impossible to open an InputStream to read from them
                		    	TFile root = FileUtils.getLocalZipArchive(parent, localArchivePath);
                		    	if (root == null) {
                		    		throw new IOException("Failed to open local ZIP archive");
                		    	}
                		    	
                		    	// the loop sets root to the topmost archive
                		    	while (root.getEnclArchive() != null) {
                		    		root = root.getEnclArchive();
                		    	}
                		    	
                		    	// open the root file input stream
                		    	is = new FileInputStream(root);
                		    	
                				List<String> nestedAnchors = getNestedZipAnchors(resolvedName);
                		    	// for every nested anchor, open a ZipInputStream 
                		    	for (String entry: nestedAnchors) {
                		    		is = FileUtils.getZipInputStream(is, entry);
                		    	}
                			}
                		}
                		
                		// TARs or remote ZIPs - open input stream
                		if ((archiveType != ArchiveType.GZIP)) {
                			if (is == null) {
                				is = FileUtils.getInputStream(parent, resolvedName);
                			}
                			fileStreamName = new FileStreamName(resolvedName, is);
                		}
            		}
            		
                	switch (archiveType) {
                	case ZIP:
                    	processZipArchive(fileStreamName, originalFileName, anchor, iPreName, iPostName, newFileStreamNames);
                		break;
                	case GZIP:
                		// no need to open parent input stream
                    	processGZipArchive(resolvedName, originalFileName, iPreName, iPostName, newFileStreamNames);
                		break;
                	case TAR:
                    	processTarArchive(fileStreamName, originalFileName, anchor, iPreName, iPostName, newFileStreamNames);
                		break;
                	case TGZ:
                    	processTGZArchive(fileStreamName, originalFileName, anchor, iPreName, iPostName, newFileStreamNames);
                    	break;
                	}
                	
            	} finally {
            		if (is != null) {
            			try {
            				is.close();
            			} catch (IOException ioe) {
            				logger.debug("Failed to close input stream", ioe);
            			}
            		}
            	}
            }
            
            return newFileStreamNames;
        } else {
        	List<String> newFileStreamNames = new ArrayList<String>();
        	for (String resolvedName: fileStreamNames) {
        		processProxy(resolvedName, originalFileName, newFileStreamNames);
        	}
           	return newFileStreamNames;
        }
        
    }

    /**
     * Verifies and gets proxy file.
     * @param fileStreamName
     * @param originalFileName
     * @param fileStreamNames 
     */
    private void processProxy(String fileStreamName, String originalFileName, List<String> fileStreamNames) {
    	if (ProxyHandler.acceptProtocol(FileUtils.getProtocol(fileStreamName))) {
    		fileStreamNames.add(originalFileName); // fileStreamName is a proxy, return originalFileName
    	} else {
    		fileStreamNames.add(fileStreamName); // not a proxy, return fileStreamName (why???)
    	}
	}

	/**
     * Gets list of zip files with full anchor names.
     * @param fileStreamName
     * @param originalFileName
     * @param anchor
     * @param iPreName
     * @param iPostName
     * @param newFileStreamNames
     * @throws IOException
     */
    private void processZipArchive(FileStreamName fileStreamName, String originalFileName, String anchor, int iPreName, int iPostName, List<String> newFileStreamNames) throws IOException {
		// no wildcards, just wrap the fileStreamName name into zip:()#anchor
    	if (fileStreamName.getInputStream() == null) {
    		newFileStreamNames.add(originalFileName.substring(0, iPreName) + fileStreamName.getFileName() + originalFileName.substring(iPostName));
    		return;
    	}
    	
		// look into an archive and check the anchor
		List<String> entries = FileUtils.getMatchingZipEntries(fileStreamName.getInputStream(), anchor);
		
    	// create list of new names generated from the anchor
    	for (String entry: entries) {
    		newFileStreamNames.add(
    				StringUtils.replaceLast(
    						originalFileName.substring(0, iPreName) + fileStreamName.getFileName() + originalFileName.substring(iPostName),
    						anchor, entry
					)
			);
    	}
    }
    
    /**
     * Gets list of tar files with full anchor names.
     * @param fileStreamName
     * @param originalFileName
     * @param anchor
     * @param iPreName
     * @param iPostName
     * @param newFileStreamNames
     * @throws IOException
     */
    private void processTarArchive(FileStreamName fileStreamName, String originalFileName, String anchor, int iPreName, int iPostName, List<String> newFileStreamNames) throws IOException {
		// no wildcards, just wrap the fileStreamName name into tar:()#anchor
    	if (fileStreamName.getInputStream() == null) {
    		newFileStreamNames.add(originalFileName.substring(0, iPreName) + fileStreamName.getFileName() + originalFileName.substring(iPostName));
    		return;
    	}
    	
		// look into an archive and check the anchor
		List<String> entries = FileUtils.getMatchingTarEntries(fileStreamName.getInputStream(), anchor);
    	
    	// create list of new names generated from the anchor
    	for (String entry: entries) {
    		newFileStreamNames.add(
    				StringUtils.replaceLast(
    						originalFileName.substring(0, iPreName) + fileStreamName.getFileName() + originalFileName.substring(iPostName),
    						anchor, entry
    				)
    		);
    	}
    }

    /**
     * Gets list of tar/gzipped files with full anchor names.
     * @throws IOException
     */
    private void processTGZArchive(FileStreamName fileStreamName, String originalFileName, String anchor, int iPreName, int iPostName, List<String> newFileStreamNames) throws IOException {
		// wrap the input stream in GZIPInputStream and call processTarArchive()
    	if (fileStreamName.getInputStream() != null) {
    		fileStreamName = new FileStreamName(fileStreamName.getFileName(), ArchiveUtils.getGzipInputStream(fileStreamName.getInputStream()));
    	}
    	
    	processTarArchive(fileStreamName, originalFileName, anchor, iPreName, iPostName, newFileStreamNames);
    }
    
    /**
     * Gets gzip file.
     * @param fileStreamName
     * @param originalFileName
     * @param iPreName
     * @param iPostName
     * @param newFileStreamNames
     * @throws IOException
     */
    private void processGZipArchive(String fileStreamName, String originalFileName, int iPreName, int iPostName, List<String> newFileStreamNames) throws IOException {
    	// GZIPs only contain one file, no need to look into the file
    	// wrap the fileStreamName into gzip:()
    	
    	newFileStreamNames.add(originalFileName.substring(0, iPreName) + fileStreamName + originalFileName.substring(iPostName));
    }
    
	private List<String> getSandboxNames(URL url) {
		List<String> fileStreamNames = new ArrayList<String>();
		if (hasWildcard(url)) {
			IAuthorityProxy authorityProxy = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph());
			String storageCode = url.getHost();
			String queryPart = url.getQuery() != null ? "?" + url.getQuery() : "";
			Collection<String> files = authorityProxy.resolveAllFiles(storageCode, url.getPath() + queryPart);
			if (files != null) {
				for (String fileName : files) {
					fileStreamNames.add(SandboxUrlUtils.SANDBOX_PROTOCOL_URL_PREFIX + fileName);
				}
			}
		} else {
			fileStreamNames.add(url.toString());
		}
		return fileStreamNames;
	}
    
    /**
     * Gets list of file names or an original name from file system. 
     * @param fileName
     * @return
     * @throws MalformedURLException for unknown protocols, see CL-2667
     */
	private List<String> resolveAndSetFileNames(String fileName) throws MalformedURLException {
		// check if the filename is a file or something else
		URL url = FileUtils.getFileURL(parent, fileName); // CL-2667: may throw MalformedURLException
		
		// try CustomPathResolvers first
		for (CustomPathResolver resolver : FileUtils.getCustompathresolvers()) {
			if (resolver.handlesURL(parent, fileName)) {
				try {
					return resolver.resolveWildcardURL(parent, fileName);
				} catch (IOException e) {
					// NOTHING - will be handled the standard way below
				}
			}
		}
		
		String protocol = url.getProtocol();
		
		// wildcards for file protocol
		if (protocol.equals(FILE)) {
			return getFileNames(fileName);
		}
		
		// wildcards for sftp protocol
		else if (resolveAllNames && protocol.equals(SFTP)) {
			return getSftpNames(url);
		}
		
		// wildcards for ftp protocol
		else if (resolveAllNames && protocol.equals(FTP)) {
			try {
				return getFtpNames(new URL(parent, fileName, FileUtils.ftpStreamHandler));	// the FTPStreamHandler is not properly tested but supports 'ls'
			} catch (MalformedURLException e) {
				//NOTHING
			}
		}
		
		else if (protocol.equals(SandboxUrlUtils.SANDBOX_PROTOCOL)) {
			return getSandboxNames(url);
		}
		else if (resolveAllNames && (
				protocol.equals(HTTP) || protocol.equals(HTTPS))) {
			return getHttpNames(url);
		}
		// CLO-5532:
		else if (protocol.equals(PORT_PROTOCOL) || protocol.equals(DICTIONARY_PROTOCOL)
				|| protocol.equals(HttpPartUrlUtils.REQUEST_PROTOCOL) 
				|| protocol.equals(HttpPartUrlUtils.RESPONSE_PROTOCOL)) {
			return Arrays.asList(fileName);
		}

		// return original filename
		List<String> mfiles = new ArrayList<String>();
		if (resolveAllNames) { // CLO-5399 and CL-1590
			mfiles.add(fileName);
		}
		return mfiles;
	}
	
	// matches the FILENAME part of a path: /dir/subdir/subsubdir/FILENAME
	private static final Pattern FILENAME_PATTERN = Pattern.compile(".*/([^/]+/?)");

	/**
	 * Gets files from fileName that can contain wildcards or returns original name.
	 * @param url
	 * @return
	 */
	private List<String> getFtpNames(URL url) {
		// result list
		List<String> mfiles = new ArrayList<String>();

		// if no wildcard found, return original name
		if (!hasWildcard(url)) {
			mfiles.add(url.toString());
			return mfiles;
		}

		// get files
		FTPConnection ftpConnection = null;
		try {
			// list files
			ftpConnection = (FTPConnection)url.openConnection();
			FTPFile[] ftpFiles = ftpConnection.ls(url.getFile());				// note: too long operation
			for (FTPFile lsFile: ftpFiles) {
				if (lsFile.getType() == FTPFile.DIRECTORY_TYPE) continue;
				String resolverdFileNameWithoutPath = lsFile.getName().replace('\\', '/');
				Matcher m = FILENAME_PATTERN.matcher(resolverdFileNameWithoutPath);
				if (m.matches()) {
					resolverdFileNameWithoutPath = m.group(1); // CL-2468 - some FTPs return full path as file name, get rid of the path
				}
				
				// replace file name
				String urlPath = url.getFile();

				// find last / or \ or set index to the start position
				// create new filepath
				int lastIndex = urlPath.lastIndexOf('/')+1;
				if (lastIndex <= 0) lastIndex = urlPath.lastIndexOf('\\')+1;
				String newUrlPath = urlPath.substring(0, lastIndex) + resolverdFileNameWithoutPath;
				
				// add new resolved url string
				mfiles.add(url.toString().replace(urlPath, newUrlPath));
			}
		} catch (Throwable e) {
			// return original name
			mfiles.add(url.toString());
		} finally {
			if (ftpConnection != null)
				try {
					ftpConnection.disconnect();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}

		return mfiles;
	}

	/**
	 * Returns true if the URL, in the concrete his file, has a wildcard. 
	 * @param url
	 * @return
	 */
	public static boolean hasWildcard(URL url) {
		return hasWildcard(url.getFile());
	}
	
	public static boolean hasWildcard(String fileURL) {
		// check if the url has wildcards
		String fileName = new File(fileURL).getName();
		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			if (fileName.indexOf("" + WCARD_CHAR[wcardIdx]) >= 0) { // wildcard found
				return true;
			}
		}
		return false;
	}
	
	private boolean hasAsteriskWildcard(URL url) {
		String topmostFile = new File(url.getFile()).getName();
		return topmostFile.indexOf('*') != -1;
	}
	
	/**
	 * Gets files from HTTP source using WebDAV 
	 */
	private List<String> getHttpNames(URL url) {
		List<String> mfiles = new ArrayList<String>();

		// We will only consider asterisk (*) as a wild-card in HTTP URL.
		// Question mark (?) serves as a query separator.
		
		if (S3InputStream.isS3File(url)) {
			if (hasAsteriskWildcard(url)) {
				try {
					return S3InputStream.getObjects(url);
				} catch (IOException e) {
					mfiles.add(url.toString());
					return mfiles;
				}
			}
			else {
				mfiles.add(url.toString());
				return mfiles;
			}
		}
		
		if (!hasWildcard(url)) {
			mfiles.add(url.toString());
			return mfiles;
		}
		
		Pair<String, String> parts = FileUtils.extractProxyString(url.toString());
		try {
			url = FileUtils.getFileURL(parent, parts.getFirst());
		} catch (MalformedURLException ex) {
			
		}
		String proxyString = parts.getSecond();
		
		String file = url.getFile();
		int lastSlash = file.lastIndexOf('/');
		if (lastSlash == -1) {
			// no slash - there's probably a question mark, 
			// but denotes a query string, not a wildcard
			mfiles.add(url.toString());
			return mfiles;
		}
		
		// When there is a wildcard, we will presume the user wants to use WebDAV access to list all the files.
		try {
			WebdavClient sardine = new WebdavClientImpl(url, new ProxyConfiguration(proxyString));
			sardine.enableCompression();
			// The issue with sardine is that user authorization must be passed in begin() of SardineFactory
			// but cannot be kept as part of the URL for which we try to get resources.
			// And finally, later we need the authorization details in the URL 
			// to actually access the file.
			// So, typically, for anonymous access a URL like http://anonymous:@host/ is required
			String dir = file.substring(0, lastSlash + 1);

			// remove authorization info 
			StringBuilder dirURL = new StringBuilder(url.getProtocol()).append("://").append(url.getHost());
			if (url.getPort() >= 0) { // proxy can't handle -1 as the port number
				dirURL.append(':').append(url.getPort());
			}
			dirURL.append(dir);
			String pattern = url.getFile();
			
			List<DavResource> resources = sardine.list(dirURL.toString());
			for (DavResource res : resources) {
				if (res.isDirectory()) {
					continue;
				}
				if (checkName(pattern, res.getPath())) {
					// add authorization info back
					StringBuilder fullURL = new StringBuilder(url.getProtocol()).append(':');
					if (proxyString != null) { // remember to include the proxyString
						fullURL.append('(').append(proxyString).append(')');
					}
					fullURL.append("//").append(url.getAuthority()).append(dir).append(res.getName());
					
					mfiles.add(fullURL.toString());
				}
			}
		} catch (SardineException se) {
			switch (se.getStatusCode()) {
			case HttpStatus.SC_NOT_IMPLEMENTED:
			case HttpStatus.SC_METHOD_NOT_ALLOWED:
				break; // "501: Not implemented" and "405: Method not allowed" are not errors, it just means it is not WebDAV
			default:
				if (logger.isDebugEnabled()) {
					logger.debug(url + " - WebDAV wildcard resolution failed - " + se.getStatusCode() + ": " + se.getResponsePhrase(), se);
				}
			}
			mfiles.add(url.toString());
			return mfiles;
		} catch (IOException e) { // some servers respond with other status codes to PROPFIND, even 200 (www.cloveretl.com)
			// it was not possible to connect using WebDAV, let's presume it's a normal HTTP request
			if (logger.isDebugEnabled()) {
				logger.debug(url + " - WebDAV wildcard resolution failed", e);
			}
			mfiles.add(url.toString());
			return mfiles;
		}
		
		return mfiles;
	}
	
	/**
	 * Gets files from fileName that can contain wildcards or returns original name.
	 * @param url
	 * @return
	 */
	private List<String> getSftpNames(URL url) {
		// result list
		List<String> mfiles = new ArrayList<String>();
		
		// if no wildcard found, return original name
		if (!hasWildcard(url)) {
			mfiles.add(url.toString());
			return mfiles;
		}

		URL originalURL = url;
		Pair<String, String> parts = FileUtils.extractProxyString(url.toString());
		try {
			url = FileUtils.getFileURL(parent, parts.getFirst());
		} catch (MalformedURLException ex) {
			
		}
		String proxyString = parts.getSecond();
		Proxy proxy = null;
		UserInfo proxyCredentials = null;
		if (proxyString != null) {
			proxy = FileUtils.getProxy(proxyString);
			try {
				String userInfo = new URI(proxyString).getUserInfo();
				if (userInfo != null) {
					proxyCredentials = new UserInfo(userInfo);
				}
			} catch (URISyntaxException use) {
			}
		}

		// get files
		SFTPConnection sftpConnection = null;
		try {
			// list files
			sftpConnection = (SFTPConnection) ((proxy != null) ? url.openConnection(proxy) : url.openConnection());
			sftpConnection.setProxyCredentials(proxyCredentials);
			Vector<?> v = sftpConnection.ls(url.getFile());				// note: too long operation
			for (Object lsItem: v) {
				LsEntry lsEntry = (LsEntry) lsItem;
				if (lsEntry.getAttrs().isDir()) {
					continue; // CLO-9619
				}
				String resolverdFileNameWithoutPath = lsEntry.getFilename();
				
				// replace file name
				String urlPath = url.getFile();

				// find last / or \ or set index to the start position
				// create new filepath
				int lastIndex = urlPath.lastIndexOf('/')+1;
				if (lastIndex <= 0) lastIndex = urlPath.lastIndexOf('\\')+1;
				String newUrlPath = urlPath.substring(0, lastIndex) + resolverdFileNameWithoutPath;
				
				// add new resolved url string
				mfiles.add(originalURL.toString().replace(urlPath, newUrlPath));
			}
		} catch (Throwable e) {
			// return original name
			logger.debug("SFTP wildcard resolution failed", e);
			mfiles.add(url.toString());
		}
		
		return mfiles;
	}

	/**
	 * Gets files from fileName that can contain wildcards or returns original name.
	 * @param fileName
	 * @return
	 */
	private List<String> getFileNames(String fileName) {
		// result list
		List<String> mfiles = new ArrayList<String>();

		// directory name and file name part
		StringBuffer dirName = new StringBuffer();
		StringBuffer filePat = new StringBuffer();
		
		// if no wildcards, return original filename
		splitFilePattern(fileName, dirName, filePat);
		int idxFileWildCard = getWildCardIndex(filePat.toString());
		int idxPathWildCard = getWildCardIndex(dirName.toString());
		if (idxFileWildCard < 0 && idxPathWildCard < 0) {	// no wildcards
			mfiles.add(fileName);
			return mfiles;
		}

		// check a directory
		List<File> lResolvedPaths = new LinkedList<File>();
		try {
			URL url = FileUtils.getFileURL(parent, dirName.toString());
			lResolvedPaths.add(new File(url.getQuery() != null ? url.getPath() + "?" + url.getQuery() : url.getPath()));
		} catch (Exception e) {
			lResolvedPaths.add(new File(dirName.toString()));
		} 
		
		// get all valid paths
		if (idxPathWildCard >= 0) {
			String wCardPath = lResolvedPaths.get(0).toString();
			lResolvedPaths.clear();
			lResolvedPaths.addAll(getResolvedPaths(wCardPath));
		}
		
		// list the directory and return its files
		for (File dir: lResolvedPaths) {
			if (dir.exists()) {
				FilenameFilter filter = new WcardFilter(filePat.toString());
				String[] curMatch = dir.list(filter);
				Arrays.sort(curMatch);
				for (int fnIdx = 0; fnIdx < curMatch.length; fnIdx++) {
					File f = new File(dir, curMatch[fnIdx]);
					if (f.isDirectory()) {
						continue; // CLO-9619
					}
					mfiles.add(f.getAbsolutePath());
				}
			}
		}
		return mfiles;
	}
	
	/**
	 * @param string
	 * @return
	 */
	private Collection<? extends File> getResolvedPaths(String sDir) {
		List<File> lResolvedPaths = new LinkedList<File>();
		int idxPathWildCard = getWildCardIndex(sDir);
		if (idxPathWildCard == -1) {
			lResolvedPaths.add(new File(sDir));
			return lResolvedPaths;
		}
		
		// get resolved path
		String sTmp = sDir.substring(0, idxPathWildCard);
		File fTmp = new File(sTmp);
		File sResolvedPath = sTmp.endsWith("/") || sTmp.endsWith("\\") ? fTmp : fTmp.getParentFile();
		
		// get wildcard dir name
		File tmpFile = new File(sDir);
		File parentFile;
		do {
			parentFile = tmpFile;
			tmpFile = parentFile.getParentFile();
		} while (!sResolvedPath.equals(tmpFile));
		String sWCardPath = parentFile.getName();
		
		// list and add all suitable directories directories
		FilenameFilter filter = new WcardFilter(sWCardPath);
		File[] curMatch = sResolvedPath.listFiles(filter);
		if (curMatch != null) {
			Arrays.sort(curMatch);
			for (File f: curMatch) {
				if (f.isDirectory()) {
					lResolvedPaths.addAll(getResolvedPaths(f.toString() + sDir.substring(parentFile.toString().length())));
				}
			}
		}
		return lResolvedPaths;
	}
	
	/**
	 * Checks if the name accepts the pattern
	 * @param pattern
	 * @param name
	 * @return
	 */
	public static boolean checkName(String pattern, String name){
		return new WcardFilter(pattern).accept(name);
	}
	
	/**
	 * Creates compiled Pattern from String pattern with simplified syntax -- containing '*' and '?' symbols.
	 * 
	 * @param pattern
	 * @return
	 */
	public static Pattern compileSimplifiedPattern(String pattern) {
		return compileSimplifiedPattern(pattern, WCARD_CHAR, REGEX_SUBST);
	}

	/**
	 * Creates compiled Pattern from String pattern. Replaces characters from wildcardCharacters with strings from regexSubstitutions.
	 * @param pattern
	 * @param wildcardCharacters eg. {'*', '?'}
	 * @param regexSubstitutions eg. {".*", "."}
	 * @return
	 */
	public static Pattern compileSimplifiedPattern(String pattern, char[] wildcardCharacters, String[] regexSubstitutions) {
		StringBuilder regex = new StringBuilder(pattern);
		regex.insert(0, REGEX_START_ANCHOR + REGEX_START_QUOTE);
		for (int wcardIdx = 0; wcardIdx < wildcardCharacters.length; wcardIdx++) {
			regex.replace(0, regex.length(), regex.toString().replace("" + wildcardCharacters[wcardIdx],
					REGEX_END_QUOTE + regexSubstitutions[wcardIdx] + REGEX_START_QUOTE));
		}
		regex.append(REGEX_END_QUOTE + REGEX_END_ANCHOR);

		return Pattern.compile(regex.toString());
	}

	/**
	 * Gets context URL.
	 * @return
	 */
	public URL getParent() {
		return parent;
	}

	/**
	 * Sets context URL.
	 */
	public void setParent(URL parent) {
		this.parent = parent;
	}
	
	/**
	 * Filename filter for wildcard matching.
	 */
	private static class WcardFilter implements FilenameFilter {
		
		// Regex pattern equivalent to specified wildcard pattern.
		private Pattern rePattern;
		
		/**
		 * Constructor. Creates regex pattern so that it is equivalent to given wildcard pattern. 
		 * @param str Wildcard pattern. 
		 */
		public WcardFilter(String str) {
			rePattern = compileSimplifiedPattern(str);
		}

		/**
		 * Part of FilenameFilter interface.
		 */
		@Override
		public boolean accept(File dir, String name) {
			return rePattern.matcher(name).matches();
		}
		
		public boolean accept(String name){
			return rePattern.matcher(name).matches();
		}
	}

	/**
	 * Supports filename and their input stream.
	 * @author Jan Ausperger (jan.ausperger@javlin.eu)
	 *         (c) Javlin, a.s. (www.javlin.eu)
	 */
	private static class FileStreamName {
		
		// name of file
		private String fileName;
		// input stream - optional
		private InputStream inputStream;

		/**
		 * Constructor.
		 * @param fileName
		 */
		public FileStreamName(String fileName) {
			this(fileName, null);
		}

		/**
		 * Constructor.
		 * @param fileName
		 * @param inputStream
		 */
		public FileStreamName(String fileName, InputStream inputStream) {
			this.fileName = fileName;
			this.inputStream = inputStream;
		}
		
		/**
		 * Gets file name.
		 * @return
		 */
		public String getFileName() {
			return fileName;
		}
		
		/**
		 * Gets input stream.
		 * @return
		 */
		public InputStream getInputStream() {
			return inputStream;
		}

		@Override
		public String toString() {
			return fileName;
		}
		
	}

	/**
	 * Finds embedded source.
	 * 
	 * Example: 
	 * 		source:      zip:http://linuxweb/~jausperger/employees.dat.zip#employees0.dat
	 *      result: (g1) zip:
	 *              (g2) http://linuxweb/~jausperger/employees.dat.zip
	 *              (g3) #employees0.dat
	 * 
	 * @param source - input/output source
	 * @return matcher or null
	 */
	public static Matcher getArchiveURLMatcher(String source) {
		Matcher matcher = ARCHIVE_SOURCE.matcher(source);
		return matcher.find() ? matcher : null;
	}

}
