/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util.file;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.enums.ArchiveType;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/05/06  
 * Class generating collection of filenames which match
 * given wildcard patterns.
 * The pattern may describe either relative or absolute filenames.
 * '*' represents any count of characters, '?' represents one character.
 * Wildcards cannot be followed by directory separators. 
 */
public class WcardPattern {

	/**
	 * Wild card marks.
	 */
	private static String WILD_QUESTION = "?";
	private static String WILD_STAR = "*";

	/**
	 * Filename filter for wildcard matching.
	 */
	private static class WcardFilter implements FilenameFilter {
		
		
		/**
		 * Regex pattern equivalent to specified wildcard pattern.
		 */
		private Pattern rePattern;
		
		/**
		 * ctor. Creates regex pattern so that it is equivalent to given wildcard pattern. 
		 * @param str Wildcard pattern. 
		 */
		public WcardFilter(String str) {

			StringBuffer regex = new StringBuffer(str);
			regex.insert(0, REGEX_START_ANCHOR + REGEX_START_QUOTE);
			for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
				regex.replace(0, regex.length(), 
						regex.toString().replace("" + WCARD_CHAR[wcardIdx],
								REGEX_END_QUOTE + REGEX_SUBST[wcardIdx] + REGEX_START_QUOTE));
			}
			regex.append(REGEX_END_QUOTE + REGEX_END_ANCHOR);

			// Create compiled regex pattern
			rePattern = Pattern.compile(regex.toString());
		}

		/**
		 * Part of FilenameFilter interface.
		 */
		public boolean accept(File dir, String name) {
			return rePattern.matcher(name).matches();
		}
		
		public boolean accept(String name){
			return rePattern.matcher(name).matches();
		}
	}

	/**
	 * Wildcard characters.
	 */
	@SuppressWarnings("MS")
	public final static char[] WCARD_CHAR = {'*', '?'};
	/**
	 * Regex substitutions for wildcards. 
	 */
	private final static String[] REGEX_SUBST = {".*", "."};
	/**
	 * Start sequence for regex quoting.
	 */
	private final static String REGEX_START_QUOTE = "\\Q";
	/**
	 * End sequence for regex quoting
	 */
	private final static String REGEX_END_QUOTE = "\\E";
	/**
	 * Regex start anchor.
	 */
	private final static char REGEX_START_ANCHOR = '^';
	/**
	 * Regex end anchor.
	 */
	private final static char REGEX_END_ANCHOR = '$';
	
	/**
	 * Collection of filename patterns.
	 */
	private List<String> patterns;
	
	private URL parent;
	
	/**
	 * ctor. Creates instance with empty set of patterns.
	 * It doesn't match any filenames initially.  
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
	private boolean splitPattern(String pat, StringBuffer dir, StringBuffer filePat) {
		dir.setLength(0);
		filePat.setLength(0);

		File f = new File(pat);
		dir.append(f.getParent());
		filePat.append(f.getName());

		for (int wcardIdx = 0; wcardIdx < WCARD_CHAR.length; wcardIdx++) {
			if (filePat.indexOf("" + WCARD_CHAR[wcardIdx]) >= 0) { // wildcard found
				return true;
			}
		}

		// no wildcard in pattern
		return false;
	}
	
	/**
	 * Generates filenames matching current set of patterns.
	 * @return Set of matching filenames.
	 * @throws IOException 
	 */
	public List<String> filenames() {
		List<String> mfiles = new ArrayList<String>();
		String fileName;
		
		// goes through filenames separated by ';'
		for (int i = 0; i < patterns.size(); i++) {
			fileName = patterns.get(i);
			try {
				// returns list of names for filename that can have a wild card '?' or '*'
				for (FileStreamName fileStreamName: innerFileNames(fileName, false)) {
					if (fileStreamName.getInputStream() != null) fileStreamName.getInputStream().close();
					mfiles.add(fileStreamName.getFileName());
				}
			} catch (IOException e) { //the zip cannot work for zip:zip:....
				mfiles.add(fileName);
			}
		}
		return mfiles;
	}

	/**
	 * Gets names from sub-archives.
	 * @param fileName
	 * @param outherPathNeedsInputStream
	 * @return
	 * @throws IOException
	 */
    private List<FileStreamName> innerFileNames(String fileName, boolean outherPathNeedsInputStream) throws IOException {
        URL url = null;
        List<FileStreamName> fileStreamNames = new ArrayList<FileStreamName>();
        
		// get inner source
		String originalFileName = fileName;
		Matcher matcher = FileUtils.getInnerInput(fileName);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			fileName = matcher.group(2) + matcher.group(3) + matcher.group(7);
			fileStreamNames = innerFileNames(innerSource, outherPathNeedsInputStream || fileName.contains(WILD_QUESTION) || fileName.contains(WILD_STAR));
		}
		
		// get archive type
		StringBuilder sbAnchor = new StringBuilder();
		StringBuilder sbInnerInput = new StringBuilder();
		ArchiveType archiveType = FileUtils.getArchiveType(fileName, sbInnerInput, sbAnchor);
		String anchor = sbAnchor.toString();
		fileName = sbInnerInput.toString();

        //open channels for all filenames
		List<String> newFileNames = resolveAndSetFileNames(fileName); // returns simple file names
        if (fileStreamNames.size() == 0) {
        	for (String newFileName: newFileNames) {
            	url = FileUtils.getFileURL(parent, newFileName);
            	if ((outherPathNeedsInputStream || (anchor.contains(WILD_QUESTION) || anchor.contains(WILD_STAR))))
            		fileStreamNames.add(new FileStreamName(newFileName, FileUtils.getAuthorizedStream(url)));
            	else fileStreamNames.add(new FileStreamName(newFileName, null));
        	}
        }
        
        List<FileStreamName> newFileStreamNames = new ArrayList<FileStreamName>();
        for (FileStreamName fileStreamName: fileStreamNames) {
            // create archive streams
            if (archiveType == ArchiveType.ZIP) {
            	if (fileStreamName.getInputStream() != null) {
            		// look into an archive and check the anchor
            		List<String> mfiles = new ArrayList<String>();
                	List<InputStream> lis = FileUtils.getZipInputStreams(fileStreamName.getInputStream(), anchor, mfiles);
                	
                	// create list of new names generated from the anchor
                	for (int i=0; lis == null || i<lis.size(); i++) {
                		newFileStreamNames.add(
                				new FileStreamName(originalFileName.replace(anchor, mfiles.get(i)), 
                				lis.get(i)));
                	}
            	} else {
            		// add original name
            		newFileStreamNames.add(new FileStreamName(originalFileName, null));
            	}
            
            // create gzip streams
            } else if (archiveType == ArchiveType.GZIP) {
            	//TODO
            	
            // create tar streams
            } else if (archiveType == ArchiveType.TAR) {
            	//TODO
            
            // return original names
            } else {
            	return fileStreamNames;
            }
        }
        
        return newFileStreamNames;
    }

    /**
     * Gets list of file names or an original name. 
     * @param fileName
     * @return
     */
	private List<String> resolveAndSetFileNames(String fileName) {
		StringBuffer dirName = new StringBuffer();
		StringBuffer filePat = new StringBuffer();
		boolean fileProtocol = false;
		List<String> mfiles = new ArrayList<String>();

		try {
			fileProtocol = parent != null ? parent.getProtocol().equals("file") :
				FileUtils.getFileURL(fileName).getProtocol().equals("file");
		} catch (MalformedURLException e) {
			// NOTHING
		}
		
		if (!fileProtocol || !splitPattern(fileName, dirName, filePat)) {	// no wildcards
			mfiles.add(fileName);
			return mfiles;
		} else {
			File dir;
			try {
				dir = parent != null ? new File(FileUtils.getFileURL(parent, dirName.toString()).toURI()) : new File(dirName.toString());
			} catch (Exception e) {
				dir = new File(dirName.toString());
			} 
			if (dir.exists()) {
				FilenameFilter filter = new WcardFilter(filePat.toString());
				String[] curMatch = dir.list(filter);
				Arrays.sort(curMatch);
				for (int fnIdx = 0; fnIdx < curMatch.length; fnIdx++) {
					File f = new File(dir, curMatch[fnIdx]);
					mfiles.add(f.getAbsolutePath());
				}
			}
		}
		return mfiles;
	}

	public static boolean checkName(String pattern, String name){
		return new WcardFilter(pattern).accept(name);
	}

	public URL getParent() {
		return parent;
	}

	public void setParent(URL parent) {
		this.parent = parent;
	}
	
	/**
	 * Supports filename and their input stream.
	 * @author Jan Ausperger (jan.ausperger@javlin.eu)
	 *         (c) OpenSys (www.opensys.eu)
	 */
	private static class FileStreamName {
		
		private String fileName;
		private InputStream inputStream;

		public FileStreamName(String fileName, InputStream inputStream) {
			this.fileName = fileName;
			this.inputStream = inputStream;
		}
		
		public void setInputStream(InputStream inputStream) {
			this.inputStream = inputStream;
		}
		
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		
		public String getFileName() {
			return fileName;
		}
		
		public InputStream getInputStream() {
			return inputStream;
		}		
	}

}
