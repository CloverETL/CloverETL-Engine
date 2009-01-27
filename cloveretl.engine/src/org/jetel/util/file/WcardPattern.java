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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

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
	 */
	public List<String> filenames() {
		List<String> mfiles = new ArrayList<String>();
		for (int i = 0; i < patterns.size(); i++) {
			StringBuffer dirName = new StringBuffer();
			StringBuffer filePat = new StringBuffer();
			String fileName = patterns.get(i);
			boolean fileProtocol = false;

			try {
				fileProtocol = parent != null ? parent.getProtocol().equals("file") :
					FileUtils.getFileURL(fileName).getProtocol().equals("file");
			} catch (MalformedURLException e) {
				// NOTHING
			}
			
			if (!fileProtocol || !splitPattern(fileName, dirName, filePat)) {	// no wildcards
				mfiles.add(fileName);
			} else {
				File dir;
				try {
					dir = parent != null ? new File(FileUtils.getFileURL(parent, dirName.toString()).getPath()) : new File(dirName.toString());
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
	
}
