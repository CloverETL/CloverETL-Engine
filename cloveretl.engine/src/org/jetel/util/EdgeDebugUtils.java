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
package org.jetel.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.string.StringUtils;

/**
 * Collection of small utilities for file name manipulation for edge debug files.
 * 
 * Edge debug file name has following pattern: ${runId}-${edgeId}[-${index}].dbg
 * 
 * Where "index" is used to distinguish multiplied edges connected to cluster components. 
 * 
 * Naming convention: "unique edge id" is unambiguous identification of all edges even
 * for multiplied edges connected with cluster partitioners and cluster gathers. For example
 * output edge from cluster partitioner with id="edge1" is actually duplicated to ensure cluster
 * partitioning, so several edges is in fact connected edge1, edge1__1, edge1__2, edge1__3, ...
 * 
 * There is small 'brainfuck' in the code, delimiter for edgeId and index for edge identification is
 * double underscore, but debug file used '-' character as delimiter.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.12.2012
 */
public class EdgeDebugUtils {

	/**
	 * Pattern for debug file name "${runId}-${edgeId}[-${index}].dbg"
	 */
	private static final String DEBUG_FILE_NAME_PATTERN = "(\\d*)-(" + StringUtils.OBJECT_NAME_PATTERN + ")(-(\\d+))?\\.dbg";
	
	private static final Pattern PATTERN = Pattern.compile(DEBUG_FILE_NAME_PATTERN);
	
	/** Delimiter between edge id and index of cluster usage */
	private static final String UNIQUE_EDGE_ID_DELIMITER = "__";
	
	/**
	 * Returns edge debug file name for given runId and edgeId. EdgeId can be unique edge identifier.
	 */
	public static String getDebugFileName(long runId, String edgeId) {
		if (runId < 0 || !StringUtils.isValidObjectId(edgeId)) {
			throw new JetelRuntimeException("invalid arguments");
		}
		if (edgeId.contains(UNIQUE_EDGE_ID_DELIMITER)) {
			String[] parts = edgeId.split(UNIQUE_EDGE_ID_DELIMITER);
			if (parts.length == 2) {
				try {
					int index = Integer.parseInt(parts[1]);
					return runId + "-" + parts[0] + "-" + index + ".dbg";
				} catch (NumberFormatException e) {
					//DO NOTHING
				}
			}
		}
		return runId + "-" + edgeId + ".dbg";
	}
	
	/**
	 * Constructs unique edge identification for given edge id and cluster index.
	 * So for values "Edge1" and 2 returns "Edge1__2".
	 */
	public static String assembleUniqueEdgeId(String edgeId, Integer index) {
		return edgeId + ((index != null && index >= 0) ? (UNIQUE_EDGE_ID_DELIMITER + index) : "");
	}

	/**
	 * @return true if the given fileName is correct edge debug file name; false otherwise
	 */
	public static boolean isDebugFileName(String fileName) {
		try {
			parseDebugFileName(fileName);
			return true;
		} catch (JetelRuntimeException e) {
			return false;
		}
	}
	
	/**
	 * @return true if the given fileName is correct edge debug file name and moreover
	 * if the given runId and edgeId is encoded in the given fileName, cluster index is ignored
	 */
	public static boolean isDebugFileName(String fileName, long runId, String edgeId) {
		try {
			ParsingResult parsingResult = parseDebugFileName(fileName);
			return parsingResult.runtId == runId && parsingResult.edgeId.equals(edgeId);
		} catch (JetelRuntimeException e) {
			return false;
		}
	}

	/**
	 * @return unique edgeId encoded in the given fileName (so for "1270-Edge1__2" returns "Edge1__2")
	 */
	public static String extractUniqueEdgeId(String fileName) {
		ParsingResult parsingResult = parseDebugFileName(fileName);
		return assembleUniqueEdgeId(parsingResult.edgeId, parsingResult.index);
	}
	
	/**
	 * @return runId encoded in the given fileName (so for "1270-Edge1__2" returns 1270)
	 */
	public static long extractRunId(String fileName) {
		return parseDebugFileName(fileName).runtId;
	}

	/**
	 * @return edgeId from given uniqueEdgeId (so for "Edge1__2" returns "Edge1")
	 */
	public static String extractEdgeIdFromUniqueEdgeId(String uniqueEdgeId) {
		if (uniqueEdgeId.contains(UNIQUE_EDGE_ID_DELIMITER)) {
			String[] parts = uniqueEdgeId.split(UNIQUE_EDGE_ID_DELIMITER);
			if (parts.length == 2) {
				return parts[0];
			}
		}
		return uniqueEdgeId;
	}

	private static ParsingResult parseDebugFileName(String fileName) {
		ParsingResult result = new ParsingResult();
		
		Matcher m = PATTERN.matcher(fileName);
		if (!m.matches()) {
			throw new JetelRuntimeException("Given filename '" + fileName + "' does not match edge debug file name pattern (${runId}-${edgeId}[-${index}].dbg)");
		}			

		//parse runId
		String runIdStr = m.group(1);
		try {
			long runId = Long.parseLong(runIdStr);
			if (runId < 0) {
				throw new JetelRuntimeException("Debug filename '" + fileName + "' is illegal. Detected runId '" + runIdStr + "' is not valid run identifier.");
			}
			result.runtId = runId;
		} catch (NumberFormatException e) {
			throw new JetelRuntimeException("Debug filename '" + fileName + "' is illegal. Detected runId '" + runIdStr + "' is not valid run identifier.", e);
		}

		//parse edgeId
		String edgeId = m.group(2);
		if (StringUtils.isEmpty(edgeId)) {
			throw new JetelRuntimeException("Given debug filename has empty edgeId.");
		}
		if (!StringUtils.isValidObjectId(edgeId)) {
			throw new JetelRuntimeException("Given debug filename has edgeId '" + edgeId + "', which is not valid graph element identifier.");
		}
		result.edgeId = edgeId;
		
		//parse index
		String indexStr = m.group(4);
		if (indexStr != null) {
			try {
				int index = Integer.parseInt(indexStr);
				if (index < 0) {
					throw new JetelRuntimeException("Debug filename '" + fileName + "' is illegal. Detected index '" + indexStr + "' is not valid.");
				}
				result.index = index;
			} catch (NumberFormatException e) {
				throw new JetelRuntimeException("Debug filename '" + fileName + "' is illegal. Detected index '" + indexStr + "' is not valid number.", e);
			}
		}
		
		
		return result;
	}
	
	private static class ParsingResult {
		public long runtId;
		public String edgeId;
		public Integer index; //can be null
	}
	
}
