/*
 *    Copyright (c) 2004-2008 Javlin Consulting s.r.o. (info@javlinconsulting.cz)
 *    All rights reserved.
 */

package org.jetel.metadata.extraction;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that handles guessing of column widths for fixed 
 * length metadata. It operates on flat file metadata model, 
 * which contains GraphMetadata model and other additional 
 * information needed for parsing and analysis.
 *
 * @author Jakub Lehotsky (jakub.lehotsky@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Feb 26, 2008
 */
public class FixedLengthFlatFileHeuristic {
	
	private static final int INTERNAL_BUFFER_LENGTH = 65536;

 	/** How many lines to use in heuristics */
	private static final int LINES_TO_ANALYZE = 10;

	/** Guessed field types */
	private static FieldTypeGuess[] dataFieldTypes = null;
	
	/**
	 * Sets up heuristic guess of column sizes from analysis of first few lines of input file.
	 * 
	 * @param metadataGuess	Flat file metadat model with metadata and parsing information (source and charcode)
	 * 
	 * @throws IOException	In the case of IO error
	 */
	@SuppressWarnings("unchecked")
	public static void analyzeFixlenWithHeuristic(MetadataModelGuess metadataGuess) throws IOException {
				
		List<Integer> result = new ArrayList<Integer>();
		
		Set<Integer> columns[] = new HashSet[3];
		
		Map<Character, Integer>[] charCounter = new HashMap[LINES_TO_ANALYZE];
		for (int i = 0; i < LINES_TO_ANALYZE; i ++) {
			charCounter[i] = new HashMap<Character, Integer>(); 
		}

		// check if all records are on a single input line
		char[] buf = new char[INTERNAL_BUFFER_LENGTH];
		int charRead = (new InputStreamReader(metadataGuess.getInputStream(), metadataGuess.getEncoding())).read(buf);		
			
		String delimString = guessRecordDelimiter(buf);

		if (delimString == null) {
			metadataGuess.setFieldCounts(new Integer[]{});
			return;
		}
		
		for (int i = 0; i < columns.length; i++) {
			columns[i] = new HashSet<Integer>();
		}
		
		int poscounter = 0;
		int lineLength = 0;		
		for (int i = 0; i < LINES_TO_ANALYZE; i++) {
			// points to position of character in a given file
			int posbegin = poscounter;
			while (true) {
				boolean doInterrupt = false;
				for (int j = 0; j < delimString.length(); j++) {
					if ((poscounter +j) >= charRead) {
						doInterrupt = true;
						break;
					}
					if (buf[poscounter + j] == delimString.charAt(j)) {
						doInterrupt = true;
						if (i==0)
							lineLength = poscounter;
					}
				}
				if (doInterrupt) {
					poscounter+=delimString.length();
					if (poscounter >= charRead)
						poscounter = charRead;
					break;
				}
				if (poscounter-posbegin>0)
				{
					if (buf[poscounter-1] == ' ' && buf[poscounter] != ' ') {
						if (i==0) columns[0].add(poscounter-1);
					} else if (i!=0) {
						columns[0].remove(poscounter-posbegin-1);
					}
					
					if (Character.isDigit(buf[poscounter-1]) 
							&& Character.isLetter(buf[poscounter])) {
						if (i==0) columns[1].add(poscounter-1);
					} else if (i!=0) {
						columns[1].remove(poscounter-posbegin-1);
					}

					if (Character.isLetter(buf[poscounter-1]) 
							&& Character.isDigit(buf[poscounter])) {
						if (i==0) columns[2].add(poscounter-1);
					} else if (i!=0) {
						columns[2].remove(poscounter-posbegin-1);
					}
				}
				poscounter++;
				if (poscounter >= charRead)
					break;			
			}
			
			if (poscounter >= charRead)
				break;
		}
			
		Set<Integer> resultSet = new HashSet<Integer>();
		for (int i = 0; i < columns.length; i++) {
			resultSet.addAll(columns[i]);
		}
			
		Iterator<Integer> it = resultSet.iterator() ;
		while (it.hasNext()) {
			result.add(it.next());
		}
		// TODO: is this correct? (added length of delimString) - isn't it a bug in a parser instead???
		result.add(lineLength + delimString.length() - 1);

		Collections.sort(result);
		
		Integer[] res = result.toArray(new Integer[result.size()]);
		
		for (int i = res.length - 1; i > 0; i--) {
			res[i] = res[i] - res[i-1];
		}
		res[0]++;
		
		metadataGuess.setFieldCounts(res);
		
		guessFieldTypes(metadataGuess);
		
	}
		
	/**
	 * Tries to guess field types - analyzes file set up in metadata model and sets up 
	 * field types in the model 
	 * 
	 * @param metadataGuess	Flat file metadata model to operate on
	 * @throws IOException
	 */

	public static void guessFieldTypes(MetadataModelGuess metadataGuess)
		throws IOException {
		
		Integer[] fieldSizes = metadataGuess.getFieldCounts(); 
			// TODO: debug for reparsing - this should be read from actual metadata - it is different, than
			//			last heuristic guess available through get/setFieldCounts()
		
		// check if all records are on a single input line
		char[] buf = new char[INTERNAL_BUFFER_LENGTH];
		int charRead = (new InputStreamReader(metadataGuess.getInputStream(), metadataGuess.getEncoding())).read(buf);
		String recordDelimiter = guessRecordDelimiter(buf);
		
		dataFieldTypes = new FieldTypeGuess[fieldSizes.length];
		for (int i = 0; i<fieldSizes.length ;i++) {
			dataFieldTypes[i] = FieldTypeGuess.defaultGuess();
		}
		
		int offset = 0;
		for (int i = 0; i < LINES_TO_ANALYZE; i++) lineLoopLabel: {

			for (int j = 0; j < fieldSizes.length; j++) {
				
				// in the case we try to parse fields past the actual read buffer.
				// this happens with fewer lines than LINES_TO_ANALYZE or with extra 
				// long records exceeding reading buffer
				if (offset + fieldSizes[j] > charRead) {
					break lineLoopLabel;
				}
				
				String fieldString = new String(buf, offset, fieldSizes[j]);
				if (fieldString.endsWith(recordDelimiter)) {
					//cut off record delimiter
					fieldString = fieldString.substring(0, fieldString.indexOf(recordDelimiter));
				}
				offset += fieldSizes[j];
				FieldTypeGuess guessedType = FieldTypeGuesser.guessFieldType(fieldString);

				if (i == 0) {
					dataFieldTypes[j] = guessedType;
				} else {
					if (!dataFieldTypes[j].equals(guessedType)) {
						dataFieldTypes[j] = FieldTypeGuess.defaultGuess();
					}
				}				
			}
		}
		
		metadataGuess.setProposedTypes(dataFieldTypes);
	}
	
	/**
	 * @param buf Data sample.
	 * @return Record delimiter guess from given sample data. 
	 */
	private static String guessRecordDelimiter(char[] buf) {
		
		String delimString = null;
		
		for (int recordDelimPosition = 0; recordDelimPosition < buf.length && delimString == null;) {
			switch (buf[recordDelimPosition]) {
				case '\n':
					delimString = String.valueOf(buf[recordDelimPosition]);
					if (recordDelimPosition > 0 && buf[recordDelimPosition-1] == '\r') {
						// use \r\n as delimiter if detected
						delimString = "\r" + delimString; //$NON-NLS-1$
						recordDelimPosition--;
					}
				break;
				default: 
					recordDelimPosition++;
					break;
			}
		}
		return delimString;
	}

}
