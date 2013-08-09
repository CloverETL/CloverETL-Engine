/*
 *    Copyright (c) 2004-2008 Javlin Consulting s.r.o. (info@javlinconsulting.cz)
 *    All rights reserved.
 */

package org.jetel.metadata.extraction;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.metadata.DataFieldType;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;

/**
 * Class that handles guessing of delimiter character. It operates 
 * on flat file metadata model, which contains GraphMetadata 
 * model and other additional information needed for parsing
 * and analysis.
 * 
 * @author Jakub Lehotsky (jakub.lehotsky@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *         
 * @since Feb 25, 2008
 */
public class DelimitedFlatFileHeuristic {
	
	private static final int INTERNAL_BUFFER_LENGTH = 65536;
	
	private final static Comparator<PossibleDelimiterData> comparator = new PossibleOccurenceComparator();

	/** These characters are preffered, if the result is undecidable */
	private static final String preferredDelimiters = ",.;:|"; //$NON-NLS-1$
	/** These characters have lower prirority as delimiters, if the result is undecidable */ 
	private static final String unpreferredDelimiters = "'\" "; //$NON-NLS-1$

	/** How many lines to use in heuristics */
	private static final int LINES_TO_ANALYZE = 100;

	/** This is delimiter to use, if the result couldn't be guessed */
	public static final String COULD_NOT_GUESS = ""; //$NON-NLS-1$

	/** Guessed field types */
	private static FieldTypeGuess[] dataFieldTypes = null;

	/**
	 * Returns heuristic guess of delimiter got from analysis of first few lines of input file -
	 * delimiter is set directly in the metadata
	 *
	 * @param metadataGuess	Flat file metadat model with metadata and parsing information (source and charcode)
	 * 
	 * @throws IOException	In the case of IO error
	 */
	
	@SuppressWarnings("unchecked")
	public static void analyzeDelimitedWithHeuristic(MetadataModelGuess metadataGuess) 
		throws IOException {
		
		int linesToAnalyze;
		
		// Initialize character counters
		Map<Character, Integer>[] charCounter = new HashMap[LINES_TO_ANALYZE];
		for (int i = 0; i < LINES_TO_ANALYZE; i ++) {
			charCounter[i] = new HashMap<Character, Integer>(); 
		}		
						
		// check if all records are on a single input line
		char[] buf = new char[INTERNAL_BUFFER_LENGTH];
		int charRead = (new InputStreamReader(metadataGuess.getInputStream(), metadataGuess.getEncoding())).read(buf);		
			
		String delimString = guessRecordDelimString(buf);

		// We need record delimiter for analysis
		if (delimString == null) {
			delimString = ""; //$NON-NLS-1$
			metadataGuess.setFieldDelimiter(COULD_NOT_GUESS); 
//			return;
		}
		
		int poscounter = 0;
		linesToAnalyze = 0;
		// for each line of input file
		for (int i = 0; i < LINES_TO_ANALYZE; i++) {
			int posbegin = poscounter;

			// Find the position of next record
			while (true) {				
				boolean doInterrupt = false;
				// Detect the record delimiter
				for (int j = 0; j < delimString.length(); j++) {
					if ((poscounter +j) >= charRead) {
						doInterrupt = true;
						break;
					}
					if (buf[poscounter + j] == delimString.charAt(j)) {
						doInterrupt = true;
					}
				}
				// Avoid reading past the end of buffer
				if (doInterrupt) {
					poscounter+=delimString.length();
					if (poscounter >= charRead)
						poscounter = charRead;
					break;
				}
				poscounter++;
				if (poscounter >= charRead)
					break;
			}
			
			// if last line isn't delimited, it may be cut by one or two chars
			for (int j = posbegin; j < poscounter - delimString.length(); j++) {  
				Character ch = buf[j];
				// do the counting of characters - letters and digits are not 
				// recognized as possible delimiters (by this heuristic)
				// we store the count of each character in each line (i is line number)
				if (!Character.isLetterOrDigit(ch)) {
					if (!charCounter[i].containsKey(ch)) {
						charCounter[i].put(ch, 1);
					} else {
						charCounter[i].put(ch, charCounter[i].get(ch) + 1);
					}					
				}
			}
			linesToAnalyze++;
			if (poscounter >= charRead)
				break;
		}

		PossibleDelimiterData[] occurenceData = new PossibleDelimiterData[charCounter[0].keySet().size()];
		// Take occurences in the first line and compare with all the others
		int charIndex = 0;
		for (char ch : charCounter[0].keySet()) {

			int currentCount = charCounter[0].get(ch);
			long distance = 0;
			int maxOccurence = 0;

			for (int j = 1; j < linesToAnalyze; j++) {
				Object countObj = charCounter[j].get(ch);
				int count;
				if (countObj != null) {
					count = (Integer) countObj;
				} else {
					count = 0;
				}
				int tmp = count - currentCount;
				distance += (long) tmp * (long) tmp;
				maxOccurence = Math.max(maxOccurence, count);
			}

			/* Following lines ensures that preferred are always before the set which is not 
			 * in the preferred neither unpreffered which is before unpreferred */
			if (unpreferredDelimiters.indexOf(ch) > -1)
				distance += 1;
			if (preferredDelimiters.indexOf(ch) > -1)
				distance -= 1;

			// distance is the sum of squares of differences between count in the given 
			// line and count in the first line, adjusted by preferrence maxOccurence 
			// is simply the maximal occurrence count of character per line

			occurenceData[charIndex] = new PossibleDelimiterData(ch, distance, maxOccurence);
			charIndex++;
		}
		
		if (occurenceData != null && occurenceData.length > 0) {
			// Now, let's take best guess - it is the one with the smallest distance - if it is the same,
			// take the one, which has higher maximal occurrence per line
			Arrays.sort(occurenceData, comparator);
			// set the delimiter and continue with guessing of field types
			metadataGuess.setFieldDelimiter(StringUtils.specCharToString(String.valueOf(occurenceData[0].ch)));
		}
		
		//set field types
		guessFieldTypes(metadataGuess, true);
	}

	/**
	 * Guesses for record delimiter of the input contained in buffer. 
	 * 
	 * @param buf	Buffer to analyze
	 * @return	The record delimiter
	 */
	private static String guessRecordDelimString(char[] buf) {
		
		String delimRecordString = null;
		
		int recordDelimPosition = -1;
		for (recordDelimPosition = 0; recordDelimPosition < buf.length && delimRecordString == null;) {
			switch (buf[recordDelimPosition]) {
				case '\n':
					delimRecordString = String.valueOf(buf[recordDelimPosition]);
					if (recordDelimPosition > 0 && buf[recordDelimPosition-1] == '\r') {
						// use \r\n as delimiter if detected
						delimRecordString = "\r" + delimRecordString; //$NON-NLS-1$
						recordDelimPosition--;
					}
					break;
				case '\r':
					delimRecordString = "\r"; //$NON-NLS-1$
					if (recordDelimPosition < buf.length - 1 && buf[recordDelimPosition + 1] == '\n') {
						delimRecordString = "\r\n"; //$NON-NLS-1$
					}
					break;

				default: 
					recordDelimPosition++;
					break;
			}
		}

		return delimRecordString;
	}


	/**
	 * Tries to guess field types - analyzes file set up in metadata model and sets up 
	 * field types in the model 
	 * 
	 * @param importedMetadata	Flat file metadata model to operate on
	 * @param forceImpliedSettings	Set to true if record properties have to be changed (set to true only for first metadata construction or after "Reparse")
	 * @throws IOException
	 */
	public static void guessFieldTypes(MetadataModelGuess importedMetadata, boolean forceImpliedSettings) 
		throws IOException {		
		
		char guessedDelimiter = 0x00;
		String del = importedMetadata.getFieldDelimiter();
		if (!StringUtils.isEmpty(del))
			guessedDelimiter = StringUtils.stringToSpecChar(del).charAt(0);
		
		QuotingDecoder quoteDecoder = null;
		if (importedMetadata.isQuotedStrings()) {
			quoteDecoder = new QuotingDecoder();
			quoteDecoder.setQuoteChar(importedMetadata.getQuoteChar());
		}
				
		// check if all records are on a single input line
		char[] buf = new char[INTERNAL_BUFFER_LENGTH];		
		int charRead = (new InputStreamReader(importedMetadata.getInputStream(), importedMetadata.getEncoding())).read(buf);
		
		int fieldCount = 0;
		
		String delimString = guessRecordDelimString(buf);
		if(delimString == null)
			throw new IOException("Can't detect delimiter");

		// count field delimiters (outside quotes)
		boolean inQuote = false;
		int skippedLines = 0;
		for (int i = 0; i < Math.min(buf.length, charRead)-delimString.length(); i++) {
			if (quoteDecoder != null) {
				if (inQuote) {
					if (quoteDecoder.isEndQuote(buf[i])) {
						if (i + 1 < buf.length && quoteDecoder.isEndQuote(buf[i + 1])) {
							i++; // skip escaped (doubled) quote
						} else {
							inQuote = false;
						}
					}
					continue;
				} else {
					if (quoteDecoder.isStartQuote(buf[i])) {
						inQuote = true;
						continue;
					}
				}
			}
			// we are outside quotes here
			
			boolean doInterrupt = true;
			for (int j = 0; j < delimString.length(); j++) {
				if (buf[i + j] != delimString.charAt(j)) {
					doInterrupt = false;				
				}
			}
			if (doInterrupt) {
				skippedLines++;
				if (skippedLines > importedMetadata.getSkipSourceRows()) {
					break;
				} else {
					fieldCount = 0;
					continue;
				}
			}
			if (buf[i] == guessedDelimiter)
				fieldCount++;
		}
		fieldCount++;
		
		// Dumbest guess - all fields are string
		dataFieldTypes = new FieldTypeGuess[fieldCount];
		for (int i = 0; i<fieldCount ;i++) {
			dataFieldTypes[i] = FieldTypeGuess.defaultGuess();
		}
		
		FieldTypeGuess[][] detectedDataFieldTypes = guessFieldTypesByValues(importedMetadata, delimString, guessedDelimiter, buf, charRead);
		for (int i = 0; i < fieldCount; i++) {
			dataFieldTypes[i] = FieldTypeGuesser.guessFieldType(detectedDataFieldTypes[i]);
		}
			
		importedMetadata.setProposedTypes(dataFieldTypes);
		if (forceImpliedSettings) {
			int newSkipGuess = guessSkipSourceRow(detectedDataFieldTypes);
			if (newSkipGuess > importedMetadata.getSkipSourceRows()) {
				importedMetadata.setSkipSourceRows(newSkipGuess);
			}
		}
	}
	
	/**
	 * Reads sample data and guesses type of field from it's values.
	 * 
	 * @param importedMetadata Model of metadata.
	 * @param recordDelimiter Detected record delimiter.
	 * @param fieldsDelimiter Detected fields delimiter
	 * @param buf Sample data to analyze.
	 * @param charRead Count of chars to be used from buffer for analysis.
	 * 
	 * @return Matrix of types guessed from values.
	 */
	private static FieldTypeGuess[][] guessFieldTypesByValues(MetadataModelGuess importedMetadata, String recordDelimiter, char fieldsDelimiter, char[] buf, int charRead) {
		
		int line = 0;
		int posbegin = 0;
		int poscounter = 0;
		FieldTypeGuess[][] detectedDataFieldTypes = new FieldTypeGuess[dataFieldTypes.length][LINES_TO_ANALYZE];
		while (line < LINES_TO_ANALYZE) {
			boolean doInterrupt = false;
			boolean nextLine = true;
			for (int j = 0; j < recordDelimiter.length(); j++) {
				// end of buffer detection
				if ((poscounter + j) >= charRead) {
					doInterrupt = true;
					break;
				}
				// edge of lines detection
				if (poscounter + Math.max(1, recordDelimiter.length()) >= charRead) {
					nextLine = true;
				} else if (buf[poscounter + j] != recordDelimiter.charAt(j)) {
					nextLine = false;
				}
			}
			if (nextLine) {
				poscounter += Math.max(1, recordDelimiter.length());
				if (poscounter >= charRead) {
					doInterrupt = true;
				}
				// line begin and end index is stored now in posbegin and poscounter
				if (importedMetadata.getSkipSourceRows() <= line && line >= 0) {
					// Read and split the line
					String aline = new String(buf, posbegin, poscounter-posbegin-recordDelimiter.length());
					String[] parsed = getFieldValues(aline, fieldsDelimiter, importedMetadata);
					
					if (dataFieldTypes.length != parsed.length) {
						//Incomplete line was read -> interrupt processing of the file (already read data will be used 
						//for guessing field types). Possible reasons are:
						//- corrupted file (different count of fields delimiters at current line)
						//- too big file (INTERNAL_BUFFER_LENGTH < lenght_of_text_being_analyzed )
						break;
					}
					
					for (int i = 0; i < dataFieldTypes.length; i++) {
						FieldTypeGuess guessedType;
						if (parsed.length > i) {
							guessedType = FieldTypeGuesser.guessFieldType(parsed[i]); 
						} else {
							guessedType = FieldTypeGuess.defaultGuess();
						}
						detectedDataFieldTypes[i][line] = guessedType;
					}
				}
				
				posbegin=poscounter;
				line++;
			}
			if (doInterrupt) {
				// don't allow reading past the end of buffer 
				break; 
			}
			poscounter++;
		}
		return detectedDataFieldTypes;
	}
	
	/**
	 * Guesses how many rows should be skipped when reading the source (header).
	 * 
	 * @param detectedDataField Types Matrix of detected types.
	 * 
	 * @return Count of source rows to be skipped.
	 */
	private static int guessSkipSourceRow(FieldTypeGuess[][] detectedDataFieldTypes) {
		
		if (detectedDataFieldTypes != null) {
			int skipSourceRow = 0;
			for (int i = 0; i < detectedDataFieldTypes.length; i++) {
				if (!DataFieldType.STRING.equals(dataFieldTypes[i].getType())) {
					for (int j = 0; j < detectedDataFieldTypes[i].length; j++) {
						if (detectedDataFieldTypes[i][j] == null || !DataFieldType.STRING.equals(detectedDataFieldTypes[i][j].getType())) {
							if (skipSourceRow <= 0) {
								skipSourceRow = j;
							} else if (skipSourceRow != j) {
								return 0;
							}
							break;
						}
					}
				}
			}
			return skipSourceRow;
		}
		return 0;
	}
	
	/**
	 * Splits given line by given delimiter and unquote the values is required. Handles quoted strings.
	 * 
	 * @param aline Line of source (record).
	 * @param guessedDelimiter Fields delimiter.
	 * @param importedMetadata Metadata model.
	 * 
	 * @return Array of fields values.
	 */
	private static String[] getFieldValues(String aline, char guessedDelimiter, MetadataModelGuess importedMetadata) {
		
		List<String> values = new ArrayList<String>();
		QuotingDecoder quoteDecoder = new QuotingDecoder();
		char[] buf = aline.toCharArray();
		boolean inQuote = false;
		boolean startQuote = false;
		boolean endQuote = false;
		boolean allFieldsQuoted = true;
		int startIndex = 0;
		
		for (int i = 0; i < buf.length; i++) {
			if (quoteDecoder != null) {
				if (inQuote) {
					if (quoteDecoder.isEndQuote(buf[i])) {
						if (i + 1 < buf.length && quoteDecoder.isEndQuote(buf[i + 1])) {
							i++;
						} else {
							inQuote = false;
						}
						endQuote = true;
						if (i + 1 == buf.length) {
							//end of buffer reached with end quote
							if (startIndex < i) {
								values.add(aline.substring(startIndex, i));
							} else {
								values.add(null);
							}
						}
					}
					continue;
				} else if (quoteDecoder.isStartQuote(buf[i])) {
					inQuote = true;
					startIndex = i + 1;
					startQuote = true;
					continue;
				} else if (buf[i] == guessedDelimiter || i == buf.length - 1) {
					int endIndex = i;
					if (startQuote && endQuote) {
						endIndex--;
					} else {
						allFieldsQuoted = false;
					}
					if (i == buf.length - 1 && buf[i] != guessedDelimiter) {
						endIndex++;
					}
					if (startIndex < endIndex) {
						values.add(aline.substring(startIndex, endIndex));
					} else {
						values.add(null);
					}
					if (i == buf.length - 1 && buf[i] == guessedDelimiter) {
						//line ends with field delimiter followed by record delimiter
						values.add(null);
					}
					startIndex = i + 1;
					startQuote = false;
					endQuote = false;
				}
			}
		}
		if (buf.length == 0) {
			//buffer is empty - CLO-1244
			values.add("");
		}
		if (allFieldsQuoted) {
			importedMetadata.setQuotedStrings(true);
		}
		return values.toArray(new String[values.size()]);
	}
}

/**
 * Helper data holding class for delimiter statistics analysis
 * 
 * @author Jakub Lehotsky (jakub.lehotsky@javlinconsulting.cz) (c) Javlin Consulting (www.javlinconsulting.cz)
 * 
 * @since Feb 26, 2008
 */

class PossibleDelimiterData {
	
	public PossibleDelimiterData(char ch, long distance, int maxOccurence) {
		this.ch = ch;
		this.quadDistance = distance;
		this.maxOccurence = maxOccurence;
	}
	
	public Character ch;
	public Integer maxOccurence;
	public Long quadDistance;
	
	@Override
	public String toString() {
		return MessageFormat.format("{0} {1} {2}", ch.toString(), maxOccurence.toString(), quadDistance.toString()); //$NON-NLS-1$
	}
}

class PossibleOccurenceComparator implements Comparator<PossibleDelimiterData> {
	
	@Override
	public int compare(PossibleDelimiterData o1, PossibleDelimiterData o2) {
		if (o2 == null) {
			return 0;
		}
		int res = o1.quadDistance.compareTo(o2.quadDistance);
		if (res != 0) {
			return res;
		} else {
			return -o1.maxOccurence.compareTo(o2.maxOccurence);
		}
	}
}