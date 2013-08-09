/*
 *    Copyright (c) 2004-2008 Javlin Consulting s.r.o. (info@javlinconsulting.cz)
 *    All rights reserved.
 */

package org.jetel.metadata.extraction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;

/**
 * This class implements data analyzer (see InputAnalyzer interface) for
 * the purposes of text flat file analysis and parsing. 
 * @author misho, Jakub Lehotsky 
 *
 */
public class FlatFileInputAnalyzer implements InputAnalyzer {

	private static final int INTERNAL_BUFFER_LENGTH = 65536;
	
	/**
	 * Method performs analysis of input incoming from a flat file. <code>input</code> parameter
	 * should be of type FileInputStream. <code>recordMetadata</code> should contain either
	 * a default delimiter or a default field length from which record fields are generated.
	 */
	@Override
	public void analyze(MetadataModelGuess importedMetadata) throws Exception {

		// FIXME: later will be ignored and delimiter will be determined by sophisticated analysis :)
			// TODO: remove above fixme gently as sophisticated analysis is in the other place ;)
		if (importedMetadata.getFieldDelimiter() == null &&
				(importedMetadata.getFieldCountSize() == MetadataModelGuess.FIELD_COUNT_UNSPECIFIED ||
						importedMetadata.getFieldCountSize() == 0)) {
			// neither delimiter nor default field count is specified
			throw new IllegalArgumentException("Neither delimiter nor field count specified");
		}		

		QuotingDecoder quoteDecoder = null;
		if (importedMetadata.isQuotedStrings()) {
			quoteDecoder = new QuotingDecoder();
			quoteDecoder.setQuoteChar(importedMetadata.getQuoteChar());
		}
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(importedMetadata.getInputStream(), importedMetadata.getEncoding()));

		// check if all records are on a single input line
		char[] buf = new char[INTERNAL_BUFFER_LENGTH];
		int readChars = reader.read(buf);
		
		String recordDelim = null;
		
		boolean inQuote = false;
		int recordDelimPosition = -1;
		for (recordDelimPosition = 0; recordDelimPosition < buf.length && recordDelim == null;) {
			if (quoteDecoder != null) {
				if (inQuote) {
					if (quoteDecoder.isEndQuote(buf[recordDelimPosition])) {
						if (recordDelimPosition + 1 < buf.length && quoteDecoder.isEndQuote(buf[recordDelimPosition + 1])) {
							recordDelimPosition++; // skip escaped (doubled) quote
						} else {
							inQuote = false;
						}
					}
					recordDelimPosition++;
					continue;
				} else {
					if (quoteDecoder.isStartQuote(buf[recordDelimPosition])) {
						inQuote = true;
						recordDelimPosition++;
						continue;
					}
				}
			}

			switch (buf[recordDelimPosition]) {
				case '\n':
					recordDelim = "\\n";
					if (recordDelimPosition > 0 && buf[recordDelimPosition-1] == '\r') {
						// use \r\n as delimiter if detected
						recordDelim = "\\r" + recordDelim;
						recordDelimPosition--;
					}
					
					// warn user if delimiter is the first character of input
					if (recordDelimPosition <= 0) {
						LogFactory.getLog(FlatFileInputAnalyzer.class).error("Record delimiter was found as first character of input. Parsing will not work!");
					}
 					
					break;
				case '\r':
					recordDelim = "\\r";
					if (recordDelimPosition < buf.length - 1 && buf[recordDelimPosition + 1] == '\n') {
						recordDelim = "\\r\\n";
					}
					break;
					
				default: 
					recordDelimPosition++;
					break;
			}
		}

		char[] cleanBuf = null;
		if (recordDelim == null) {
			if (importedMetadata.getMetadataType() == DataRecordParsingType.DELIMITED) {
				// no record delimiter has been found
				/*
				 * a little cheating here... we will remove last field (which may be incomplete) together with preceding
				 * delimiter if the input is larger than buffer. Then we will parse the line as if it was a record on a
				 * single line. Otherwise eofAsDelimiter is set to true.
				 */
				if (readChars < INTERNAL_BUFFER_LENGTH) {
					int delimPos = findLastDelimiter(buf, importedMetadata.getFieldDelimiter().toCharArray());
					if (delimPos == -1) {
						// no field delimiter has been found - it is a long field or what
						// use the original buffer as whole record
						LogFactory.getLog(FlatFileInputAnalyzer.class).error("Input line does not contain field delimiter");
						cleanBuf = buf;
					} else {
						// remove delimiter and (incomplete) field following it
						cleanBuf = buf;
					}
				} else {
					// there is probably unsupported record delimiter or record is too long. Extraction can not be
					// performed correctly.
					LogFactory.getLog(FlatFileInputAnalyzer.class).error("Record delimiter was not found. Incorrect record delimiter configured or input record/header size larger than " + INTERNAL_BUFFER_LENGTH + ".");
				}
			} else {
				// fixed record -> create fixed-length columns from the original buffer
				cleanBuf = buf;
			}
		} else {
			// found record delimiter -> remove it (with the rest of possibly incomplete records)
			cleanBuf = new char[recordDelimPosition];
			System.arraycopy(buf,0,cleanBuf,0,recordDelimPosition);
		}

		// do analysis according to the record type
		if (importedMetadata.getMetadataType() == DataRecordParsingType.FIXEDLEN) {
			analyzeFixed(cleanBuf, importedMetadata);
		} else {
			analyzeDelimited(cleanBuf, importedMetadata, recordDelim);
		}
		
	}
	
	private CharSequence[] parseDelimited(String lineToParse, String delimiter, QuotingDecoder quoteDecoder) {
		List<CharSequence> output = new ArrayList<CharSequence>();
		//StringBuffer buf = new StringBuffer(lineToParse);
		
		if (StringUtils.isEmpty(delimiter)) {
			return new String[] {lineToParse};
		}

		int indexAfterLastDelimiter = 0;
		boolean inQuote = false;
		for (int i = 0; i <= lineToParse.length() - delimiter.length(); i++) {
			if (quoteDecoder != null) {
				if (inQuote) {
					if (quoteDecoder.isEndQuote(lineToParse.charAt(i))) {
						if (i + 1 < lineToParse.length() && quoteDecoder.isEndQuote(lineToParse.charAt(i+1))) {
							i++; // skip escaped (doubled) quote
						} else {
							inQuote = false;
						}
					}
					continue;
				} else {
					if (quoteDecoder.isStartQuote(lineToParse.charAt(i))) {
						inQuote = true;
						continue;
					}
				}
			}
			
			if (delimiter.equals(lineToParse.substring(i, i + delimiter.length()))) {
				
				CharSequence s = lineToParse.substring(indexAfterLastDelimiter, i);
				if (quoteDecoder != null) {
					s = quoteDecoder.decode(s);
				}
				output.add(s);
				
				i += delimiter.length() - 1;
				indexAfterLastDelimiter = i + 1;
			}
		}
		
		// check if there's some text after last delimiter and add it to the result
		if (indexAfterLastDelimiter < lineToParse.length() + 1) {
			CharSequence s = lineToParse.substring(indexAfterLastDelimiter, lineToParse.length());
			if (quoteDecoder != null) {
				s = quoteDecoder.decode(s);
			}
			output.add(s);
		}
		
		
		return output.toArray(new CharSequence[0]);
	}

	private String[] parseFixed(String lineToParse, int fieldLength) {
		List<String> output = new ArrayList<String>();

		for (int i = 0; i < lineToParse.length(); i += fieldLength) {
			output.add(lineToParse.substring(i, i + fieldLength));
		}
		
		return output.toArray(new String[output.size()]);
		
	}
	
	/**
	 * Locates position of the last delimiter in buffer. Method was written to avoid creating
	 * redundant string allocations to perform search using <code>String.lastIndexOf(String)</code>
	 * @param buf 			Buffer to search in
	 * @param delimArray	delimiter to locate
	 * @return Position of first character of delimiter inside the buffer, or -1 when none was 
	 * 		   found
	 */
	private int findLastDelimiter(char[] buf, char[] delimArray) {
		boolean ok;
		for (int i = buf.length - 1; i >= 0; i -= delimArray.length) {
			ok = true;// search for delimiter
			for (int j = 0; j < delimArray.length && ok; j++) {
				ok = buf[i + j] == delimArray[j];
			}
			
			if (ok) {
				return i;
			}
		}

		return -1;
	}
	
	private void analyzeDelimited(char[] inputLine, MetadataModelGuess importedMetadata, String recordDelimiter) {
		
		QuotingDecoder quoteDecoder = null;
		if (importedMetadata.isQuotedStrings()) {
			quoteDecoder = new QuotingDecoder();
			quoteDecoder.setQuoteChar(importedMetadata.getQuoteChar());
		}
		
		// parse line according to the delimiter
		String delimiter = StringUtils.stringToSpecChar(importedMetadata.getFieldDelimiter());
		CharSequence[] parsedLine= parseDelimited(String.valueOf(inputLine), delimiter, quoteDecoder);		
		
		// TODO: join fields containing quoted delimiter
		
		importedMetadata.setFieldDelimiter(importedMetadata.getFieldDelimiter());
		importedMetadata.setRecordDelimiter(recordDelimiter);

		String[] originalNames = new String[parsedLine.length];
		
		for (int i=0; i<parsedLine.length; i++) {
			// use either generic name or field name from the first line
			
			CharSequence fieldName;
			if (importedMetadata.getSkipSourceRows() == 1) {
				fieldName = parsedLine[i]; // the name is normalized later
			} else {
				fieldName = "Field" + (i+1);
			}
			// the names will be set later
			originalNames[i] = (fieldName != null) ? fieldName.toString() : null;
		}
		
		String[] modifiedNames = importedMetadata.isNormalizeNames()
				? StringUtils.normalizeNames(originalNames)
				: originalNames;
				
		boolean equal = Arrays.equals(originalNames, modifiedNames);

		int i = 0;
		for (FieldTypeGuess guess: importedMetadata.getProposedTypes()) {
			guess.setName(modifiedNames[i]);
			if (!equal) {
				guess.setLabel(StringUtils.specCharToString(originalNames[i]));
			}
			i++;
		}
	}
	
	private void analyzeFixed(char[] inputLine, MetadataModelGuess importedMetadata) {
		
		// TODO: this must be improved for extractNames functionality - it doesn't work
		//		 with column widths specified. However extractNames is not used for
		//		 fixed length metadata importing right now
		
		// all record lines do have the same length, compute average field length
		int fieldLength = inputLine.length / importedMetadata.getFieldCountSize();

		// extract names
		String[] fieldNames = null;
		if (importedMetadata.getSkipSourceRows() == 1) {
			fieldNames = parseFixed(String.valueOf(inputLine), fieldLength);
		}

		importedMetadata.setFieldDelimiter(importedMetadata.getFieldDelimiter());
		importedMetadata.setRecordDelimiter(null);

		String[] originalNames = new String[importedMetadata.getFieldCountSize()];
		
		for (int i=0; i < importedMetadata.getFieldCountSize(); i++) {
			// use either generic name or field name from the first line
			
			originalNames[i] = importedMetadata.getSkipSourceRows() == 1 ? fieldNames[i] : "Field"+(i+1);
		}
		
		// names are not yet extracted for fixed metadata, added for future safety 
		String[] modifiedNames = importedMetadata.isNormalizeNames()
				? StringUtils.normalizeNames(originalNames)
				: originalNames;

		boolean equal = Arrays.equals(originalNames, modifiedNames);

		int i = 0;
		for (FieldTypeGuess guess: importedMetadata.getProposedTypes()) {
			guess.setName(modifiedNames[i]);
			if (!equal) {
				guess.setLabel(StringUtils.specCharToString(originalNames[i]));
			}
			i++;
		}	
	}
}
