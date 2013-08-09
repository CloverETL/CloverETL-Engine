/*
 *    Copyright (c) 2004-2008 Javlin Consulting s.r.o. (info@javlinconsulting.cz)
 *    All rights reserved.
 */

package org.jetel.metadata.extraction;


/**
 * @author misho

 * Input analyzer provides method for analyzing input. Analysis of input CREATES METADATA for
 * record fields (according to some brief information about input). 
 */
public interface InputAnalyzer {

	/**
	 * Method providing input analysis. Metadata record fields are stored in metadataToFill
	 * object for later use. recordMetadata should contain  may content some brief information about helpful
	 * to the analysis (e.g. main delimiter, field length)
	 * 
	 * @param metadataGuess	metadata object where to store metadata information extracted from input 
	 */
	public void analyze(MetadataModelGuess metadataGuess) throws Exception;
	
}
