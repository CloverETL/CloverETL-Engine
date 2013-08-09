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
package org.jetel.metadata.extraction;

import java.io.IOException;

/**
 * @author slamam (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.8.2013
 */
public class FlatFileAnalyzer {

	private static InputAnalyzer inputAnalyzer = new FlatFileInputAnalyzer();
	
	public static MetadataModelGuess guessMetadata(String metadataName, MetadataModelGuess metadataGuess, String encoding) throws Exception {
		
		metadataGuess.setFieldDelimiter(null);
		metadataGuess.setEncoding(encoding);
		metadataGuess.setFieldCounts(null);
		metadataGuess.setOriginName(metadataName);
		metadataGuess.setProposedTypes(null);
		metadataGuess.setSkipSourceRows(0);
		
		int fieldCount;
		try {
			// run fix length heuristic
			FixedLengthFlatFileHeuristic.analyzeFixlenWithHeuristic(metadataGuess);
			fieldCount = metadataGuess.getFieldCountSize(); 
			if (fieldCount > 1) {
				CommonHeuristic.guessExtractNames(metadataGuess,
						new IGuessTypes() {
							@Override
							public void guessFieldTypes(MetadataModelGuess importedMetadata, boolean forceImpliedSettings) throws IOException {
								FixedLengthFlatFileHeuristic.guessFieldTypes(importedMetadata);
							}
						});
			}
		} catch (IOException e) {
			metadataGuess.setFieldCounts(null);
		}

		try {
			// run delimited heuristic
			DelimitedFlatFileHeuristic.analyzeDelimitedWithHeuristic(metadataGuess);
			if (metadataGuess.getProposedTypes().length > 1) {
				CommonHeuristic.guessExtractNames(metadataGuess,
						new IGuessTypes() {
							@Override
							public void guessFieldTypes(MetadataModelGuess importedMetadata, boolean forceImpliedSettings) throws IOException {
								DelimitedFlatFileHeuristic.guessFieldTypes(importedMetadata, forceImpliedSettings);
							}
						});
			}
		} catch (IOException e) {
			metadataGuess.setFieldDelimiter(null);
		}
		
		inputAnalyzer.analyze(metadataGuess);
		return metadataGuess;
	}
}
