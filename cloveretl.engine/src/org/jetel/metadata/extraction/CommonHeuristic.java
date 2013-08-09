package org.jetel.metadata.extraction;

import java.io.IOException;

import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.extraction.FieldTypeGuess;

/**
 * Helper class for heuristics, contains several methods for heuristic guessing common to all types of metadata (regardless of 
 * delimination methods)
 * 
 * @author jakub
 *
 */
public class CommonHeuristic {
	

	public static void guessExtractNames(MetadataModelGuess model, IGuessTypes typesGuesser) {
		
		try {
			MetadataModelGuess model1 = new MetadataModelGuess(model);
			model1.setSkipSourceRows(0);
			typesGuesser.guessFieldTypes(model1, false);
			MetadataModelGuess model2 = new MetadataModelGuess(model);
			model2.setSkipSourceRows(1);
			typesGuesser.guessFieldTypes(model2, false);
			
			for (FieldTypeGuess proposedType : model1.getProposedTypes()) {
				if (proposedType != null && !proposedType.getType().equals(DataFieldType.STRING.getName()))
					return;
			}
			
			boolean allAreStrings = true;
			for (FieldTypeGuess proposedType : model2.getProposedTypes()) {
				if (proposedType != null && !proposedType.getType().equals(DataFieldType.STRING.getName()))
					allAreStrings = false;
			}
			
			if (!allAreStrings) {
				model.setSkipSourceRows(1);
			}
		} catch (IOException e) {
			return; // we simply won't try guessing this as we cannot read files 
		}
	}
}
