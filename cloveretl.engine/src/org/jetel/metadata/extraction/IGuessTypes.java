package org.jetel.metadata.extraction;

import java.io.IOException;

/**
 * Internal interface - used as a strategy for the "extract names" indicator heuristic. Not for use use outside this package
 * @author jakub
 *
 */
interface IGuessTypes {
	
	public void guessFieldTypes(MetadataModelGuess importedMetadata, boolean forceImpliedSettings) throws IOException;
	
}
