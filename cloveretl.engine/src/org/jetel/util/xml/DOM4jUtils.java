package org.jetel.util.xml;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.DOMWriter;

/**
 * Helper class for supporting operations with the dom4j XML data model.
 * @author Pavel Pospichal
 *
 */
public class DOM4jUtils {

	/**
	 * Converts the representation of XML document in dom4j document model into 
	 * the equivalent representation in document model according W3C standard.
	 * @param dom4jDocument
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static org.w3c.dom.Document convertToW3CDOM(Document dom4jDocument) throws IllegalArgumentException {
		org.w3c.dom.Document w3cDocument = null;
		try {
			w3cDocument = new DOMWriter().write(dom4jDocument);
		} catch(DocumentException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		
		return w3cDocument;
	}
}
