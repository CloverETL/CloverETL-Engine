package org.jetel.component;

import java.nio.BufferUnderflowException;
import java.nio.CharBuffer;
import java.util.Properties;

import org.jetel.data.parser.MultiLevelSelector;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

public class HDTNumberSelector implements MultiLevelSelector {

	int skipChars;
	int chosen = -1;
	
	public void choose(CharBuffer arg0)
			throws BufferUnderflowException {
		char c;
		// read characters
		// and discard newlines and comments
		int inCommentType = 0;
		int commentNest = 0;
		int commentChars = 0;
		do {
			c = arg0.get();
			if (c == '\r' || c == '\n') {
				if (inCommentType == 1) {
					inCommentType = 0;
				}
				skipChars++;
				continue;
			} else if ((inCommentType == 1)) {
				skipChars++;
				continue;
			} else if ((c == '#') && inCommentType == 0) {
				skipChars++;
				inCommentType = 1;
				continue;
			} else if ((commentChars == 0) && (c == '/')) { 
				commentChars++;
				skipChars++;
				continue;
			} else if (commentChars == 1) { 
				commentChars = 0;
				if (c == '*') {
					if (commentNest == 0) {
						inCommentType = 2;
					}
					commentNest++;
				}
				skipChars++;
				continue;
			} else if (inCommentType == 2)  {
				if ((commentChars == 0) && (c == '*')) {
					commentChars--;
				} else if (commentChars == -1) {
					commentChars = 0;
					if (c == '/') {
						commentNest--;
						if (commentNest <= 0) {
							inCommentType = 0;
							commentNest = 0;
						}
					}
				}
				skipChars++;
				continue;
			} else if (c == ' ' || c == '\t') {
				skipChars++;
				continue;
			} else {
				break;
			}
		} while (true);

		System.out.println("ExampleSelector: Decision character is : " + c);
		
		switch (c) {
		case '1':
			chosen = 0;
			break;
		case '2':
			chosen = 1;
			break;
		case '3':
			chosen = 2;
			break;
		case 'H' :
		case 'F' :
			chosen = 3;
			break;
		case 'C' :
			chosen = 4;
			break;
		default:
			chosen = -1;
			break;
		}
	}

	public void init(DataRecordMetadata[] arg0, Properties arg1)
			throws ComponentNotReadyException {
	}

	public int lookAheadCharacters() {
		return 1;
	}

	public int nextRecordOffset() {
		return skipChars;
	}

	public void reset() {
		skipChars = 0;
		chosen = -1;
	}

	public int nextRecordMetadataIndex() {
		return chosen;
	}

	public void recoverToNextRecord(CharBuffer data)
			throws BufferUnderflowException, BadDataFormatException {
		// TODO Auto-generated method stub
		
	}



}
