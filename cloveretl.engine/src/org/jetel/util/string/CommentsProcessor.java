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
package org.jetel.util.string;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class implements simple algorithm, which removes all java-like comments from given string.
 * Both types of comments are removed - one line (//) and multi-line (\/* *\/)
 * 
 * Usage: CommentsProcessor.stripComments(String transformationCode)
 *
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.6.2010
 */
public class CommentsProcessor {

	private static final String SINGLE_LINE_COMMENT = "//"; //$NON-NLS-1$
	
	private static final String MULTI_LINE_START_COMMENT = "/*"; //$NON-NLS-1$
	private static final String MULTI_LINE_END_COMMENT = "*/"; //$NON-NLS-1$
	
	private static final String EOL_REGEXP = "(\\r)|(\\n)"; //$NON-NLS-1$
	
	private enum CommentType {
		SINGLE_LINE, MULTI_LINE;
	}
	
	public static String stripComments(String transform) {
		return (new CommentsProcessor(transform)).stripComments();
	}
	
	private StringBuilder result;
	private String transform;
	private int index;
	private Matcher eolMatcher;
	private CommentsProcessor(String transform) {
		this.result = new StringBuilder(transform.length());
		this.transform = transform;
		this.index = 0;
		Pattern p = Pattern.compile(EOL_REGEXP);
		eolMatcher = p.matcher(transform);
	}
	
	private String stripComments() {
		CommentType commentType;
		
		while ((commentType = nextComment()) != null) {
			switch (commentType) {
			case SINGLE_LINE:
				if (eolMatcher.find(index)) {
					incrementIndexAndForget(eolMatcher.start());
				} else {
					incrementIndexAndForget(transform.length());
				}
				break;
			case MULTI_LINE:
				int newIndex = transform.indexOf(MULTI_LINE_END_COMMENT, index);
				if (newIndex != -1) {
					incrementIndexAndForget(newIndex + MULTI_LINE_END_COMMENT.length());
				} else {
					incrementIndexAndForget(transform.length());
				}
				break;
			default:
				throw new IllegalStateException("Unexpected type of comment.");
			}
		}
		
		return result.toString();
	}
	
	private CommentType nextComment() {
		int singleLineComment = getCommentStart(SINGLE_LINE_COMMENT);
		int multiLineComment = getCommentStart(MULTI_LINE_START_COMMENT);

		if (singleLineComment == -1 && multiLineComment == -1) {
			incrementIndexAndRemember(transform.length());
			return null;
		}

		if (singleLineComment == -1) {
			incrementIndexAndRemember(multiLineComment);
			return CommentType.MULTI_LINE;
		}
		if (multiLineComment == -1) {
			incrementIndexAndRemember(singleLineComment);
			return CommentType.SINGLE_LINE;
		}
		if (singleLineComment > multiLineComment) {
			incrementIndexAndRemember(multiLineComment);
			return CommentType.MULTI_LINE;
		} else {
			incrementIndexAndRemember(singleLineComment);
			return CommentType.SINGLE_LINE;
		}
	}
	
	private void incrementIndexAndRemember(int newIndex) {
		result.append(transform.substring(index, newIndex));
		index = newIndex;	
	}

	private void incrementIndexAndForget(int newIndex) {
		index = newIndex;	
	}

	private int getCommentStart(String commentPattern) {
		int tmpIndex;
		int commentStart = transform.indexOf(commentPattern, index);
		
        while (commentStart > 0 && endsInsideString(transform.substring(index, commentStart))) {
            tmpIndex = commentStart + commentPattern.length();
            commentStart = transform.indexOf(commentPattern, tmpIndex);
        }
		return commentStart;
	}
	
	private boolean endsInsideString(String code) {
		if (!code.contains("\"")) { //$NON-NLS-1$
			return false;
		}
        // remove all \", but first remove all \\ for cases like \\"
		code = code.replace("\\\\", "").replace("\\\"", "");   //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		int count = 0;
		int start = -1;
		while ((start=code.indexOf("\"", start+1)) >= 0) { //$NON-NLS-1$
			count++;
		}
		return count % 2 != 0;
	}
}
