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
package org.jetel.ctl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.ctl.ASTnode.SimpleNode;

/**
 * Utility class for easy extraction of specific pieces of code from CTL code.
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 5. 2014
 */
public class CodePieceExtractor {
	
	private static final CodePieceExtractor instance = new CodePieceExtractor();
	
	public static CodePieceExtractor getInstance() {
		return instance;
	}

	/**
	 * Extracts pieces of code from given CTL code using the provided PositionedCodeVisitorFactory.
	 * 
	 * <p>The factory should return PositionedCodeVisitor instances. These instances can then use
	 * the {{@link PositionedCodeVisitor#getCodePiece(SimpleNode)} method to obtain specific code pieces.
	 * 
	 * @param compiler
	 * @param ctlCode CTL code to analyze.
	 * @param visitorFactory Factory that creates PositionedCodeVisitor instances.
	 * @return
	 */
	public <T extends PositionedCodeVisitor> T extract(TLCompiler compiler, String ctlCode, PositionedCodeVisitorFactory<T> visitorFactory) {
		compiler.setTabSize(1);
		compiler.validate(ctlCode);
		int[] lineStartPositions = PositionedString.getLineStartPositions(ctlCode);
		SyntacticPositionedString positionedSourceCode = new SyntacticPositionedString(ctlCode, lineStartPositions);

		T visitor = visitorFactory.getPositionedCodeVisitor(positionedSourceCode);
		compiler.getStart().jjtAccept(visitor, null);
		return visitor;
	}
	
	public static interface PositionedCodeVisitorFactory<T extends PositionedCodeVisitor> {
		
		public abstract T getPositionedCodeVisitor(SyntacticPositionedString positionedSourceCode);
		
	}
	
	public static abstract class PositionedCodeVisitor extends NavigatingVisitor {
		private final SyntacticPositionedString positionedSourceCode;
	
		public PositionedCodeVisitor(SyntacticPositionedString positionedSourceCode) {
			this.positionedSourceCode = positionedSourceCode;
		}
		
		/**
		 * Extracts the piece of code represented by this node.
		 * 
		 * @param node
		 * @return
		 */
		public String getCodePiece(SimpleNode node) {
			return positionedSourceCode.getExpression(node);
		}
	}
	
	/**
	 * Returns pieces of input string indexed by start line, start column, end line, end column.
	 * 
	 * @author Raszyk (info@cloveretl.com)
	 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 31.7.2013
	 */
	public static class PositionedString {
		protected final String input;
		protected final int[] lineStartPositions;
		private static final Pattern newlineRegex = Pattern.compile("\r\n|\n\r|\n|\r", Pattern.MULTILINE);
		
		public PositionedString(String input, int[] lineStartPositions) {
			this.input = input;
			this.lineStartPositions = lineStartPositions;
		}
		
		public int getIndex(int line, int column) {
			return lineStartPositions[line - 1] + column - 1;
		}
		
		/**
		 * Lines and columns are numbered starting from 1. End character is included in the output.
		 * 
		 * @param startLine
		 * @param startColumn
		 * @param endLine
		 * @param endColumn
		 * @return
		 */
		public String getCodePiece(int startLine, int startColumn, int endLine, int endColumn) {
			int startIndex = getIndex(startLine, startColumn);
			int endIndex = getIndex(endLine, endColumn);
			return input.substring(startIndex, endIndex + 1);
		}
		
		public static int[] getLineStartPositions(String input) {
			List<Integer> positions = new ArrayList<Integer>();
			positions.add(0); // first line start index
			Matcher newlineMatcher = newlineRegex.matcher(input);
			while (newlineMatcher.find()) {
				positions.add(newlineMatcher.end());
			}
			return integerListToArray(positions);
		}
	
		private static int[] integerListToArray(List<Integer> arrayList) {
			int[] array = new int[arrayList.size()];
			int idx = 0;
			for (Integer pos : arrayList) {
				array[idx++] = pos;
			}
			return array;
		}
	}
	
	/**
	 * Enhanced PositionedString that can use SyntacticPosition to index the input string.
	 * 
	 * @author Raszyk (info@cloveretl.com)
	 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 31.7.2013
	 */
	public static class SyntacticPositionedString extends PositionedString {
		public SyntacticPositionedString(String input, int[] lineStartPositions) {
			super(input, lineStartPositions);
		}
	
		public String getCodePiece(SyntacticPosition start, SyntacticPosition end) {
			return getCodePiece(start.getLine(), start.getColumn(), end.getLine(), end.getColumn());
		}
		
		public String getExpression(SimpleNode node) {
			return getCodePiece(node.getBegin(), node.getEnd());
		}
	}

}

