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

import org.jetel.ctl.ASTnode.CLVFBlock;
import org.jetel.ctl.ASTnode.CLVFBreakStatement;
import org.jetel.ctl.ASTnode.CLVFContinueStatement;
import org.jetel.ctl.ASTnode.CLVFDoStatement;
import org.jetel.ctl.ASTnode.CLVFForStatement;
import org.jetel.ctl.ASTnode.CLVFForeachStatement;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFIfStatement;
import org.jetel.ctl.ASTnode.CLVFImportSource;
import org.jetel.ctl.ASTnode.CLVFRaiseErrorNode;
import org.jetel.ctl.ASTnode.CLVFReturnStatement;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.CLVFSwitchStatement;
import org.jetel.ctl.ASTnode.CLVFWhileStatement;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.data.TLType;

/**
 * Control flow checking implementation. The AST pass checks for
 * correct use of break, continue and return statements, reporting 
 * and possible unreachable block.
 * 
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class FlowControl {

	private ProblemReporter problemReporter;

	private class ReturnChecker extends NavigatingVisitor {

		private TLType returnType;

		public void checkFunction(CLVFStart node) {
			node.jjtAccept(this, null);
		}

		/*
		 * All other nodes except the ones below do NOT return
		 */
		@Override
		protected Object visitNode(SimpleNode node, Object data) {
			return false;
		}

		/*
		 * We have to iterate over CVLFStart children since visitNode() 
		 * method is redefined to return false automatically for all nodes.
		 * See above.
		 */
		@Override
		public Boolean visit(CLVFStart node, Object data) {
			boolean isOK = true;
			for (int i = 0; i < node.jjtGetNumChildren(); i++) {
				SimpleNode child = (SimpleNode) node.jjtGetChild(i);
				switch (child.getId()) {
				case TransformLangParserTreeConstants.JJTIMPORTSOURCE:
					isOK &= (Boolean) child.jjtAccept(this, data);
					break;
				case TransformLangParserTreeConstants.JJTRETURNSTATEMENT:
					error(child, "Misplaced return statement");
					break;
				case TransformLangParserTreeConstants.JJTFUNCTIONDECLARATION:
					// we need to return-check only functions that have a return type
					if (child.getType() != TLType.VOID) {
						boolean ret = (Boolean) child.jjtAccept(this, data);
						isOK = isOK && ret;
					}
					break;
				}
			}

			// return value is not really used but for consistency reasons we keep it as boolean
			return isOK;
		}

		@Override
		public Object visit(CLVFImportSource node, Object data) {
			// store current "import context" so we can restore it after parsing this import
			String importFileUrl = problemReporter.getImportFileUrl();
			ErrorLocation errorLocation = problemReporter.getErrorLocation();

	        // set new "import context", propagate error location if already defined
			problemReporter.setImportFileUrl(node.getSourceToImport());
			problemReporter.setErrorLocation((errorLocation != null)
					? errorLocation : new ErrorLocation(node.getBegin(), node.getEnd()));

			Object result;
			if (node.jjtHasChildren() && node.jjtGetChild(0) != null) {
				result = node.jjtGetChild(0).jjtAccept(this, data);
			} else {
				result = true; // this was duplicate import which was ignored 
			}

			// restore current "import context"
			problemReporter.setImportFileUrl(importFileUrl);
			problemReporter.setErrorLocation(errorLocation);

			return result;
		}

		@Override
		public Boolean visit(CLVFFunctionDeclaration node, Object data) {
			this.returnType = node.getType();
			CLVFBlock body = (CLVFBlock) node.jjtGetChild(2);
			if ((Boolean) body.jjtAccept(this, data)) {
				return true;
			}

			// none of statements returns ... report error
			error(((SimpleNode) node.jjtGetChild(0)).getBegin(), ((SimpleNode) node.jjtGetChild(1)).getEnd(), "Missing 'return' statement (" + returnType.name() + ")");
			return false;
		}

		/**
		 * returns(s1,s2) <=> returns(s1) || returns(s2)
		 * 
		 * Block returns if any of its statements returns. We do not visit loops so we can check for misplaced
		 * break/continue
		 */
		@Override
		public Boolean visit(CLVFBlock node, Object data) {
			for (int i = 0; i < node.jjtGetNumChildren(); i++) {
				SimpleNode child = (SimpleNode) node.jjtGetChild(i);
				switch (child.getId()) {
				case TransformLangParserTreeConstants.JJTFOREACHSTATEMENT:
				case TransformLangParserTreeConstants.JJTFORSTATEMENT:
				case TransformLangParserTreeConstants.JJTWHILESTATEMENT:
				case TransformLangParserTreeConstants.JJTDOSTATEMENT:
					// we do not visit loop bodies since they might not get executed even once
					// thus they don't return by default
					continue;
				default:
					if ((Boolean) child.jjtAccept(this, data)) {
						if (i < node.jjtGetNumChildren() - 1) {
							error((SimpleNode) node.jjtGetChild(i + 1), "Unreachable code");
						}
						return true;
					}
					break;
				}
			}

			return false;
		}

		/**
		 * return(switch(case1,case2,...)) <=> return(case1) && return(case2) && ... Switch returns if it has default
		 * branch and all of its statements return.
		 */
		// @Override
		// public Boolean visit(CLVFSwitchStatement node, Object data) {
		// if (!node.hasDefaultClause()) {
		// return false;
		// }
		//			
		// // skip switch expression: start from first case
		// for (int i=1; i<node.jjtGetNumChildren(); i++) {
		// SimpleNode caseStmt = (SimpleNode)node.jjtGetChild(i);
		// // return on first non-returning case
		// if (! (Boolean)caseStmt.jjtAccept(this, data)) {
		// return false;
		// }
		// }
		// return true;
		// }

		@Override
		public Boolean visit(CLVFSwitchStatement node, Object data) {
			/*
			 * Switch without default clause does NOT return because none of the case-branches might get executed
			 * This also covers switch with no case-labels
			 */
			if (!node.hasDefaultClause()) {
				return false;
			}

			/*
			 * Switch-body representation |c1|s2|s3|c4|c5|...|d6|s7|
			 * 
			 * Algorithm: 
			 * STEP1. Scan for next case-label. If case found GOTO(STEP2). 
			 * If anything else found GOTO(ERROR) STEP2. 
			 * 	If label found, start scanning further until a statement like below is found:
			 * 	 a, break/continue =>
			 * 		relevant case-label does NOT return => END(switch does NOT return) 
			 *	 b, statement that returns => 
			 *		case statement returns => GOTO(STEP1) 
			 *	 c, ignore any case-labels you encounter since they have no effect on
			 * 		returns ERROR. 
			 * 		(We found a statement that is preceded by a returning statement, 
			 * 		break or continue). Tag* the statement as "Unreachable code". 
			 * 		GOTO(STEP1)
			 * 
			 */
			int idx = 1; // skip switch expression
			boolean switchReturns = false; // presume switch does not return
			while (idx < node.jjtGetNumChildren()) {
				// scan until next 'case' statement is found
				while (idx < node.jjtGetNumChildren()) {
					SimpleNode stmt = (SimpleNode) node.jjtGetChild(idx);
					if (stmt.getId() == TransformLangParserTreeConstants.JJTCASESTATEMENT) {
						// found (another) case-label that might not return
						switchReturns = false; 
						break;
					} else {
						error(stmt, "Unreachable code");
					}
					idx++;
				}
				// idx now is either out of range or on case statement
				if (idx >= node.jjtGetNumChildren()) {
					break;
				}

				// scan to the right until the next break/continue on return(s) == true
				while (idx < node.jjtGetNumChildren() && !switchReturns) {
					SimpleNode stmt = (SimpleNode) node.jjtGetChild(idx);
					switch (stmt.getId()) {
					case TransformLangParserTreeConstants.JJTCASESTATEMENT:
						// ignore any case statements we might encounter
						break;
					case TransformLangParserTreeConstants.JJTBREAKSTATEMENT:
					case TransformLangParserTreeConstants.JJTCONTINUESTATEMENT:
						// this case does NOT return => whole switch does not return
						return false;
					default:
						switchReturns = (Boolean) node.jjtGetChild(idx).jjtAccept(this, null);
						break;
					}
					idx++;
				}
			}

			/*
			 * We are done because:
			 * 1. We processed all statements and they all return => switch returns
			 * 2. We processed all statements but there was a case-label that does not return => switch does NOT returns
			 */
			node.setTerminal(switchReturns);
			return switchReturns;
		}

		/**
		 * returns(if) <=> returns(then) && return (else)
		 * 
		 * If statement without else does not return since then-branch may not be executed.
		 */
		@Override
		public Boolean visit(CLVFIfStatement node, Object data) {
			if (node.jjtGetNumChildren() < 3) {
				return false;
			} else {
				SimpleNode thenBranch = (SimpleNode) node.jjtGetChild(1);
				boolean ret = (Boolean) thenBranch.jjtAccept(this, data);
				SimpleNode elseBranch = (SimpleNode) node.jjtGetChild(2);
				return ret && (Boolean) elseBranch.jjtAccept(this, data);
			}
		}

		@Override
		public Boolean visit(CLVFReturnStatement node, Object data) {
			return true;
		}
		
		@Override
		public Boolean visit(CLVFRaiseErrorNode node, Object data) {
			return true;
		}

	}

	private class BreakContinueChecker extends NavigatingVisitor {

		private boolean breakAllowed;
		private boolean continueAllowed;

		public void checkCode(CLVFStart node) {
			this.breakAllowed = false;
			this.continueAllowed = false;
			node.jjtAccept(this, null);
		}

		@Override
		public Object visit(CLVFImportSource node, Object data) {
			// store current "import context" so we can restore it after parsing this import
			String importFileUrl = problemReporter.getImportFileUrl();
			ErrorLocation errorLocation = problemReporter.getErrorLocation();

	        // set new "import context", propagate error location if already defined
			problemReporter.setImportFileUrl(node.getSourceToImport());
			problemReporter.setErrorLocation((errorLocation != null)
					? errorLocation : new ErrorLocation(node.getBegin(), node.getEnd()));

			Object result = super.visit(node, data);

			// restore current "import context"
			problemReporter.setImportFileUrl(importFileUrl);
			problemReporter.setErrorLocation(errorLocation);

			return result;
		}

		@Override
		public Object visit(CLVFForStatement node, Object data) {
			checkLoop(node.jjtGetChild(node.jjtGetNumChildren() - 1));
			return data;
		}

		@Override
		public Object visit(CLVFForeachStatement node, Object data) {
			checkLoop(node.jjtGetChild(1));
			return data;
		}

		@Override
		public Object visit(CLVFDoStatement node, Object data) {
			checkLoop(node.jjtGetChild(0));
			return data;
		}

		@Override
		public Object visit(CLVFWhileStatement node, Object data) {
			checkLoop(node.jjtGetChild(1));
			return data;
		}

		@Override
		public Object visit(CLVFSwitchStatement node, Object data) {
			boolean breakOld = this.breakAllowed;
			this.breakAllowed = true;
			// inherit continue allowance from the upper level
			
			// visit body
			for (int i=0; i<node.jjtGetNumChildren(); i++) {
				node.jjtGetChild(i).jjtAccept(this, null);
			}

			this.breakAllowed = breakOld;

			return data;
		}

		@Override
		public Object visit(CLVFBreakStatement node, Object data) {
			if (!breakAllowed) {
				error(node, "Break statement cannot be used outside of a loop");
				return data;
			}

			SimpleNode parent = (SimpleNode)node.jjtGetParent();
			int breakIdx = parent.indexOf(node);
			if (breakIdx < parent.jjtGetNumChildren() - 1) {
				SimpleNode unrchCode = (SimpleNode)parent.jjtGetChild(breakIdx+1);
				// we do not report errors for case label after break within switch body
				if (unrchCode.getId() != TransformLangParserTreeConstants.JJTCASESTATEMENT) {
					error(unrchCode, "Unreachable code");
				}
			}
			
			return data;

		}

		@Override
		public Object visit(CLVFContinueStatement node, Object data) {
			if (!continueAllowed) {
				error(node, "Continue statement cannot be used outside of a loop");
				return data;
			}

			Node parent = node.jjtGetParent();
			for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
				if (parent.jjtGetChild(i) == node) {
					if (i < parent.jjtGetNumChildren() - 1) {
						SimpleNode nextChild = (SimpleNode) parent.jjtGetChild(i + 1);
						// we do not report errors for case label after break within switch body
						if (nextChild.getId() != TransformLangParserTreeConstants.JJTCASESTATEMENT) {
							error(nextChild, "Unreachable code");
						}
					}
				}
			}

			return data;

		}

		private void checkLoop(Node body) {
			boolean breakOld = this.breakAllowed;
			boolean continueOld = this.continueAllowed;
			this.breakAllowed = true;
			this.continueAllowed = true;

			// visit body
			body.jjtAccept(this, null);

			this.breakAllowed = breakOld;
			this.continueAllowed = continueOld;

		}
	}

	public FlowControl(ProblemReporter problemReporter) {
		this.problemReporter = problemReporter;
	}

	public void check(CLVFStart ast) {
		ReturnChecker returnCheck = new ReturnChecker();
		returnCheck.checkFunction(ast);
		BreakContinueChecker breakCheck = new BreakContinueChecker();
		breakCheck.checkCode(ast);
	}

	// ----------------- Error Reporting --------------------------

	private void error(SyntacticPosition begin, SyntacticPosition end, String error) {
		problemReporter.error(begin, end, error, null);
	}

	private void error(SimpleNode node, String error) {
		problemReporter.error(node.getBegin(), node.getEnd(), error, null);
	}

	@SuppressWarnings("unused")
	private void error(SimpleNode node, String error, String hint) {
		problemReporter.error(node.getBegin(), node.getEnd(), error, hint);
	}

	@SuppressWarnings("unused")
	private void warn(SimpleNode node, String error) {
		problemReporter.warn(node.getBegin(), node.getEnd(), error, null);
	}

	@SuppressWarnings("unused")
	private void warn(SimpleNode node, String error, String hint) {
		problemReporter.warn(node.getBegin(), node.getEnd(), error, hint);
	}

}
