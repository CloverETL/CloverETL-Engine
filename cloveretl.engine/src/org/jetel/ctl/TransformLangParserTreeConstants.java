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

public interface TransformLangParserTreeConstants
{
  public int JJTSTART = 0;
  public int JJTSTARTEXPRESSION = 1;
  public int JJTVOID = 2;
  public int JJTIMPORTSOURCE = 3;
  public int JJTPARAMETERS = 4;
  public int JJTTYPE = 5;
  public int JJTFUNCTIONDECLARATION = 6;
  public int JJTVARIABLEDECLARATION = 7;
  public int JJTASSIGNMENT = 8;
  public int JJTCONDITIONALEXPRESSION = 9;
  public int JJTOR = 10;
  public int JJTAND = 11;
  public int JJTCOMPARISON = 12;
  public int JJTADDNODE = 13;
  public int JJTSUBNODE = 14;
  public int JJTMULNODE = 15;
  public int JJTDIVNODE = 16;
  public int JJTMODNODE = 17;
  public int JJTUNARYEXPRESSION = 18;
  public int JJTPOSTFIXEXPRESSION = 19;
  public int JJTMEMBERACCESSEXPRESSION = 20;
  public int JJTARRAYACCESSEXPRESSION = 21;
  public int JJTINFUNCTION = 22;
  public int JJTFUNCTIONCALL = 23;
  public int JJTISNULLNODE = 24;
  public int JJTNVLNODE = 25;
  public int JJTNVL2NODE = 26;
  public int JJTIIFNODE = 27;
  public int JJTPRINTERRNODE = 28;
  public int JJTPRINTLOGNODE = 29;
  public int JJTPRINTSTACKNODE = 30;
  public int JJTRAISEERRORNODE = 31;
  public int JJTREADDICTNODE = 32;
  public int JJTWRITEDICTNODE = 33;
  public int JJTDELETEDICTNODE = 34;
  public int JJTFIELDACCESSEXPRESSION = 35;
  public int JJTIDENTIFIER = 36;
  public int JJTARGUMENTS = 37;
  public int JJTDATEFIELD = 38;
  public int JJTLOGLEVEL = 39;
  public int JJTLITERAL = 40;
  public int JJTLISTOFLITERALS = 41;
  public int JJTBLOCK = 42;
  public int JJTIFSTATEMENT = 43;
  public int JJTSWITCHSTATEMENT = 44;
  public int JJTCASESTATEMENT = 45;
  public int JJTWHILESTATEMENT = 46;
  public int JJTFORSTATEMENT = 47;
  public int JJTFOREACHSTATEMENT = 48;
  public int JJTDOSTATEMENT = 49;
  public int JJTBREAKSTATEMENT = 50;
  public int JJTCONTINUESTATEMENT = 51;
  public int JJTRETURNSTATEMENT = 52;
  public int JJTSEQUENCENODE = 53;
  public int JJTLOOKUPNODE = 54;


  public String[] jjtNodeName = {
    "Start",
    "StartExpression",
    "void",
    "ImportSource",
    "Parameters",
    "Type",
    "FunctionDeclaration",
    "VariableDeclaration",
    "Assignment",
    "ConditionalExpression",
    "Or",
    "And",
    "Comparison",
    "AddNode",
    "SubNode",
    "MulNode",
    "DivNode",
    "ModNode",
    "UnaryExpression",
    "PostfixExpression",
    "MemberAccessExpression",
    "ArrayAccessExpression",
    "InFunction",
    "FunctionCall",
    "IsNullNode",
    "NVLNode",
    "NVL2Node",
    "IIfNode",
    "PrintErrNode",
    "PrintLogNode",
    "PrintStackNode",
    "RaiseErrorNode",
    "ReadDictNode",
    "WriteDictNode",
    "DeleteDictNode",
    "FieldAccessExpression",
    "Identifier",
    "Arguments",
    "DateField",
    "LogLevel",
    "Literal",
    "ListOfLiterals",
    "Block",
    "IfStatement",
    "SwitchStatement",
    "CaseStatement",
    "WhileStatement",
    "ForStatement",
    "ForeachStatement",
    "DoStatement",
    "BreakStatement",
    "ContinueStatement",
    "ReturnStatement",
    "SequenceNode",
    "LookupNode",
  };
}
/* JavaCC - OriginalChecksum=6675c06794f65d97d22be46a151c5ffd (do not edit this line) */
