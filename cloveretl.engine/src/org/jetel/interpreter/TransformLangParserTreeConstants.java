package org.jetel.interpreter;

public interface TransformLangParserTreeConstants
{
  public int JJTSTART = 0;
  public int JJTSTARTEXPRESSION = 1;
  public int JJTVOID = 2;
  public int JJTIMPORTSOURCE = 3;
  public int JJTFUNCTIONDECLARATION = 4;
  public int JJTVARDECLARATION = 5;
  public int JJTASSIGNMENT = 6;
  public int JJTMAPPING = 7;
  public int JJTOR = 8;
  public int JJTAND = 9;
  public int JJTCOMPARISON = 10;
  public int JJTADDNODE = 11;
  public int JJTSUBNODE = 12;
  public int JJTMULNODE = 13;
  public int JJTDIVNODE = 14;
  public int JJTMODNODE = 15;
  public int JJTOPERATOR = 16;
  public int JJTPOSTFIXEXPRESSION = 17;
  public int JJTUNARYEXPRESSION = 18;
  public int JJTLITERAL = 19;
  public int JJTINPUTFIELDLITERAL = 20;
  public int JJTVARIABLELITERAL = 21;
  public int JJTREGEXLITERAL = 22;
  public int JJTSYMBOLNAMEEXP = 23;
  public int JJTBLOCK = 24;
  public int JJTINCRDECRSTATEMENT = 25;
  public int JJTIFSTATEMENT = 26;
  public int JJTSWITCHSTATEMENT = 27;
  public int JJTCASEEXPRESSION = 28;
  public int JJTWHILESTATEMENT = 29;
  public int JJTFORSTATEMENT = 30;
  public int JJTFOREACHSTATEMENT = 31;
  public int JJTDOSTATEMENT = 32;
  public int JJTBREAKSTATEMENT = 33;
  public int JJTCONTINUESTATEMENT = 34;
  public int JJTRETURNSTATEMENT = 35;
  public int JJTFUNCTIONCALLSTATEMENT = 36;
  public int JJTISNULLNODE = 37;
  public int JJTNVLNODE = 38;
  public int JJTIFFNODE = 39;
  public int JJTPRINTSTACKNODE = 40;
  public int JJTBREAKPOINTNODE = 41;
  public int JJTRAISEERRORNODE = 42;
  public int JJTPRINTERRNODE = 43;
  public int JJTPRINTLOGNODE = 44;
  public int JJTSEQUENCENODE = 45;
  public int JJTLOOKUPNODE = 46;


  public String[] jjtNodeName = {
    "Start",
    "StartExpression",
    "void",
    "ImportSource",
    "FunctionDeclaration",
    "VarDeclaration",
    "Assignment",
    "Mapping",
    "Or",
    "And",
    "Comparison",
    "AddNode",
    "SubNode",
    "MulNode",
    "DivNode",
    "ModNode",
    "Operator",
    "PostfixExpression",
    "UnaryExpression",
    "Literal",
    "InputFieldLiteral",
    "VariableLiteral",
    "RegexLiteral",
    "SymbolNameExp",
    "Block",
    "IncrDecrStatement",
    "IfStatement",
    "SwitchStatement",
    "CaseExpression",
    "WhileStatement",
    "ForStatement",
    "ForeachStatement",
    "DoStatement",
    "BreakStatement",
    "ContinueStatement",
    "ReturnStatement",
    "FunctionCallStatement",
    "IsNullNode",
    "NVLNode",
    "IffNode",
    "PrintStackNode",
    "BreakpointNode",
    "RaiseErrorNode",
    "PrintErrNode",
    "PrintLogNode",
    "SequenceNode",
    "LookupNode",
  };
}
