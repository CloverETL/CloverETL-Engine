/* Generated By:JJTree&JavaCC: Do not edit this line. TransformLangParserConstants.java */
package org.jetel.interpreter;

public interface TransformLangParserConstants {

  int EOF = 0;
  int SINGLE_LINE_COMMENT = 9;
  int INTEGER_LITERAL = 10;
  int DIGIT = 11;
  int LETTER = 12;
  int UNDERSCORE = 13;
  int DECIMAL_LITERAL = 14;
  int HEX_LITERAL = 15;
  int OCTAL_LITERAL = 16;
  int FLOATING_POINT_LITERAL = 17;
  int EXPONENT = 18;
  int STRING_LITERAL = 19;
  int DQUOTED_STRING = 20;
  int UNTERMINATED_STRING_LITERAL = 21;
  int UNTERMINATED_DQUOTED_STRING = 22;
  int BOOLEAN_LITERAL = 23;
  int TRUE = 24;
  int FALSE = 25;
  int DATE_LITERAL = 26;
  int DATETIME_LITERAL = 27;
  int SEMICOLON = 28;
  int BLOCK_START = 29;
  int BLOCK_END = 30;
  int NULL_LITERAL = 31;
  int STRING_PLAIN_LITERAL = 32;
  int MAPPING = 33;
  int OR = 34;
  int AND = 35;
  int NOT = 36;
  int EQUAL = 37;
  int NON_EQUAL = 38;
  int IN_OPER = 39;
  int LESS_THAN = 40;
  int LESS_THAN_EQUAL = 41;
  int GREATER_THAN = 42;
  int GREATER_THAN_EQUAL = 43;
  int REGEX_EQUAL = 44;
  int CMPOPERATOR = 45;
  int MINUS = 46;
  int PLUS = 47;
  int MULTIPLY = 48;
  int DIVIDE = 49;
  int MODULO = 50;
  int INCR = 51;
  int DECR = 52;
  int TILDA = 53;
  int FIELD_ID = 54;
  int REC_NAME_FIELD_ID = 55;
  int REC_NAME_FIELD_NUM = 56;
  int REC_NUM_FIELD_ID = 57;
  int REC_NUM_FIELD_NUM = 58;
  int REC_NUM_ID = 59;
  int REC_NAME_ID = 60;
  int OPEN_PAR = 61;
  int CLOSE_PAR = 62;
  int INT_VAR = 63;
  int LONG_VAR = 64;
  int DATE_VAR = 65;
  int DOUBLE_VAR = 66;
  int DECIMAL_VAR = 67;
  int BOOLEAN_VAR = 68;
  int STRING_VAR = 69;
  int BYTE_VAR = 70;
  int LIST_VAR = 71;
  int MAP_VAR = 72;
  int RECORD_VAR = 73;
  int OBJECT_VAR = 74;
  int BREAK = 75;
  int CONTINUE = 76;
  int ELSE = 77;
  int FOR = 78;
  int FOR_EACH = 79;
  int FUNCTION = 80;
  int IF = 81;
  int RETURN = 82;
  int WHILE = 83;
  int CASE = 84;
  int ENUM = 85;
  int IMPORT = 86;
  int SWITCH = 87;
  int CASE_DEFAULT = 88;
  int DO = 89;
  int TRY = 90;
  int CATCH = 91;
  int YEAR = 92;
  int MONTH = 93;
  int WEEK = 94;
  int DAY = 95;
  int HOUR = 96;
  int MINUTE = 97;
  int SECOND = 98;
  int MILLISEC = 99;
  int IDENTIFIER = 100;
  int DATE_FIELD_LITERAL = 128;
  int ERROR = 129;

  int DEFAULT = 0;
  int WithinComment = 1;

  String[] tokenImage = {
    "<EOF>",
    "\" \"",
    "\"\\t\"",
    "\"\\n\"",
    "\"\\r\"",
    "\"\\n\\r\"",
    "\"/*\"",
    "\"*/\"",
    "<token of kind 8>",
    "<SINGLE_LINE_COMMENT>",
    "<INTEGER_LITERAL>",
    "<DIGIT>",
    "<LETTER>",
    "<UNDERSCORE>",
    "<DECIMAL_LITERAL>",
    "<HEX_LITERAL>",
    "<OCTAL_LITERAL>",
    "<FLOATING_POINT_LITERAL>",
    "<EXPONENT>",
    "<STRING_LITERAL>",
    "<DQUOTED_STRING>",
    "<UNTERMINATED_STRING_LITERAL>",
    "<UNTERMINATED_DQUOTED_STRING>",
    "<BOOLEAN_LITERAL>",
    "\"true\"",
    "\"false\"",
    "<DATE_LITERAL>",
    "<DATETIME_LITERAL>",
    "\";\"",
    "\"{\"",
    "\"}\"",
    "\"null\"",
    "\"\\\'\"",
    "\":=\"",
    "<OR>",
    "<AND>",
    "<NOT>",
    "<EQUAL>",
    "<NON_EQUAL>",
    "\".in.\"",
    "<LESS_THAN>",
    "<LESS_THAN_EQUAL>",
    "<GREATER_THAN>",
    "<GREATER_THAN_EQUAL>",
    "<REGEX_EQUAL>",
    "<CMPOPERATOR>",
    "\"-\"",
    "\"+\"",
    "\"*\"",
    "\"/\"",
    "\"%\"",
    "\"++\"",
    "\"--\"",
    "\"~\"",
    "<FIELD_ID>",
    "<REC_NAME_FIELD_ID>",
    "<REC_NAME_FIELD_NUM>",
    "<REC_NUM_FIELD_ID>",
    "<REC_NUM_FIELD_NUM>",
    "<REC_NUM_ID>",
    "<REC_NAME_ID>",
    "\"(\"",
    "\")\"",
    "\"int\"",
    "\"long\"",
    "\"date\"",
    "<DOUBLE_VAR>",
    "\"decimal\"",
    "\"boolean\"",
    "\"string\"",
    "\"bytearray\"",
    "\"list\"",
    "\"map\"",
    "\"record\"",
    "\"object\"",
    "\"break\"",
    "\"continue\"",
    "\"else\"",
    "\"for\"",
    "\"foreach\"",
    "\"function\"",
    "\"if\"",
    "\"return\"",
    "\"while\"",
    "\"case\"",
    "\"enum\"",
    "\"import\"",
    "\"switch\"",
    "\"default\"",
    "\"do\"",
    "\"try\"",
    "\"catch\"",
    "\"year\"",
    "\"month\"",
    "\"week\"",
    "\"day\"",
    "\"hour\"",
    "\"minute\"",
    "\"second\"",
    "\"millisec\"",
    "<IDENTIFIER>",
    "\",\"",
    "\"=\"",
    "\":\"",
    "\"[\"",
    "\"]\"",
    "\"isnull(\"",
    "\"nvl(\"",
    "\"nvl2(\"",
    "\"iif(\"",
    "\"print_stack(\"",
    "\"breakpoint(\"",
    "\"raise_error(\"",
    "\"print_err(\"",
    "\"eval(\"",
    "\"eval_exp(\"",
    "\"print_log(\"",
    "\"sequence(\"",
    "\".next\"",
    "\".current\"",
    "\".reset\"",
    "\"lookup(\"",
    "\".\"",
    "\"lookup_next(\"",
    "\"lookup_found(\"",
    "\"lookup_admin(\"",
    "\"init\"",
    "\"free\"",
    "<DATE_FIELD_LITERAL>",
    "<ERROR>",
  };

}
