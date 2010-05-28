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
package org.jetel.util.ddl2clover;

public interface DDL2CloverConstants {

  int EOF = 0;
  int BIGINT = 13;
  int BINARY = 14;
  int BOOLEAN = 15;
  int BLOB = 16;
  int CHAR = 17;
  int CHARACTER = 18;
  int CLOB = 19;
  int COMMIT = 20;
  int CONSTRAINT = 21;
  int CREATE = 22;
  int DATE = 23;
  int DATETIME = 24;
  int DEC = 25;
  int DECIMAL = 26;
  int DEFAULT_ = 27;
  int DELETE = 28;
  int DOUBLE = 29;
  int FLOAT = 30;
  int FOREIGN = 31;
  int GLOBAL = 32;
  int INT = 33;
  int INTEGER = 34;
  int KEY = 35;
  int LOCAL = 36;
  int NOT = 37;
  int NULL = 38;
  int NUMBER = 39;
  int NUMERIC = 40;
  int ON = 41;
  int PRESERVE = 42;
  int PRIMARY = 43;
  int REFERENCES = 44;
  int REAL = 45;
  int ROWS = 46;
  int SMALLINT = 47;
  int STRING = 48;
  int TABLE = 49;
  int TEXT = 50;
  int TIME = 51;
  int TIMESTAMP = 52;
  int TEMPORARY = 53;
  int TINYINT = 54;
  int UNIQUE = 55;
  int VARBINARY = 56;
  int VARCHAR = 57;
  int VARCHAR2 = 58;
  int STRING_LITERAL = 59;
  int INTEGER_LITERAL = 60;
  int FLOAT_LITERAL = 61;
  int EXP = 62;
  int IDENTIFIER = 63;
  int LETTER = 64;
  int DIGIT = 65;
  int CLOSEPAREN = 66;
  int COMA = 67;
  int DOT = 68;
  int OPENPAREN = 69;
  int SEMICOLON = 70;
  int STRSTR = 71;
  int ALL = 72;

  int DEFAULT = 0;
  int WithinComment = 1;
  int WithinLineComment = 2;

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
    "\"//\"",
    "\"--\"",
    "\"\\n\"",
    "<token of kind 12>",
    "\"bigint\"",
    "\"binary\"",
    "\"boolean\"",
    "\"blob\"",
    "\"char\"",
    "\"character\"",
    "\"clob\"",
    "\"commit\"",
    "\"constraint\"",
    "\"create\"",
    "\"date\"",
    "\"datetime\"",
    "\"dec\"",
    "\"decimal\"",
    "\"default\"",
    "\"delete\"",
    "\"double\"",
    "\"float\"",
    "\"foreign\"",
    "\"global\"",
    "\"int\"",
    "\"integer\"",
    "\"key\"",
    "\"local\"",
    "\"not\"",
    "\"null\"",
    "\"number\"",
    "\"numeric\"",
    "\"on\"",
    "\"preserve\"",
    "\"primary\"",
    "\"references\"",
    "\"real\"",
    "\"rows\"",
    "\"smallint\"",
    "\"string\"",
    "\"table\"",
    "\"text\"",
    "\"time\"",
    "\"timestamp\"",
    "\"temporary\"",
    "\"tinyint\"",
    "\"unique\"",
    "\"varbinary\"",
    "\"varchar\"",
    "\"varchar2\"",
    "<STRING_LITERAL>",
    "<INTEGER_LITERAL>",
    "<FLOAT_LITERAL>",
    "<EXP>",
    "<IDENTIFIER>",
    "<LETTER>",
    "<DIGIT>",
    "\")\"",
    "\",\"",
    "\".\"",
    "\"(\"",
    "\";\"",
    "\"\\\"\"",
    "<ALL>",
  };

}
