function long VipStr2Long(string sValue, string sIntermediair, string sElement, boolean bAllowNull) {
  if (!bAllowNull and isnull(sValue)) {
    raiseError("Data Type Error: " + sElement + " is empty for intermediary " + sIntermediair + ".");
  }
  else if (isnull(sValue)) {
    return null;
  }
  else {
    if (not isLong(sValue)) {
      raiseError("Data Type Error: Cannot convert element " + sElement + " to long for intermediary " + sIntermediair + ". Value is \"" + sValue + "\".");
    }
    return str2long(sValue);
  }
}

long value = VipStr2Long("123456", "", "", false);

function integer transform() {
	return 0;
}