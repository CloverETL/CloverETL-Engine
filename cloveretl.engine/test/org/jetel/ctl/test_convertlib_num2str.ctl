string[] intOutput;
string[] longOutput;
string[] doubleOutput;
string[] decimalOutput;
string[] nullIntRet;
string[] nullLongRet;
string[] nullDoubleRet;
string[] nullDecRet;

string test;
function integer transform() {

	// integer conversions
	intOutput[0] = num2str(16);
	intOutput[1] = num2str(16,2);
	intOutput[2] = num2str(16,8);
	intOutput[3] = num2str(16,16);
	intOutput[4] = num2str(1235,'0.###E0');
	intOutput[5] = num2str(12350001,'###,###.# Kcs', 'cs.CZ');
	printErr("integer output: " + intOutput);

	integer int = null;
	string s = null;
	nullIntRet[0] = num2str(int);
	nullIntRet[1] = num2str(int,2);
	nullIntRet[2] = num2str(int, '###,##');
	nullIntRet[3] = num2str(int, '###,###.# Kcs', 'cs.CZ');
	nullIntRet[4] = num2str(12,s);
	nullIntRet[5] = num2str(12,s,s);
	nullIntRet[6] = num2str(int,s);
	nullIntRet[7] = num2str(int,s,s);
	
	// long conversions
	longOutput[0] = num2str(16);
	longOutput[1] = num2str(16,2);
	longOutput[2] = num2str(16,8);
	longOutput[3] = num2str(16,16);
	longOutput[4] = num2str(12352387956654L,'0.###E0');
	longOutput[5] = num2str(12350001L,'###,###.# Kcs', 'cs.CZ');
	printErr("long output: " + longOutput);

	long l_var = null;
	nullLongRet[0] = num2str(l_var);
	nullLongRet[1] = num2str(l_var,2);
	nullLongRet[2] = num2str(l_var, '###.##');
	nullLongRet[3] = num2str(l_var, '###.##', 'cs.CZ');
	nullLongRet[4] = num2str(12L,s);
	nullLongRet[5] = num2str(12l,s,s);
	nullLongRet[6] = num2str(l_var,s);
	nullLongRet[7] = num2str(l_var,s,s);

	// double conversions
	doubleOutput[0] = num2str(16.16);
	doubleOutput[1] = num2str(16.16,16);
	doubleOutput[2] = num2str(1235.48,'###.###E0');
	doubleOutput[3] = num2str(12350001.1,'###,###.# Kcs', 'cs.CZ');
	printErr("double output: " + doubleOutput);
	
	double dou = null;
	nullDoubleRet[0] = num2str(dou);
	nullDoubleRet[1] = num2str(dou,2);
    nullDoubleRet[2] = num2str(dou, '###,#');
    nullDoubleRet[3] = num2str(dou, '##,#', 'cs.CZ');
    nullDoubleRet[4] = num2str(12.2,s);
    nullDoubleRet[5] = num2str(12.2,s,s);
    nullDoubleRet[6] = num2str(dou,s);
    nullDoubleRet[7] = num2str(dou,s,s);
    
	// decimal conversions
	decimalOutput[0] = num2str(16.16D);
	decimalOutput[1] = num2str(1235.44D, '###.###');
	decimalOutput[2] = num2str(12350001.1d, '###,###.# Kcs', 'cs.CZ');
	printErr("decimal output: " + decimalOutput);
	
	decimal dec = null;
	nullDecRet[0] = num2str(dec);
	nullDecRet[1] = num2str(dec, '##,#');
	nullDecRet[2] = num2str(dec, '##.#', 'cs.CZ');
	nullDecRet[3] = num2str(12.2d,s);
	nullDecRet[4] = num2str(12.2d,s,s);
	nullDecRet[5] = num2str(dec,s);
	nullDecRet[6] = num2str(dec,s,s);
	
	return 0;
}