string test;
boolean isBlank;
string blank; 
boolean isBlank1;
string nullValue;
boolean isBlank2;
boolean isAscii1;
boolean isAscii2;
boolean isAscii3;
boolean isAscii4;
boolean isNumber;
boolean isNumber1;
boolean isNumber2;
boolean isNumber3;
boolean isNumber4;
boolean isNumber5;
boolean isNumber6;
boolean isNumber7;
boolean isNumber8;
boolean isNumber9;
boolean isInteger;
boolean isInteger1;
boolean isInteger2;
boolean isInteger3;
boolean isInteger4;
boolean isInteger5;
boolean isInteger6;
boolean isLong;
boolean isLong1;
boolean isLong2;
boolean isLong3;
boolean isLong4;
boolean isLong5;
boolean isDate5;
boolean isDate6;
boolean isDate3;
boolean isDate;
boolean isDate1;
boolean isDate2;
boolean isDate4;
boolean isDate7;
boolean isDate8;
boolean isDate9;
boolean isDate10;
boolean isDate11; 
boolean isDate12;
boolean isDate13;
boolean isDate14;
boolean isDate15;
boolean isDate16;
boolean isDate17;
boolean isDate18;
boolean isDate19;
boolean isDate20;
boolean isDate21;
boolean isDate22;
boolean isDate23;
boolean isDate24;
boolean isDate25;
boolean isDate26;
boolean isDate27;
boolean isDate28;
boolean isDate29;
boolean isDate30;
boolean isDate31;
boolean isDate32;
boolean isDate33;
boolean isDate34;
boolean isDate35;
boolean isDate36;
boolean isDate37;
boolean isDate38;

boolean isDecimal;
boolean isDecimal1;
boolean isDecimal2;
boolean isDecimal3;
boolean isDecimal4;
boolean isDecimal5;
boolean isDecimal6;
boolean isDecimal7;
boolean isDecimal8;

function integer transform() {
	test='test';
	isBlank=isBlank(test);
	blank = ''; 
	isBlank1=isBlank(blank);
	nullValue=null;
	isBlank2=isBlank(nullValue);
	isAscii1=isAscii('test');
	isAscii2=isAscii('aęř');
	isAscii3=isAscii(nullValue);
	isAscii4=isAscii(blank);
	isNumber=isNumber('t1');
	isNumber1=isNumber('1g');
	isNumber2=isNumber('1');
	printErr(str2integer('1'));
	isNumber3=isNumber('-382.334');
	printErr(str2double('-382.334'));
	isNumber4=isNumber('+332e2');
	isNumber5=isNumber('8982.8992e-2');
	printErr(str2double('8982.8992e-2'));
	isNumber6=isNumber('-7888873.2E3');
	printErr(str2decimal('-7888873.2E3'));
	isNumber7=isNumber(nullValue);
	isNumber8=isNumber(blank);
	isNumber9=isNumber('-');
	isInteger=isInteger('h3');
	isInteger1=isInteger('78gd');
	isInteger2=isInteger('8982.8992');
	isInteger3=isInteger('-766542378');
	isInteger4=isInteger(nullValue);
	isInteger5=isInteger(blank);
	isInteger6=isInteger('-');
	printErr(str2integer('-766542378'));
	isLong=isLong('7864232568822234');
	isLong1=isLong('12345678901234567890');
	isLong2=isLong('LONG!');
	isLong3=isLong(nullValue);
	isLong4=isLong(blank);
	isLong5=isLong('-');
	isDate5=isDate('20Jul2000','ddMMMyyyy','en.US');
	printErr(str2date('20Jul2000','ddMMMyyyy','en.GB'));
	isDate6=isDate('20July     2000',"ddMMMM     yyyy",'en.US');
	printErr(str2date('20July    2000','ddMMM    yyyy','en.GB'));
	isDate3=isDate('4:42','HH:mm');
	printErr(str2date('4:42','HH:mm'));
	isDate=isDate('20.11.2007','dd.MM.yyyy');
	printErr(str2date('20.11.2007','dd.MM.yyyy'));
	isDate1=isDate('20.11.2007','dd-MM-yyyy');
	isDate2=isDate('24:00 20.11.2007','kk:mm dd.MM.yyyy');
	isDate4=isDate('test 20.11.2007','hhmm dd.MM.yyyy');
	isDate7=isDate('                ','HH:mm dd.MM.yyyy');
	isDate8=isDate('                ','HH:mm dd.MM.yyyy');
	isDate9=isDate('20-15-2007','dd-MM-yyyy');
	isDate10=isDate('20-15-2007','dd-MM-yyyy'); 
	isDate11=isDate('942-12-1996','dd-MM-yyyy','en.US');
	isDate12=isDate('12-prosinec-1996','dd-MMM-yyyy','cs.CZ');
	isDate13=isDate('12-prosinec-1996','dd-MMM-yyyy','en.US'); 
	isDate14=isDate('24:00 20.11.2007','HH:mm dd.MM.yyyy');
	isDate15=isDate(blank,'HH:mm dd.MM.yyyy');
	
	// switch to DST in USA from 2 to 3 AM, 2:30 does not exist in New York
	isDate16=isDate('10/03/2013 02:30', 'dd/MM/yyyy HH:mm', 'en.US', 'America/New_York');
	isDate17=isDate('10/03/2013 02:30', 'dd/MM/yyyy HH:mm', 'cs.CZ', 'Europe/Prague');
	// switch to DST in CR from 2 to 3 AM, 2:30 does not exist in Prague
	isDate18=isDate('31/03/2013 02:30', 'dd/MM/yyyy HH:mm', 'en.US', 'America/New_York');
	isDate19=isDate('31/03/2013 02:30', 'dd/MM/yyyy HH:mm', 'cs.CZ', 'Europe/Prague');
	
	isDate20=isDate(nullValue, 'dd/MM/yyyy HH:mm', 'cs.CZ', 'Europe/Prague');
	isDate21=isDate('', 'dd/MM/yyyy HH:mm', 'cs.CZ', 'Europe/Prague');
	
	// CLO-1190
	isDate22=isDate('2013-12-12','');
	isDate23=isDate('2013/12/11','');
	
	// test for using default pattern from Defaults - assumed pattern yyyy-MM-dd - if different change the input string
	isDate24=isDate('2013-12-12',null); // expected result - true
	isDate25=isDate('17/11/1990', null); // expected result - false
	isDate26 = isDate('2013-12-12', null, null, false);
	isDate27 = isDate('2013-12-12', null, null, null, false);
	
	// test for strict parsing: CLO-6601 - expected false
	isDate28 = isDate('1924011', 'yyyyMMdd', true);
	isDate29 = isDate('2015-001-012', null, true);
	isDate30 = isDate('2015-1-12', null, true);
	isDate31 = isDate('2015-November-12', 'yyyy-MMM-dd', 'en.US', true);
	isDate32 = isDate('2015-Nov-12', 'yyyy-MMMM-dd', 'en.US', true);
	isDate33 = isDate('2015-Nov-12', 'yyyy-MM-dd', 'en.US', true);
	isDate34 = isDate('2015-November-12', 'yyyy-MM-dd', 'en.US', true);
	
	// test for strict parsing: CLO-6601 - expected true
	isDate35 = isDate("2015-May-25", "yyyy-MMM-dd", 'en.US', true);
	isDate36 = isDate("2015-May-25", "yyyy-MMMM-dd", 'en.US', true);
	isDate37 = isDate("2015-Nov-25", "yyyy-MMM-dd", 'en.US', true);
	isDate38 = isDate("2015-November-25", "yyyy-MMMM-dd", 'en.US', true);

	isDecimal=isDecimal('t1');
	isDecimal1=isDecimal('1g');
	isDecimal2=isDecimal('1');
	printErr(str2integer('1'));
	isDecimal3=isDecimal('-382.334');
	printErr(str2double('-382.334'));
	isDecimal4=isDecimal('+332e2');
	isDecimal5=isDecimal('8982.8992e-2');
	printErr(str2double('8982.8992e-2'));
	isDecimal6=isDecimal('-7888873.2E3');
	printErr(str2decimal('-7888873.2E3'));
	isDecimal7=isDecimal(nullValue);
	isDecimal8=isDecimal(blank);

	return 0;
}