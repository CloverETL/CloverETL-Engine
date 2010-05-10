//#TL
integer phone_number = -1;
string absoluteFilePath = "";

//parse input line to 12 fields
//for every line add phone_number
//phone_number == -1 for useless line
function integer transform(){
	string[] l;
	integer i;
//	integer row_id = 0;

	if ( $Name != absoluteFilePath ){
		phone_number = -1;
		absoluteFilePath = $Name;
	}
	
	if ( !isnull($0.Name) ){
		l = split($0.Name, ';');
	}
	

	for (i = length(l); i < 11; i++){
		l[i] = "";
	}
	
	if ( !isnull($0.Name) && $0.Name ~= '\b\d{9}\b' ){
		phone_number = str2int($0.Name);
	}
	
	return 0;
	
}