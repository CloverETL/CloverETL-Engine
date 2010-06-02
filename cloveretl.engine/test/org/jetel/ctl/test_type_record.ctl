firstInput copy;
firstInput modified; 
firstInput modified2; 
firstInput modified3; 
firstInput reference;  
firstInput nullRecord; 

function integer transform() {
	// copy field by value
	copy.* = $firstInput.*;
	// copy fields by value and modify - original record is untouched (input -> record variable)
	modified.* = $firstInput.*; 
	modified.Name = 'empty';
	modified.Value = 321;
	modified.Born = 1987-11-13;
	// copy fields by value and modify (record variable -> record variable) 
	modified2.* = modified.*; 
	modified2.Name = 'not empty';
	// copy reference and modify the original target as well
	modified3.* = modified2.*; 
	reference = modified3;  
	reference.Value = 654321;
	// copy fields by value to output (record variable -> output)
	$secondOutput.* = reference.*;
	// set all fields in record to null value
	nullRecord.* = $firstInput.*; 
	nullRecord.* = null;
	
	return 0;
}