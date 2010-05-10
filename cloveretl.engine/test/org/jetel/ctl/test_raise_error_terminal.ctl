// This code is valid - raise_error is a valid function termination point 

function integer transform(){
	integer a = 1;
    if (a>1)
       raise_error("Ship time can't be lower than order date");
    else
       return 1;
}