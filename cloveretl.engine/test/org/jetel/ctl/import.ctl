integer a =3;

function void print_result(integer from,integer to, integer format) {
	if (isnull(format)) {
        if (try_convert(from,to)) 
        	printErr('converted:'+from+'-->'+to);
        else {
        	printErr('cant convert:'+from+'-->'+to);
        }
   	} else {
        if (try_convert(from,to, format)) 
        	printErr('converted:'+from+'-->'+to);
        else 
        	printErr('cant convert:'+from+'-->'+to+' with pattern '+format);
    }
}