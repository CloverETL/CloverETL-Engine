string[] chars;
string input;


function integer transform() {
	input='The QUICk !!$  broWn fox 	juMPS over the lazy DOG	'; 
	for (integer i = 0; i < length(input); i++) {
		chars[i] = (charAt(input,i));
	};	
	return 0;
}