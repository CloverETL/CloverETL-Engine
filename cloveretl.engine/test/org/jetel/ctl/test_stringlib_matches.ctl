string input;

boolean matches1;
boolean matches2;
boolean matches3;
boolean matches4;
boolean matches5;

boolean matches6;
boolean matches7;

boolean matches8;
boolean matches9;
boolean matches10;

function integer transform() {
	input = 'The quick brown fox jumps over the lazy dog';
	matches1 = matches(input, '.*');
	matches2 = matches(input, '^T[a-z A-Z]*g$');
	matches3= matches(input, '[a-z]*');
	matches4 = matches(input, 'The quick brown fox jumps over the lazy dog');
	matches5 = matches(input, '[.&&[o^]]*');
	
	matches6 = matches(null,'[a-z]*');
	matches7 = matches(null, '');
	
	matches8 = matches('','[a-z]+');
	matches9 = matches('','[a-z]*');
	matches10 = matches('','');	
	
	return 0;
} 