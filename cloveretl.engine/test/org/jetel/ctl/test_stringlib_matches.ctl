string input;

boolean matches1;
boolean matches2;
boolean matches3;
boolean matches4;
boolean matches5;

function integer transform() {
	input = 'The quick brown fox jumps over the lazy dog';
	matches1 = matches(input, '.*');
	matches2 = matches(input, '^T[a-z A-Z]*g$');
	matches3= matches(input, '[a-z]*');
	matches4 = matches(input, 'The quick brown fox jumps over the lazy dog');
	matches5 = matches(input, '[.&&[o^]]*');
	return 0;
} 