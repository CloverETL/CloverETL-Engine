integer dist;
integer dist1;
integer dist2;
integer dist3;
integer dist4;
integer dist5;
integer dist6;
integer dist7;
integer dist8;
integer dist9;

function integer transform() {
	dist = editDistance('agata','ahata');
	dist1 = editDistance('agata','agatą'); 
	dist2 = editDistance('agata','agatą',2); 
	dist5 = editDistance('agata','agatą',2,'CZ.cz'); 
	dist3 = editDistance('agata','agatą',3);
	dist4 = editDistance('agata','Agata',3);
	dist6 = editDistance('hello','vitej',3);
	dist7 = editDistance('hello','vitej',3,10); 
	dist8 = editDistance('aAeEiIoOuUÚnN','áÁéÉíÍóÓúüÜñÑ',2,'ES.es');
	dist9 = editDistance('aAAaaeeeeEEEEcC','àâAÀÂéêëEÈÉÊËçÇ',2,'FR.fr');
	return 0;
}