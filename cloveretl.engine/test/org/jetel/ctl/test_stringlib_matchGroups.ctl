string[] result1;
string[] result2;

function integer transform() {
	string pattern = "(([^:]*)([:])([\\(]))(.*)(\\))(((#)(.*))|($))";
	
	result1 = matchGroups("ftp://username:password@server/path/filename.txt", pattern);
	result2 = matchGroups("zip:(zip:(/path/name?.zip)#innerfolder/file.zip)#innermostfolder?/filename*.txt", pattern);
	return 0;
} 