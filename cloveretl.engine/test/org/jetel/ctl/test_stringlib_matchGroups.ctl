string[] result1;
string[] result2;
string[] result3;

string[] test_empty1;
string[] test_empty2;
string[] test_null1;
string[] test_null2;
function integer transform() {
	string pattern = "(([^:]*)([:])([\\(]))(.*)(\\))(((#)(.*))|($))";
	
	result1 = matchGroups("ftp://username:password@server/path/filename.txt", pattern);
	result2 = matchGroups("zip:(zip:(/path/name?.zip)#innerfolder/file.zip)#innermostfolder?/filename*.txt", pattern);
	result3 = matchGroups('kokon','viliam');
	test_empty1 = matchGroups('','[a-z]+');
	test_empty2 = matchGroups('','[a-z]*');
	test_null1 = matchGroups(null,'[a-z]+');
	test_null2 = matchGroups(null,'[a-z]*');
	return 0;
} 