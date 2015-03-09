string[] paths = [
	"/foo/../bar/../baz/out5.txt",
	"/cloveretl.test.scenarios/data-in/fileOperation/input.txt",
	"/data/file.txt",
	"C:\\a\\b\\c.txt",
	"a/b/c.ab.jpg"
];

string path_1;
string path_2;
string path_3;
string path_full;
string normalized_1;
string ext_1;
string ext_2;
string name_1;
string name_2;
string name_noext_1;
string name_noext_2;


function integer transform() {

	path_1=getPath(paths[0]);
	path_2=getPath(paths[3]);
	path_full=getFullPath(paths[3]);
	normalized_1=normalizePath(paths[0]);
	ext_1=getFileExtension(paths[0]);
	ext_2=getFileExtension(paths[4]);
	name_1=getFileName(paths[4]);
	name_2=getFileName(paths[0]);
	name_noext_1=getFileNameWithoutExtension(paths[4]);
	name_noext_2=getFileNameWithoutExtension(paths[0]);
	
	
	return 0;
}