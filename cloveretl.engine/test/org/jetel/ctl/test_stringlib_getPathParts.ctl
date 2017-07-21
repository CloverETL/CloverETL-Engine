string[] paths = [
	null,
	"",
	"/foo/../bar/../baz/out5.txt",
	"/cloveretl.test.scenarios/./data-in/fileOperation/input.xlsx",
	"/data/file.dat",
	"C:/a/b/c.cdf",
	"C:\\a\\b\\c.xml",
	"a/b/c.ab.jpg",
	"file:/C:/Users/krivanekm/workspace/Experiments/",
	"sandbox://cloveretl.test.scenarios/",
	"sandbox://cloveretl.test.scenarios/a/b/c.dbf",
	"ftp://user:password@hostname.com/a/b",
	"ftp://user:password@hostname.com/a/../b/c.gz",
	"s3://user:password@hostname.com/a/b/c.Z",
	"sftp://user:password@hostname.com/../a/b",
	"sandbox://cloveretl.test.scenarios",
	"sandbox://cloveretl.test.scenarios/file_with_query?and#hash.txt"
];

string[] extensions;
string[] filenames;
string[] filenamesWithoutExtension;
string[] filepaths;
string[] normalized;

function integer transform() {

	for (integer i = 0; i < paths.length(); i++) {
		extensions[i] = getFileExtension(paths[i]);
		filenames[i] = getFileName(paths[i]);
		filenamesWithoutExtension[i] = getFileNameWithoutExtension(paths[i]);
		filepaths[i] = getFilePath(paths[i]);
		normalized[i] = normalizePath(paths[i]);
	}
	
	return 0;
}