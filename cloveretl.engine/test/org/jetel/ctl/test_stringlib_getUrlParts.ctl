string[] urls = [
	"sftp://user%40gooddata.com:password@ava-fileManipulator1-devel.getgooddata.com/users/a6/15e83578ad5cba95c442273ea20bfa/msf-183/out5.txt",
	"sandbox://cloveretl.test.scenarios/data-in/fileOperation/input.txt",
	"ftp://test:test@ftp.test.com:21/data/file.txt",
	"https://test:test@www.test.com:80/data/file.txt",
	"smb://user:password@hostname/share/dir/file.txt",
	"hdfs://HADOOP0/dir/file.txt",
	"s3://ACCESSKEY:secretkey@s3.amazonaws.com/bucketname/dir/file.txt",
	"unknown://test:test@unknown.host.com:123/dir/file"
];

boolean[] isUrl;
string[] path;
string[] protocol;
string[] host;
integer[] port;
string[] userInfo;
string[] ref;
string[] query;

boolean isURL_empty;
string path_empty;
string protocol_empty;
string host_empty;
integer port_empty;
string userInfo_empty;
string ref_empty;
string query_empty;

boolean isURL_null;
string path_null;
string protocol_null;
string host_null;
integer port_null;
string userInfo_null;
string ref_null;
string query_null;


function integer transform() {
	for (integer i = 0; i < urls.length(); i++) {
		isUrl[i] = isUrl(urls[i]);
		path[i] = getUrlPath(urls[i]);
		protocol[i] = getUrlProtocol(urls[i]);
		host[i] = getUrlHost(urls[i]);
		port[i] = getUrlPort(urls[i]);
		userInfo[i] = getUrlUserInfo(urls[i]);
		ref[i] = getUrlRef(urls[i]);
		query[i] = getUrlQuery(urls[i]);
	}

	isURL_empty = isUrl("");
	path_empty = getUrlPath("");
	protocol_empty = getUrlProtocol("");
	host_empty = getUrlHost("");
	port_empty = getUrlPort("");
	userInfo_empty = getUrlUserInfo("");
	ref_empty = getUrlRef("");
	query_empty = getUrlQuery("");
		
	isURL_null = isUrl(null);
	path_null = getUrlPath(null);
	protocol_null = getUrlProtocol(null);
	host_null = getUrlHost(null);
	port_null = getUrlPort(null);
	userInfo_null = getUrlUserInfo(null);
	ref_null = getUrlRef(null);
	query_null = getUrlQuery(null);

	return 0;
}