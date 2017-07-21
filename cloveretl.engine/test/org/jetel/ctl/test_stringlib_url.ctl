boolean[] urlValid;
string[] protocol;
string[] userInfo;
string[] host;
integer[] port;
string[] path;
string[] query;
string[] ref;

function integer transform() {
	string[] url =
		['http://example.com',
		 'https://chuck:norris@server.javlin.eu:12345/backdoor/trojan.cgi?hash=SHA560;god=yes#autodestruct',
		 'HEY! This is NOT valid URL!',
		 'sandbox://cloveretl.test.scenarios/graph/UDR_FileURL_SFTP_OneGzipFileSpecified.grf',
		 'unknown://cloveretl.test.scenarios/graph/UDR_FileURL_SFTP_OneGzipFileSpecified.grf',
		 'zip:(sftp://test:test@koule/home/test/data-in/file2.zip)#innerfolder2/URLIn21.txt'
		];
	integer n = url.length();
	
	for (integer i = 0; i < n; i = ++i) {
		urlValid[i] = isUrl(url[i]);
		protocol[i] = getUrlProtocol(url[i]);
		userInfo[i] = getUrlUserInfo(url[i]);
		host[i] = getUrlHost(url[i]);
		port[i] = getUrlPort(url[i]);
		path[i] = getUrlPath(url[i]);
		query[i] = getUrlQuery(url[i]);
		ref[i] = getUrlRef(url[i]);
	}
	return 0;
}