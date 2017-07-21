map[string, string] env;
string path;
string ret1;
string ret2;
string ret3;

boolean empty;

function integer transform() {
	env = getEnvironmentVariables();
	foreach (string key: env.getKeys()) {
		if (upperCase(key) == "PATH") {
			path = env[key];
		}
	}
	empty = isBlank(path);
	ret1 = getEnvironmentVariables()['Vladimir'];
	string emp = null;
	//CLO-1700
//	ret2 = getEnvironmentVariables()[null];
//	ret3 = getEnvironmentVariables()[emp];
	return 0;
}