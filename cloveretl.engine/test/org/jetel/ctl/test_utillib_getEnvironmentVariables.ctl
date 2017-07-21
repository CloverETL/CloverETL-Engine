map[string, string] env;
string path;
boolean empty;

function integer transform() {
	env = getEnvironmentVariables();
	foreach (string key: env.getKeys()) {
		if (upperCase(key) == "PATH") {
			path = env[key];
		}
	}
	empty = isBlank(path);
	
	return 0;
}