import re


class Options(object):
    options: dict[str, str]

    def __init__(self, options: str = ""):
        options_match = re.findall(r"(\w+)=(\w+)", options)
        self.options = {}
        for option_match in options_match:
            self.options[option_match[0]] = option_match[1]

    def __getitem__(self, key: str | tuple):
        default = None
        index = ""
        if issubclass(type(key), tuple):
            default = key[1]
            index = key[0]
        else:
            index = key

        if index not in self.options.keys():
            return default
        return self.options[index]

    def __setitem__(self, key: str, value: str):
        self.options[key] = value
