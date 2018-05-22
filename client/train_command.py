import copy

class TrainCommand(object):
    def __init__(self, command_string, defaults = {}):
        self.parameter_map = copy.copy(defaults)
        self.commands_processed = []
        if command_string:
            self.parse(command_string)
    def update(self, dic):
        self.parameter_map.update(dic)
    def _stringify(self, major_seperator, sub_parameter_pattern):
        commands_processed = copy.copy(self.commands_processed)
        for key, val in self.parameter_map.iteritems():
            commands_processed.append(sub_parameter_pattern % (key, val))
        return major_seperator.join(commands_processed)
    def to_python_command(self):
        return self._stringify(" ", "--%s %s")
    def parse(self, command_string):
        parameter_map = self.parameter_map
        commands_processed = self.commands_processed
        for seg in command_string.split(","):
            if ":" in seg:
                parameters = seg.split(":")
                #key has to be a string
                parameter_map[parameters[0].strip()] = (str(parameters[1])).strip()
            else:
                commands_processed.append(seg.strip())
    def unparse(self):
        return self._stringify(",", "%s:%s")