import ConfigParser


class ConfigStage:
    def __init__(self, ini, stage=None):
        self.config = ConfigParser.ConfigParser()
        self.stage = stage
        self.config.read(ini)

    def get_stage_section(self, section):
        if self.stage is None:
            return section
        else:
            return "{}.{}".format(section, self.stage)

    def get(self, section, option, default=None):
        stage_section = self.get_stage_section(section)
        if self.config.has_section(stage_section):
            if self.config.has_option(stage_section, option):
                return self.config.get(stage_section, option)

        if self.config.has_section(section):
            if self.config.has_option(section, option):
                return self.config.get(section, option)

        if default is None:
            raise ConfigParser.NoOptionError(option, section)
        else:
            return default

    def items(self, section, default=None):
        stage_section = self.get_stage_section(section)
        if self.config.has_section(stage_section):
            result = dict(self.config.items(stage_section))
        else:
            result = {}

        if self.config.has_section(section):
            for item in self.config.items(section):
                if item[0] not in result:
                    result[item[0]] = item[1]

        if len(result) == 0 and default is None:
            raise ConfigParser.NoSectionError(section)
        else:
            return result

