import os
import re
from collections import defaultdict
import yaml

ESCAPE_CHARS = ")(.*|"


class ConverterType(type):
    def __init__(cls, *args, **kwargs):
        super(ConverterType, cls).__init__(*args, **kwargs)
        if not hasattr(cls, 'registry'):
            cls.registry = {}
        if hasattr(cls, 'name'):
            cls.registry[cls.name] = cls

    def __getitem__(cls, item):
        return cls.registry[item]

    def __contains__(cls, item):
        return item in cls.registry

    def get(cls, item):
        return cls.registry[item]

    def get_types(self):
        return self.registry


class Converter(object, metaclass=ConverterType):
    """
    Converter Base Class
    """
    chars = r"."

    @classmethod
    def regex(cls, name, size=None):
        if size is None:
            return rf'\s*?(?P<{name}>{cls.chars}+)\s*?'
        else:
            return rf'{cls.chars}{{1,{size}}}'


class Int(Converter):
    name = 'int'
    chars = r'[\d]'

    @classmethod
    def regex(cls, name, size=None):
        if not size:
            return rf'\s*?(?P<{name}>[-+]?\d+)\s*?'
        else:
            return rf'(?<=\s)(?=.{{{size}}})(?P<{name}>\s*[-+]?\d+)(?=[^\d])'

    @staticmethod
    def to_python(value):
        return int(value.strip())


class String(Converter):
    name = 'str'
    chars = r'.'

    @staticmethod
    def to_python(value):
        return value.strip()


class Char(Converter):
    name = 'char'
    chars = r'.'

    @staticmethod
    def to_python(value):
        return value

    @classmethod
    def regex(cls, name, size=None):
        if not size:
            return rf'(?P<{name}>{cls.chars})'
        else:
            return rf'(?P<{name}>{cls.chars}{{{size}}})'


class Slug(Converter):
    name = 'slug'
    chars = r'[-a-zA-Z0-9_/.]'

    @staticmethod
    def to_python(value):
        return value.strip()


class Float(Converter):
    name = 'float'

    @classmethod
    def regex(cls, name, size=None):
        if not size:
            return rf'\s*?(?P<{name}>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)\s*?'
        else:
            return rf'(?<=\s)(?=.{{{size}}})(?P<{name}>\s*[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)(?=[^\d])'

    @staticmethod
    def to_python(value):
        return float(value.strip())


class Line(Converter):
    name = 'line'

    @classmethod
    def regex(cls, name, size=None):
        return rf'^|\n(?P<{name}>[^\n]*)\n|$'

    @staticmethod
    def to_python(value):
        return value


def escape(text):
    for c in ESCAPE_CHARS:
        text = text.replace(c, '\{}'.format(c))
    return text


def build(pattern):
    """
    Parse the text and generate the corresponding regex expression, replacing all fields

    :param pattern: parser specification
    """
    field_pattern = re.compile(r'^(?P<type>\w+):(?P<name>\w+)(?::(?P<regex>\(.*?\)))?(?::(?P<size>\d+))?$')
    tokens = re.findall(r'<([^<>]+?)>', pattern)
    counts = defaultdict(int)
    variables = []
    # extract token parameters
    for token in tokens:
        match = field_pattern.match(token)
        if not match: continue

        # Extract token fields
        data = {k: v for k, v in match.groupdict().items() if v is not None}
        data['token'] = '<{}>'.format(token)

        if data['type'] not in Converter:
            continue
        data['converter'] = Converter[data['type']]
        counts[data['name']] += 1
        variables.append(data)

    tuples = [key for key, count in counts.items() if count > 1]
    index = defaultdict(int)

    # select key name for regex, and prepare pattern
    pattern = escape(pattern)

    for variable in variables:
        if variable['name'] in tuples:
            variable['key'] = '{}_{}'.format(variable['name'], index[variable['name']])
            index[variable['name']] += 1
        else:
            variable['key'] = variable['name']

        # Build field regex and replace token in pattern
        if 'regex' in variable:
            regex = r'(?P<{}>{})'.format(variable['key'], variable['regex'])
        else:
            regex = variable['converter'].regex(variable['key'], variable.get('size'))
        pattern = pattern.replace(variable['token'], regex, 1)
    return re.compile(pattern), variables


def parse_fields(spec, text, table=False):
    groups = defaultdict(list)
    regex, variables = build(spec)
    converters = {variable['key']: variable['converter'] for variable in variables}
    for variable in variables:
        # ignore internal names
        if not variable['name'].startswith('_'):
            groups[variable['name']].append(variable['key'])

    if table:
        results = []
        for m in regex.finditer(text):
            raw_values = {k: converters[k].to_python(v) for k, v in m.groupdict().items() if not k.startswith('_')}
            results.append({
                name: raw_values[name] if len(keys) == 1 else tuple(raw_values[key] for key in keys)
                for name, keys in groups.items()
            })
        return results
    else:
        m = regex.search(text)
        if m:
            raw_values = {k: converters[k].to_python(v) for k, v in m.groupdict().items() if not k.startswith('_')}
            return {
                name: raw_values[name] if len(keys) == 1 else tuple(raw_values[key] for key in keys)
                for name, keys in groups.items()
            }
    return {}


def parse_section(section, data):
    if section.get('domains'):
        sub_data = '\n'.join(re.findall(section["domains"], data, re.DOTALL))
    elif section.get('domain'):
        m = re.search(section["domain"], data, re.DOTALL)
        if m:
            sub_data = m.group(0)
        else:
            sub_data = ""
    else:
        sub_data = data

    output = {}
    if sub_data:
        if 'fields' in section:
            if isinstance(section['fields'], list):
                for spec in section['fields']:
                    output.update(parse_fields(spec, sub_data))
        elif 'lines' in section:
            if isinstance(section['lines'], list):
                spec = '\\n' +'\\n'.join(section['lines'])
                output.update(parse_fields(spec, sub_data))
        elif 'table' in section:
            if isinstance(section['table'], list):
                spec = '\\n' + '\\n'.join(section['table'])
            else:
                spec = section['table']
            return parse_fields(spec, sub_data, table=True)
        if 'sections' in section:
            for sub_name, sub_section in section['sections'].items():
                output[sub_name] = parse_section(sub_section, sub_data)
    return output


def parse_text(data, specs):
    return parse_section(specs['root'], data)


def parse(data_file, specs, size=-1):
    with open(data_file, 'r', encoding='utf-8') as handle:
        data = handle.read(size)
    return parse_text(data, specs)


def cut_section(start, end, s, position=0):
    """
    extract the piece of text between start pattern and end pattern starting at
    position <position>
    returns a tuple (subsection, end-position)

    """
    if start is None and end is None:
        return (s, 0)

    if start is not None:
        start_re = re.compile(start, re.DOTALL)
        start_m = start_re.search(s, position)
        if start_m:
            _s = start_m.start()
        else:
            _s = position
    else:
        _s = position
    if end is not None:
        end_re = re.compile(end, re.DOTALL)
        end_m = end_re.search(s, _s)
        if end_m:
            _e = end_m.end()
        else:
            _e = len(s)
    else:
        _e = len(s)
    result = (s[_s:_e], _e)
    return result
