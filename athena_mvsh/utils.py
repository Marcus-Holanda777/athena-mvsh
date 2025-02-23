import re
import textwrap


PATTERN_OUTPUT_LOCATION = re.compile(r'^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$')


def logs_print(response: dict, logger):
    def print_dict(dict_type: dict):
        for k, v in dict_type.items():
            if isinstance(v, dict):
                logger.info(f'[**{k}**]')
                print_dict(v)
            else:
                logger.info(f'{k} - {v}')

    for k, v in response.items():
        if isinstance(v, dict):
            logger.info(f'[*{k}*]')
            print_dict(v)
        else:
            logger.info(f'{k} - {v}')


def parse_output_location(output_location: str) -> tuple:
    match = PATTERN_OUTPUT_LOCATION.search(output_location)
    if match:
        return match.group('bucket'), match.group('key')
    else:
        raise ValueError('Unknown `output_location` format.')


def query_is_ddl(stmt) -> bool:
    code = textwrap.dedent(stmt.strip())

    token_specification = [
        ('UPDATE', r'^UPDATE\b'),
        ('DELETE', r'^DELETE\b'),
        ('DROP', r'^DROP\b'),
        ('CREATE', r'^CREATE\b'),
        ('MERGE', r'^MERGE\b'),
        ('TRUNCATE', r'^TRUNCATE\b'),
        ('ALTER', r'^ALTER\b'),
        ('RENAME', r'^RENAME\b'),
        ('INSERT', r'^INSERT\b'),
        ('VACUUM', r'^VACUUM\b'),
        ('MSCK', r'^MSCK\b'),
        ('OPTIMIZE', r'^OPTIMIZE\b'),
    ]

    tok_regex = '|'.join('(?P<%s>%s)' % pair for pair in token_specification)
    return bool(list(re.finditer(tok_regex, code, re.I | re.X)))
