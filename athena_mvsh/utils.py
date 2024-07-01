import re

PATTERN_OUTPUT_LOCATION = re.compile(
    r"^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$"
)

def parse_output_location(output_location: str) -> tuple:
    match = PATTERN_OUTPUT_LOCATION.search(output_location)
    if match:
        return match.group("bucket"), match.group("key")
    else:
        raise ValueError("Unknown `output_location` format.")