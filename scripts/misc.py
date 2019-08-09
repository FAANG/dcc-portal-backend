"""
A collection of commonly-used functions, could be cross-projects
"""
import re
import utils

logger = utils.create_logging_instance('misc')


def to_lower_camel_case(str_to_convert):
    """
    This function will convert any string with spaces or underscores to lower camel case string
    :param str_to_convert: target string
    :return: converted string
    """
    if type(str_to_convert) is not str:
        raise TypeError("The method only take str as its input")
    str_to_convert = str_to_convert.replace("_", " ")
    tmp = re.split(r'\s|-', str_to_convert)
    return "".join([item.lower() for i, item in enumerate(tmp) if i == 0] +
                   [item.capitalize() for i, item in enumerate(tmp) if i != 0])


def from_lower_camel_case(str_to_convert):
    """
    This function will convert 'lowerCamelCase' to 'lower camel case'
    :param str_to_convert: string to convert
    :return: return converted string
    """
    if type(str_to_convert) is not str:
        raise TypeError("The method only take str as its input")
    tmp = re.split(r'([A-Z])', str_to_convert)
    tmp2 = list()
    tmp2.append(tmp[0])
    for i in range(1, len(tmp), 2):
        tmp2.append(tmp[i] + tmp[i+1])
    return " ".join([item.lower() for item in tmp2])


def get_filename_from_url(url, accession):
    """
    Return the filename extracted from the given URL. If it is not a pdf file, return the original url
    :param url: url to parse
    :param accession: accession number
    :return: file name
    """
    if (not url) or (url and len(url) == 0):
        logger.debug(f"{accession} url is empty")
        return ""
    if url.lower().endswith(".pdf"):
        return url.split("/")[-1]
    else:
        return url


def convert_readable(size_to_convert):
    """
    This function will convert size to human readable string
    :param size_to_convert: size in bytes
    :return: human-readable string with size
    """
    size_to_convert = int(size_to_convert)
    units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB']
    for i in range(6):
        size_to_convert /= 1024
        if size_to_convert < 1:
            break
    size_to_convert *= 1024
    if i == 0:
        return f"{size_to_convert}B"
    return f"{round(size_to_convert, 2)}{units[i]}"


def parse_date(date_str: str) -> str:
    """
    extract YYYY-MM-DD from ISO date string used by BioSamples
    :param date_str: ISO date string
    :return: parsed date
    """
    if date_str:
        match = re.search(r'(\d+-\d+-\d+)T', date_str)
        if match:
            return match.group(1)
        else:
            return date_str
    return None