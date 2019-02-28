import re

def to_lower_camel_case(str_to_convert):
    """
    This function will convert any string with spaces or underscores to lower camel case string
    :param str_to_convert: target string
    :return: converted string
    """
    tmp = re.split(r'\s|-', str_to_convert)
    return "".join([item.lower() for i,item in enumerate(tmp) if i == 0] +
                   [item.capitalize() for i,item in enumerate(tmp) if i != 0])