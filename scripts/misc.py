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


def from_lower_camel_case(str_to_convert):
    """
    This function will convert 'lowerCamelCase' to 'lower camel case'
    :param str_to_convert: string to convert
    :return: return converted string
    """
    tmp = re.split(r'([A-Z])', str_to_convert)
    tmp2 = list()
    tmp2.append(tmp[0])
    for i in range(1, len(tmp), 2):
        tmp2.append(tmp[i] + tmp[i+1])
    return " ".join([item.lower() for item in tmp2])


def get_filename_from_url(url, accession):
    """
    Return filename from url
    :param url: url to parse
    :param accession: accession number
    :return: file name
    """
    if (not url) or (url and len(url) == 0):
        # TODO add logging
        print(f"{accession} url is empty")
        return ""
    if 'pdf' in url.lower():
        return url.split("/")[-1]
    else:
        return url