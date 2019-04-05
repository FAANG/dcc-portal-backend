"""
Remove the oldest local etag cache files
"""
import click
import subprocess

@click.command()
@click.option(
    '--number_to_keep',
    default="5",
    help='Specify how many etag cache files will be kept locally, default to be 5'
)

def main(number_to_keep):
    """
    The main function
    :param number_to_keep:
    :return:
    """
    try:
        num = int(number_to_keep)
    except ValueError:
        print(f"The provided parameter value {number_to_keep} is not an integer")
    # get the shell command output, the shell command list files in the datestamp order, newest at the top
    result = subprocess.run(['ls -tl etag_list_*.txt'], stdout=subprocess.PIPE, shell=True).stdout.decode('utf-8')
    lines = result.split('\n')
    # remove the empty line as the command prompt always start in the new line,
    # i.e. there is a \n at the end of ls result
    lines.pop()
    total = len(lines)
    if num >= total:
        print ("Within the limit, all etag cache files are kept")
    else:
        for i in range(num, len(lines)):
            remaining = lines[i].split("etag_list_")[1:]
            subprocess.run(f"rm etag_list_{remaining[0]}", shell=True)


if __name__ == "__main__":
    main()
