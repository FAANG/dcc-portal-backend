# In Elastic Search V6, the FAANG backend is comprised of a set of indices
# To help development, the set of indice often needs to be backed up
# This script takes two or three (not supported yet) parameters. The first two parameters defined pattern
# for example, faang_ parameter means that the matched indice would be faang_organism, faang_specimen etc.
# The script copy all indices matching the first parameter to the new indices generating from the second parameter
import subprocess
import constants
import click


@click.command()
@click.option(
    '--es_host',
    default=constants.STAGING_NODE1,
    help='Specify the Elastic Search server(s) (port could be included), e.g. wp-np3-e2:9200. '
)
@click.option(
    '--input_index_pattern',
    help='Specify the pattern of source indices, e.g. faang_build_1. '
)
@click.option(
    '--output_index_pattern',
    help='Specify the pattern of destination indices, e.g. faang_build_2. '
)
def main(es_host, input_index_pattern, output_index_pattern):
    if not input_index_pattern:
        print("Mandatory parameter input_index_pattern is not provided")
        exit()
    if not output_index_pattern:
        print("Mandatory parameter output_index_pattern is not provided")
        exit()

    host: str = f'http://{es_host}/'

    for es_type in constants.INDICES:
        arr = ["elasticdump", f"--input={host}{input_index_pattern}_{es_type}",
               f"--output={host}{output_index_pattern}_{es_type}", "--type=data"]
        cmd = " ".join(arr)
        print(cmd)
        subprocess.run(arr)


if __name__ == "__main__":
    main()
