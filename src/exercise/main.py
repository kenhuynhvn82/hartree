import argparse
from exercise.pandasdo import merge_two_datasets_by_pandas
from exercise.beamdo import merge_two_datasets_by_beam
from exercise.bubblesort import sort

parser = argparse.ArgumentParser(description='Merging two datasets with Pandas and Beam')
parser.add_argument(
    '-if1',
    '--inputfile1',
    type=str,
    required=True,
    help='The input file 1'
)
parser.add_argument(
    '-if2',
    '--inputfile2',
    type=str,
    required=True,
    help='The input file 2'
)
parser.add_argument(
    '-of',
    '--outputfile',
    type=str,
    required=True,
    help='The output file'
)
parser.add_argument(
    '-f',
    '--framework',
    type=str,
    required=True,
    help='Framework: pandas, beam'
)

def main(input_file1: str, input_file2: str, output_file: str, framework: str):
    if framework == 'pandas':
        merge_two_datasets_by_pandas(input_file1, input_file2, output_file)
    elif framework == 'beam':
        merge_two_datasets_by_beam(input_file1, input_file2, output_file)
    else:
        raise Exception('Invalid framework')

if __name__ == '__main__':
    args = parser.parse_args()
    main(args.inputfile1, args.inputfile2, args.outputfile, args.framework)

    # nums = [5,3,4,2]
    # result = sort(nums)
    # print(result)