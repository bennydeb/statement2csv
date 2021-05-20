import os.path
import argparse
from Statement import StatementCorpus


# sttmt2csv.py --path ~/Server/Downloads/Transactions_2020/credit_test/ --table_layout trans_date post_date id
# description amount --account_type='credit'
#
# python sttmt2csv.py --path ~/Server/Downloads/Transactions_2020/test/

def dir_path(dir_string):
    if os.path.isdir(dir_string):
        return dir_string
    else:
        raise NotADirectoryError(dir_string)


def file(filename):
    if os.path.isfile(filename):
        return filename
    else:
        raise FileNotFoundError(filename)


def args_handler(*args):
    #
    # Use argparse to handle parsing the command line arguments.
    #   https://docs.python.org/3/library/argparse.html
    #

    parser = argparse.ArgumentParser(description='Statement to CSV')
    parser.add_argument('--path', metavar='S', type=dir_path, default='.',
                        help='directory with statement corpus')
    parser.add_argument('--account_type', metavar='S', type=str, default='savings',
                        help='Account Type: savings, debit, credit')
    parser.add_argument('--bank', metavar='S', type=str, default='natwest',
                        help='Bank name')
    parser.add_argument('--extension', metavar='ext', type=str, default='pdf',
                        help='Statements extension: pdf')
    parser.add_argument('--table_layout', metavar='str1 str2 str3 ...', nargs="+",
                        default=['date', 'type', 'description', 'paid in',
                                 'paid out', 'balance'],
                        help='Table columns layout')
    parser.add_argument('--date_position', metavar='N', type=int, default=0,
                        help='Index of date field on each transaction')
    parser.add_argument('--date_format', metavar='%d%m%y', type=str,
                        default='%d %b %Y',
                        help='date format found on transactions')
    parser.add_argument('--reg_exp', metavar='RE', type=str,
                        default="((\d{2}\s[A-Z]{3})\s{0,1}(\d{2}\s[A-Z]{3}))\s(\d{0,9})"
                                "\s{0,1}(\w[\S\s]*?)(((\d{0,3}){0,}\.(\d{2}))(\s(-)){0,1})",
                        help='regular expresion to extract tx')
    parser.add_argument('--reg_exp_groups', metavar='RE_G', type=list,
                        default=[2, 3, 4, 5, 7, 11],
                        help='groups from regexp to extract from tx')
    parser.add_argument('--input_file', metavar='F', type=file, default=None,
                        help='File to read in (optional)')
    parser.add_argument('--output_file', metavar='F', type=str, default='output.csv',
                        help='CSV file to write to')
    args = parser.parse_args(args)

    return args


def main(*args):
    parsed_args = args_handler(*args)
    args_ = vars(parsed_args)

    debit = StatementCorpus(**args_)
    debit.to_csv()


if __name__ == '__main__':
    import sys

    main(*sys.argv[1:])
