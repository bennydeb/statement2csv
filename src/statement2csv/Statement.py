from datetime import datetime
from datetime import date as dt_date
from glob import glob
import pdfplumber
import pandas as pd
import re


# TODO: tests
# StatementCorpus>Statement>page>table>tx
class StatementCorpus(object):
    def __init__(self, **kwargs):

        self.path = kwargs.pop('path', None)
        self.account_type = kwargs.pop('account_type', None)
        self.bank = kwargs.pop('bank', None)
        self._extension = kwargs.pop('extension', None)
        self._table_layout = kwargs.pop('table_layout', None)
        self._date_position = kwargs.pop('date_position', None)
        self._date_format = kwargs.pop('date_format', None)
        self._output_file = kwargs.pop('output_file', None)
        self._reg_exp = kwargs.pop('reg_exp', None)
        self._reg_exp_groups = kwargs.pop('reg_exp_groups', None)

        # TODO: do in_file case.
        self._filename_list = self._get_filename_list()
        self._Statements = self._get_statements()
        self.N_txs = self._get_N_txs()
        self.txs = self._to_dataframe()
        self.duplicated = self.any_duplicated()

    def _get_filename_list(self):
        filename_string = f'{self.path}/*.{self._extension}'
        filename_list = glob(filename_string)
        if not len(filename_list):
            raise TypeError(f'No files were found in {filename_string}')
        return filename_list

    def _get_statements(self):
        _Statements = []
        for statement in self._filename_list:
            _Statements.append(self.Statement(in_file=statement, corpus=self))
        return _Statements

    def _get_N_txs(self):
        N_txs = 0
        for statement in self._Statements:
            N_txs = N_txs + statement.n_txs
        return N_txs

    def get_date_position(self):
        return self._date_position

    def get_date_format(self):
        return self._date_format

    def get_table_layout(self):
        return self._table_layout

    def get_reg_exp(self):
        return self._reg_exp

    def get_reg_exp_groups(self):
        return self._reg_exp_groups

    def print_txs(self):
        for statement in self._Statements:
            print(statement)

    def _to_dataframe(self, first_column_index=True):
        df = pd.DataFrame([], columns=self._table_layout)
        for statement in self._Statements:
            df1 = pd.DataFrame(statement.statement_txs,
                               columns=self._table_layout)
            df = pd.concat([df, df1], axis=0)
            df.reset_index()
        if first_column_index:
            df.set_index(df.columns[0], inplace=True)
            df.sort_index(inplace=True)

        return df

    def to_csv(self, **kwargs):
        output_file = kwargs.pop('output_file', self._output_file)
        # TODO: Better hash, this one had collissions
        # if self.duplicated:
        #     for (i, hash_) in self.duplicated:
        #         print(self.txs.iloc[i], hash_)
        #     raise UserWarning(self.duplicated)

        print(f'{output_file} with {len(self.txs)} transactions written.')
        self.txs.to_csv(output_file)

    def print_all_args(self):
        print("All parameters:")
        print(self.path, self.account_type, self.bank, self._extension,
              self._table_layout, self._date_position, self._date_format,
              self._output_file)

    def any_duplicated(self):
        """
        Check if there is any duplicated row by hashing it and comparing
        :param self:
        :return: index of duplicated rows
        """
        hashes = self.txs.apply(lambda x: hash(tuple(x)), axis=1)
        duplicates = hashes.duplicated()
        return [(i, hashes[i]) for i, x in enumerate(duplicates) if x]

    class Statement(object):

        def __init__(self, **kwargs):
            self.StatementCorpus = kwargs.pop('corpus', None)
            self.in_file = kwargs.pop('in_file', None)
            self.sttmt_period = []
            self.statement_txs = []
            self.n_txs = 0
            self._extract_txs()

        def __repr__(self):
            columns = self.StatementCorpus.get_table_layout()
            return f'{pd.DataFrame(self.statement_txs, columns=columns)}'

        def print_sttmt(self):
            columns = self.StatementCorpus.get_table_layout()

            print(pd.DataFrame(self.statement_txs,
                               columns=columns))

        @staticmethod
        def _extract_regexp_groups(found_txs, reg_exp_groups):
            txs = []
            for i, tx in enumerate(found_txs):
                tx_txt = []
                # TODO fix this hack. It would be better
                # not to assume location and use the table layout
                # For now, we consider last position the symbol "-"
                for group in reg_exp_groups[:-1]:
                    tx_txt.append(f'{tx[group - 1]}')

                if tx[reg_exp_groups[-1]-1] == '-':
                    tx_txt[-1] = '-' + tx_txt[-1]

                txs.append(tx_txt)

            return txs

        @staticmethod
        def _is_datetime(date):
            return isinstance(date, dt_date)

        @staticmethod
        def _get_sttmt_period(txs_text,
                              period_regexp="((\d{2})\s(January|February|March|April|"
                                            "May|June|July|August|September|October|"
                                            "November|December)((\s\d{4}){0,1})\s-\s"
                                            "(\d{2})\s(January|February|March|April|"
                                            "May|June|July|August|September|October|"
                                            "November|December)\s((\d{4}){0,1}))",
                              period_date_groups=([1, 2, 3], [5, 6, 7]),
                              period_date_format='%d %B %Y'):
            """
            Find period (start, end dates) of statement
            :param txs_text: Transaction as Text list
            :param period_regexp: regexp used to get period
            :param period_date_groups:
            :param date_format:
            :return:
            """
            (start_groups, end_groups) = period_date_groups

            date_found = re.findall(period_regexp, txs_text)
            if not date_found:
                return []

            [date_found] = date_found
            start_period = [date_found[group] for group in start_groups]
            end_period = [date_found[group] for group in end_groups]
            # When start and end belong to same year, only end has year value
            # so we make start take the year from end
            if start_period[2] == '': start_period[2] = end_period[2]

            start_period = datetime.strptime(" ".join(start_period), period_date_format)
            end_period = datetime.strptime(" ".join(end_period), period_date_format)

            return start_period, end_period

        def _extract_text(self):
            crr_file = []
            with pdfplumber.open(self.in_file) as f:
                for page in f.pages:
                    crr_file.append(page.extract_text())
            return crr_file

        def _extract_txs(self):
            """
            Extract transactions correcting dates depending on
            the bank and account type.
            :return:
            """
            txs = []
            if self.StatementCorpus.bank == 'natwest' \
                    and self.StatementCorpus.account_type in ['debit', 'savings']:
                self.statement_txs = self._extract_txs_natwest_tables()
                # debit and savings do not repeat date field on tx
                # from same date, so we need to fill those dates.
                self._fill_empty_dates()
            elif self.StatementCorpus.bank == 'natwest' \
                    and self.StatementCorpus.account_type == 'credit':
                self.statement_txs = self._extract_txs_natwest_txt()
                self._fix_dates()

        def _extract_txs_natwest_tables(self):
            """
            Obtain transactions depending on the type of format.
            :return: a list with transactions as lists
            """
            with pdfplumber.open(self.in_file) as f:
                crr_file_txs = []
                # check if each page has at least 3 tables
                for page in f.pages:
                    tables = page.extract_tables()
                    # 3 tables means there are transactions
                    if len(tables) == 3:
                        self.n_txs = self.n_txs + len(tables[2])
                        # The middle table contains the transactions
                        for tx in tables[2]:
                            # Replace some print characters
                            for att in range(len(tx)):
                                tx[att] = tx[att].replace("\n", " ")
                        crr_file_txs.append(tables[2])
            txs = [tx for txs_list in crr_file_txs for tx in txs_list]
            print(f'{self.in_file}: {len(txs)}')
            return txs

        def _extract_txs_natwest_txt(self):
            """
            Obtain transactions from text extracted from statements
            :return: a list with transactions as lists.
            """
            txs = []
            with pdfplumber.open(self.in_file) as f:
                # Find text
                for page in f.pages:
                    page_txt = page.extract_text()
                    # Get starting and finishing dates, for fixing date format
                    if not self.sttmt_period:
                        sttmt_period = self._get_sttmt_period(page_txt)
                        if sttmt_period:
                            self.sttmt_period = sttmt_period

                    page_txs = self._extract_w_regexp(page_txt)
                    self.n_txs = self.n_txs + len(page_txs)
                    # Append if not empty
                    if page_txs:

                        txs.append(page_txs)

            txs = [tx for pages in txs for tx in pages]
            print(f'{self.in_file}: {len(txs)}')
            return txs

        def _extract_w_regexp(self, page):
            reg_exp = self.StatementCorpus.get_reg_exp()
            reg_exp_groups = self.StatementCorpus.get_reg_exp_groups()
            found_txs = re.findall(reg_exp, page)
            found_txs = self._extract_regexp_groups(found_txs, reg_exp_groups)
            return found_txs

        def _fix_dates(self, date_position=[0, 1]):
            """
            Makes sure dates on statament_txs have the
            correct format (%d %b %Y).
            Note: natwest credit requires a special treatment
            as date format is %d %B
            :return:
            """
            start, end = self.sttmt_period

            # {month:year} dictionary
            year = {start.strftime('%b').upper(): start.strftime('%Y'),
                    end.strftime('%b').upper(): end.strftime('%Y')}

            for tx in self.statement_txs:

                for date_pos in date_position:
                    # split day and month from date format is "05 DEC"
                    date = tx[date_pos].split(" ")
                    # add year to date
                    date_str = tx[date_pos] + " " + year[date[1]]
                    tx[date_pos] = self._datestring_to_datetime(date_str,
                                                                date_position=date_pos, )

        def _fill_empty_dates(self):
            """Some transactions do not have date. It is assumed that
            the last valid date also correspond to transactions without date.
            This function takes the last valid date an assigns it to
            transactions without date.

            Arguments
            statement_txs: transactions obtained from extract_txs()
            date_index: position of the 'date' attribute in each tx
            """
            # fill empty dates with previous value
            for tx_index, tx in enumerate(self.statement_txs):
                date_position = self.StatementCorpus.get_date_position()
                if tx[date_position] == '':
                    date = self._get_previous_valid_date(tx_index,
                                                         date_position)
                    # statement_txs,tx_index, #date_index)
                    self.statement_txs[tx_index][date_position] = date

                # change datestring to datetime
                if type(self.statement_txs[tx_index][date_position]) is not datetime:
                    self.statement_txs[tx_index][date_position] = \
                        self._datestring_to_datetime(tx[date_position])

        def _get_previous_valid_date(self, tx_index, date_position):
            # Do nothing if already and the beginning of list
            if tx_index == 0:
                raise ValueError("No valid date found on current statement")

            # Get date of the previous entry, if empty lookup recursively
            date = self.statement_txs[tx_index - 1][date_position]
            if date == '':
                date = self._get_previous_valid_date(tx_index - 1, date_position)
            return date

        def _datestring_to_datetime(self, date_str, **kwargs):
            date_format = kwargs.pop("date_format",
                                     self.StatementCorpus.get_date_format())

            return datetime.strptime(date_str, date_format)
