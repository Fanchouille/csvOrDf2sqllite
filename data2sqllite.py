import pandas as pd
from sqlalchemy import create_engine
import os
import argparse



class Data2SqlliteLoader:
    def __init__(self):
        return

    def load_df_to_sql_lite(self, iDf, iDbName, iTableName, iIndex=False, iIf_exists='replace', iDbPath=None):
        """

        :param iDf: input dataframe
        :param iDbName: name of the database to be created
        :param iTableName: tablename inside the database
        :param iIndex: True or False if we want to save index or not in db (default = False)
        :param iIf_exists: 'fail','replace','append', default = 'replace'
        :param iDbPath: if None, put db file in current directory
        :return:
        """
        if iDbPath is None:
            db_path = os.getcwd()
        else:
            db_path = iDbPath
        disk_engine = create_engine('sqlite:///' + db_path + '/' + iDbName + '.db')
        iDf.to_sql(iTableName, disk_engine, index=iIndex, if_exists=iIf_exists)
        return

    def load_csv_to_sql_lite(self, iCsvFilePath, iDbName, iTableName, iIndex=False, iIf_exists='replace', iDbPath=None):
        """

        :param iCsvFilePath: path of file
        ::param iDbName: name of the database to be created
        :param iTableName: tablename inside the database
        :param iIndex: True or False if we want to save index or not in db (default = False)
        :param iIf_exists: 'fail','replace','append', default = 'replace'
        :param iDbPath: if None, put db file in current directory
        :return:
        """
        df = pd.read_csv(iCsvFilePath, sep=None)
        self.load_df_to_sql_lite(df, iDbName, iTableName, iIndex, iIf_exists, iDbPath)
        return


if __name__ == '__main__':
    loader = Data2SqlliteLoader()
    parser = argparse.ArgumentParser(description='''Convert a CSV file to a table in a SQLite database.''')
    parser.add_argument('csv_file', type=str, help='Input CSV file path')
    parser.add_argument('dbname', type=str, help='Output SQLite db')
    parser.add_argument('table_name', type=str, help='Name of table to write in db')
    parser.add_argument('index', type=bool, nargs='?', help='Keep index or not', default=False)
    parser.add_argument('ifexists', type=str, nargs='?', help='What to do if exists', default='replace')
    parser.add_argument('db_path', type=str, nargs='?', help='DbPath', default=None)

    args = parser.parse_args()
    loader.load_csv_to_sql_lite(args.csv_file, args.dbname, args.table_name, args.index, args.ifexists, args.db_path)
    #
