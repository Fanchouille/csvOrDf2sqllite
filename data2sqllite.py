import pandas as pd
from sqlalchemy import create_engine
import os


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
        disk_engine = create_engine('sqlite://' + db_path + '/' + iDbName + '.db')
        iDf.to_sql(iTableName, disk_engine, index=iIndex, if_exists=iIf_exists)
        return

    def load_csv_to_sql_lite(self, iCsvFilePath, iDbName, iTableName, iIndex=False, iIf_exists='replace', iDbPath=None):
        """

        :param iCsvFilePath: path of file
        :param iDbName:
        :param iTableName:
        :param iIndex:
        :param iIf_exists:
        :param iDbPath:
        :return:
        """
        #https://github.com/Fanchouille/data2sqllite
        return