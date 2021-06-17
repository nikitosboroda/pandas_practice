import re
import json

import pandas as pd

from db import move_df_to_db


class DataConverter:
    def __init__(self, list_of_files):
        self.df_1 = self._read_files(list_of_files[0])
        self.df_2 = self._read_files(list_of_files[1])
        self.df_3 = self._read_files(list_of_files[2])

    def _read_files(self, file_path):
        with open(file_path) as data_file:
            data = json.load(data_file)['result']

        df = pd.json_normalize(data)
        df = self._rename_columns(df)
        df = df.drop_duplicates(subset=["name"], keep="last")
        return df

    @staticmethod
    def _rename_columns(df):
        main_df = df.rename(
            columns={
                col: "_".join(
                    re.findall("[a-zA-Z]+_?[0-9]*", col)
                ).lower()
                for col in df.columns
            }
        )
        return main_df

    @staticmethod
    def merge_dfs(df_1, df_2):
        result = df_1.merge(df_2, on='name', how='outer', suffixes=["", "_drop"])
        drop_cols = result.columns.str.contains("_drop$")
        result = (result
                  .loc[:, ~drop_cols]
                  .fillna(result.loc[:, drop_cols]
                          .rename(columns=lambda c: c.replace("_drop", ""))))
        return result

    def create_one_df(self):
        df_1_2 = self.merge_dfs(self.df_1, self.df_2)
        full_df = self.merge_dfs(df_1_2, self.df_3)

        return full_df[~full_df.name.str.lower().str.contains("dummy")].drop(columns=['class'])


if __name__ == '__main__':
    converter = DataConverter(
        [
            "test_tasks/python_task_3_1_emps.json",
            "test_tasks/python_task_3_2_emps.json",
            "test_tasks/python_task_3_3_emps.json"
        ]
    )

    result_df = converter.create_one_df()
    move_df_to_db(result_df, "third_task", "employee")
