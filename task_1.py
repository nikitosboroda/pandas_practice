import re
import json

import pandas as pd

from db import move_df_to_db


class DataConverter:
    def __init__(self, path_to_file):
        self.main_df = self._convert_columns_names(self._read_file(path_to_file)).iloc[1:]
        self.main_df.department_id = self.correct_types()
        self.employees_df = self._get_employee_table()
        self.department_df = self._get_departments_table()
        self.synonyms_df = self.department_df[['department_synonyms', 'department_id']]
        self.department_df = self.department_df.drop(columns=['department_synonyms'])
        self.synonyms_df.department_synonyms = self.synonyms_df.department_synonyms.apply(lambda x: ','.join(x) if type(x) != float else '')

    @staticmethod
    def _transform_to_df(json_data):
        return pd.json_normalize(json_data)

    @staticmethod
    def _convert_columns_names(df):
        main_df = df.rename(
            columns={
                col: "_".join(
                    re.findall("[a-zA-Z]+", col)
                ).lower()
                for col in df.columns
            }
        )
        return main_df

    def _read_file(self, path_to_file):
        with open(path_to_file) as file:
            data = json.load(file)['result']
        return self._transform_to_df(data)

    def correct_types(self):
        dep_id = self.main_df.department_id.copy().fillna(0)
        return dep_id.astype("int32")

    def _get_employee_table(self):
        employees_df = self.main_df.loc[:, :"department_id"].copy()
        return employees_df.drop(columns=['class'])

    def _get_departments_table(self):
        columns = [col for col in self.main_df.columns if col.startswith("depart")]
        departments_df = self.main_df.groupby(["department_id"]).apply(
            lambda x: x.iloc[x["rid"].values.argmax()]
        )[columns]
        departments_df = departments_df.reset_index(drop=True)
        return departments_df


if __name__ == '__main__':
    converter = DataConverter("test_tasks/python_task_1_emp_dep_embedded.json")
    move_df_to_db(converter.department_df, "first_task", "department")
    move_df_to_db(converter.employees_df, "first_task", "employee")
    move_df_to_db(converter.synonyms_df, "first_task", "synonyms")
