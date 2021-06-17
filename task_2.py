import re
import json

import pandas as pd

import log
from db import move_df_to_db

class DataConverter:
    def __init__(self, path_fo_files):
        self.employee = self._rename_columns(self._read_file(path_fo_files[0], level=0))
        self.departments = self._rename_columns(self._read_file(path_fo_files[1]))
        self.dummy = self._get_dummy_employee()
        self.employee = self.employee.loc[
            ~self.employee['rid'].isin(self.dummy['rid'].tolist())
        ]
        self.valid_values = self.find_index_of_valid_rows()
        self._report_invalid_rows_rid()
        self.employee = self._rename_columns(self._read_file(path_fo_files[0])).loc[self.valid_values]

        # self.dummy_copy = self.dummy.copy()
        # self.dummy_copy.comment = ""
        # self.employee = pd.concat([self.employee.copy(), self.dummy_copy.copy()])
        self.employee = self._create_unique_id(self.employee.copy(), 'employee_id')
        self.departments = self._create_unique_id(self.departments.copy(), "department_id")
        self.employee = self.replace_strings_to_id()
        self.departments = self.departments.drop(columns=['rid'])

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

    def _read_file(self, path_to_file, level=None):
        with open(path_to_file) as data_file:
            data = json.load(data_file)['result']

        df = pd.json_normalize(data, max_level=level)
        if '@class' in df.columns:
            return df.drop(columns=['@class'])
        return df

    def _create_mappers(self):
        rid_mapper = dict(zip(self.departments.rid, self.departments.department_id))
        name_mapper = dict(zip(self.departments.name, self.departments.department_id))

        return rid_mapper, name_mapper

    @staticmethod
    def _create_unique_id(df, col_name):
        original_names = df['name'].unique()
        new_ids = {cid: indx+100 for indx, cid in enumerate(original_names)}
        df[col_name] = df['name'].map(new_ids)
        return df

    def _get_dummy_employee(self):
        dummy_df = self.employee.iloc[self.employee.query('name == "DummyEmployee"').index].fillna(0)
        return dummy_df

    def find_index_of_valid_rows(self):
        masker = self.employee[['department_1', 'department_2', 'department_3']].applymap(
            lambda x: tuple(x.values()) if type(x) == dict else x)
        res = self.employee[masker.isin(list(self.departments["rid"].values)) | masker.isin(
            list(map(tuple, self.departments[["name", "comment"]].values.tolist())))]

        return res[['department_1', 'department_2', 'department_3']].dropna().index

    def _report_invalid_rows_rid(self):
        invalid_values = self.employee.drop(self.valid_values)
        log.info(f"Invalid @rid's: {invalid_values.rid.to_list()}")

    def replace_strings_to_id(self):
        df = self.employee.copy()
        rid_mapper, name_mapper = self._create_mappers()
        df['department_1'] = df['department_1'].map(rid_mapper)
        df['department_2'] = df['department_2'].map(rid_mapper)
        df['department_3'] = df['department_3'].map(rid_mapper)
        df['department_1_name'] = df['department_1_name'].map(name_mapper)
        df['department_2_name'] = df['department_2_name'].map(name_mapper)
        df['department_3_name'] = df['department_3_name'].map(name_mapper)

        df = df.fillna(0)
        df['department_1'] = (df['department_1_name'] + df['department_1']).astype('int32')
        df['department_2'] = (df['department_2_name'] + df['department_2']).astype('int32')
        df['department_3'] = (df['department_3_name'] + df['department_3']).astype('int32')

        drop_cols = df.columns.str.contains("_name|_comment")
        df = df.loc[:, ~drop_cols].drop(columns=["rid"])

        return df


if __name__ == "__main__":
    log.init()
    path_to_file_1 = "test_tasks/python_task_2_1_employees.json"
    path_to_file_2 = "test_tasks/python_task_2_2_departments.json"
    conv = DataConverter([path_to_file_1, path_to_file_2])
    # pd.set_option('display.max_columns', None)

    move_df_to_db(conv.departments, "second_task", "department")
    move_df_to_db(conv.employee, "second_task", "employee")
