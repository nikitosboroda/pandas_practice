import re
import json

import pandas as pd

from db import move_df_to_db


class DataConverter:
    def __init__(self, path_to_file):
        self.main_df = self._rename_columns(self._read_file(path_to_file))
        self.depart_df = self.main_df.drop(
            columns=["rid", "sub__department", "comment"]
        )
        self.sub_df = self.main_df[['sub__department', 'name']].copy().explode("sub__department")
        self.departments_names = self.sub_df.name.unique()

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

    @staticmethod
    def set_up_order(df):
        mapper = {name: value for value, name in enumerate(sorted(df.name_sub))}

        if (mapper.get("B", 0) != 0) and mapper.get("A") == 0:
            mapper["B"], mapper["A"] = mapper["A"], mapper["B"]
        df["order_"] = df.name_sub.map(mapper)
        return df.sort_values("order_")

    def create_and_save_subdepartments_dfs(self):
        for name in self.departments_names:
            sub_dep_df = self.sub_df.query(f"name == '{name}'").copy()
            sub_dep_df["name_sub"] = sub_dep_df.sub__department.apply(lambda x: x.get("NAME"))
            sub_dep_df["desc_sub"] = sub_dep_df.sub__department.apply(lambda x: x.get("DESCRIPTION"))
            sub_dep_df = sub_dep_df.drop(columns=["sub__department"])

            sub_dep_df = self.set_up_order(sub_dep_df)

            move_df_to_db(sub_dep_df, "forth_task", "subdepartments")


if __name__ == '__main__':
    converter = DataConverter("test_tasks/python_task_4_departments.json")
    move_df_to_db(converter.depart_df, "forth_task", "departments")
    converter.create_and_save_subdepartments_dfs()
