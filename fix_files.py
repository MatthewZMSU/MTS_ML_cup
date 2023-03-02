# This module fixes files

import sys
import fastparquet as fpq
import pandas as pd


def correct_row(broken_row: pd.Series, settlement_types: pd.DataFrame) -> pd.Series:
    short_types = set(settlement_types.index)
    fixed_row = broken_row.copy()

    # "Name2" column does not have any NaN values!
    max_len = 0
    cor_type = None
    for short_type in short_types:
        if fixed_row["Name2"].startswith(short_type) and len(short_type) > max_len:
            cor_type = short_type
    if not (cor_type is None):
        fixed_row["Real_name"] = fixed_row["Name2"][len(cor_type):]
        fixed_row["Real_type"] = settlement_types["New_name"].loc[cor_type]
        return fixed_row
    else:
        fixed_row["Real_name"] = fixed_row["Name2"]

    if fixed_row["SettlementTypeName"] != "":
        fixed_row["Real_type"] = fixed_row["SettlementTypeName"]
    elif fixed_row["SettlementTypeID"] != 0:
        type_id = int(fixed_row["SettlementTypeID"])
        fixed_row["Real_type"] = settlement_types["New_name"].iloc[type_id - 1]
    else:
        if fixed_row["Notes"] != "":
            for short_type in short_types:
                if fixed_row["Notes"].startswith(short_type):
                    fixed_row["Real_type"] = settlement_types["New_name"].loc[short_type]
                    return fixed_row
        fixed_row["Notes"] = "деревня"
    return fixed_row


if __name__ == "__main__":
    answer = input('''What do you want to fix?\n
                   1 - parquet files with given data
                   2 - csv files with settlement types''')
    if answer.strip('\n') == "1":
        for i in range(10):
            file_path = "broken_pqt/" + str(i) + ".parquet"
            broken_pqt = fpq.ParquetFile(file_path)
            tmp_df = broken_pqt.to_pandas()
            print(tmp_df)
            name = "fixed_pqt/data" + str(i) + ".pqt"
            fpq.write(name, tmp_df, compression="ZSTD")
    elif answer.strip('\n') == "2":
        # Here we fix regions:
        regions = pd.read_csv("BrokenSettlements/Regions.csv", sep=';')
        regions = regions.drop(columns=['ID', 'FederalDistrictID', 'FederalDistrictName'])
        regions.to_csv("Settlements/Regions&TableIDs.csv", index=False)
        # Here we fix Settlement types:
        set_types = pd.read_csv("SettlementTypes.csv", sep=';')
        # Change 2-11 IDs to "поселок"
        # Change 12-32 IDs to "деревня"
        set_types["New_name"] = set_types["Name"]
        for i in range(2 - 1, 11):
            set_types.loc[i, "New_name"] = "поселок"
        for i in range(12 - 1, 32):
            set_types.loc[i, "New_name"] = "деревня"
        set_types.rename(columns={"Name": "Old_name"}, inplace=True)
        set_types.to_csv("SettlementTypes.csv", index=False)
        # Here we build "Region-Name-Type" file:
        types = pd.read_csv("Oktmo.csv", sep=';')
        types = types.drop(
            columns=["ID", "Kod", "Kod2", "SubKod2", "SubKod3", "SubKod4", "P1", "P2", "Kch", "FederalDistrictID",
                     "FederalDistrictName", "WhenAdd", "Source"]
        )
        types = types[
            ~(pd.isna(types["Notes"]) & pd.isna(types["SettlementTypeID"]) & pd.isna(types["SettlementTypeName"]))
        ]
        types["SettlementTypeID"].fillna(0, inplace=True)
        types["SettlementTypeName"].fillna("", inplace=True)
        types["Notes"].fillna("", inplace=True)
        types["Real_name"] = types["Name"]
        types["Real_type"] = types["SettlementTypeName"]
        types = types.apply(correct_row, args=(set_types,), axis=1)
        final_types = types[["RegionName", "Real_name", "Real_type"]]
        final_types.sort_values(by=["RegionName"], inplace=True)
        final_types.rename(columns={"Real_name": "SettlementName", "Real_type": "SettlementType"}, inplace=True)
        final_types.to_csv("SetTypes.csv", index=False)
    else:
        sys.exit("Did not understand your request!")
