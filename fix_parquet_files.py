# This module fixes parquet files

import fastparquet as fpq
import pandas as pd

if __name__ == "__main__":
    for i in range(10):
        file_path = "broken_pqt/" + str(i) + ".parquet"
        broken_pqt = fpq.ParquetFile(file_path)
        tmp_df = broken_pqt.to_pandas()
        print(tmp_df)
        name = "fixed_pqt/data" + str(i) + ".pqt"
        fpq.write(name, tmp_df, compression="ZSTD")
