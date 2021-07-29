import numpy as np
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from argparse import ArgumentParser


def main(filepath: Path):
    # Open dataframe
    kw_args = {
        'parse_dates': ['dateRep'],
        'infer_datetime_format': True,
        'dayfirst': True,
    }
    df = pd.read_csv(filepath, **kw_args)

    # Extract all the dates
    dates = df['dateRep'].unique()

    step = 50
    for limit in tqdm(range(step, dates.size + step, step)):
        # Only keep records within the first {limit} days
        df_cut = df[df['dateRep'].isin(dates[:limit])]
        n_records = df_cut['dateRep'].count()
        # Write out to file
        outpath = filepath.parent.joinpath(f'{filepath.stem}_{n_records}.csv')
        df_cut.to_csv(outpath, index=False, header=True,
                      date_format='%d/%m/%Y')


def add_arguments(parser: ArgumentParser):
    parser.add_argument(
        'filepath',
        metavar='FILE',
        type=Path,
        help='path to the CSV file with data in ECDC format'
    )


if __name__ == '__main__':
    # Parse CLI arguments
    parser = ArgumentParser(
        description='Produce different versions of the ECDC dataset with progressively more dates')
    add_arguments(parser)
    args = parser.parse_args()

    main(args.filepath)
