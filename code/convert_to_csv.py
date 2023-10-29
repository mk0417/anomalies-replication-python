from functions import *


def convert_to_csv(out_dir):
    for i in out_dir.rglob('*.gzip'):
        if not os.path.exists(i.parent/'csv'):
            os.makedirs(i.parent/'csv')

        (pd.read_parquet(i)
         .to_csv(
             i.parent/'csv'/str(i.name).replace('parquet.gzip', 'csv'),
             index=False))

convert_to_csv(Path('../data/predictors'))
