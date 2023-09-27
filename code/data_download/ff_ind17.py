import sys
sys.path.insert(0, '../')
from functions import *
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
import re


@print_log
def ff_ind17_download():
    url = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes17.zip'
    url_source = urlopen(url)
    zipfile = ZipFile(BytesIO(url_source.read()))
    res = []
    with zipfile.open('Siccodes17.txt') as f:
        for i in f.readlines():
            i = i.decode('utf-8')
            if re.search('^\s?\d', i):
                sic = i.strip().split(' ', 2)[0]
            if re.search('^\s\s\s', i):
                i = i.strip().split('-', 1)
                sic1 = int(i[0])
                sic2 = int(i[1].split(' ', 1)[0])
                sic_name = i[1].split(' ', 1)[1]
                res.append((sic, sic1, sic2, sic_name))

    zipfile.close()
    df = (
        pd.DataFrame(res, columns=['ind17', 'sic1', 'sic2', 'ind_name'])
        .sort_values(['ind17', 'sic1'], ignore_index=True))

    df.to_parquet(download_dir/'ff_ind17.parquet.gzip', compression='gzip')

ff_ind17_download()
