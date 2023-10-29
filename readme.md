# anomalies-replication-python

This repository is a replication of the work of **Chen and Zimmermann
(2022)**, which highlights the successful replication of anomalies.
Their project provides valuable data on firm characteristics and
portfolio returns. And many reserachers are using their data in
academic research. For the original study and more information, please
refer to their project of [Open Source Cross-Sectional Asset
Pricing](https://github.com/OpenSourceAP/CrossSection).

- Everything in **Python** (Chen and Zimmermann use **Stata** and
  **R**)
- Firm characteristics are in **parquet** format
  - Faster read and write speed
  - Smaller file size
  - For R users, to read **parquet** data, you can install **arrow**
  package `install.packages("arrow")`
  - For Stata users, to read **parquet** data, see this guidance:
    https://github.com/mcaceresb/stata-parquet#examples
- 96 different ways to compute portfolio returns ([see example in step
  3 of How to use](#3.-Portfolio-returns))
- Easy to update firm characteristics and portfolio returns

## Replication process

I have followed the code structure and firm characteristic definitions
outlined in Chen and Zimmermann's work to replicate various anomalies
in Python. Specifically, I have replicated anomalies that rely on CRSP
or Compustat data. Please note that anomalies requiring data from
other sources, such as IBES or OptionMetrics, have not been included
in this replication.

There are a total of 138 anomalies. To view the complete list of
anomalies, please refer to the `predictors.csv` file.

> [!WARNING]
> - The code has been tested on *macOS Sonoma version 14.1* with *Python
3.10.13*.
> - And it is easy to run the bash files in any Linux system.
> - Windows users should use with cautions (I do not have windows
> enviornment to test it)

## How to use

### 0_a. Required packages

Before you proceed to run the code, please ensure that you have the
following packages installed (*the versions used for testing are shown
in parentheses*).

- numpy (1.26.0)
- scipy (1.11.3)
- pandas (2.1.2)
- pandas-datareader (0.10.0)
- wrds (3.1.6)
- python-duckdb (0.9.1)
- statsmodels (0.14.0)
- joblib (1.3.2)
- pandarallel (1.6.5)

### 0_b. Clone or downlaod the repo

You can obtain the code in two ways:

**Option 1:** Clone the repository using the following command:

```bash
git clone https://github.com/mk0417/anomalies-replication-python.git
```

**Option 2:** Download the ZIP file by following these steps:

- Click on the "Code" button at the top right with a green background.
- Select "Download ZIP."
- Unzip the downloaded file.

### 0_c. WRDS access
> [!IMPORTANT]
> Data is extracted from WRDS cloud and you need to
> change the *WRDS username*
> - Open the file of `functions.py` under `code` directiry
> - Move to line 32
> - Replace <u><i>change to your> wrds username</i></u> with your
> WRDS username (keep the quotation mark)

### 1. Download data 

To download the necessary datasets, navigate to the `data_download`
folder (under the folder of `code`) in your terminal and execute the
following command:

```bash
sh run_download.sh
```

> [!IMPORTANT]
> - A prompt will appear and you nedd to type your WRDS credentials 
> - Answer **y** in the prompt to create .pgpass file

This will download all required datasets. A folder named
`data_download` (under the folder of `data`) will be automatically
created, and all raw data will be saved in this directory.

### 2. Generate firm characteristics

To generate firm characteristics, navigate to the `predictors` folder
(under the folder of `code`) in your terminal and run the following
command:

```bash
sh run_predictors.sh
```

This process will create a folder named `predictors` under the `data`
folder, and it will store firm characteristics in this directory.

#### Convert parquet to csv (optional)
To convert parquet data to csv files, navigate to the folder of `code`
in your terminal and run the following command:
 
```bash
python convert_to_csv.py
```

A folder named `csv` will be created under `data/predictors`, and all
csv files are stored in the directory.

If you just need firm characteristics, then you can ignore step 3.

### 3. Portfolio returns

For portfolio returns, navigate to the `portfolios` folder (under the
folder of `code`) in your terminal and run the following command:

```bash
sh run_port_ret.sh
```

This will compute portfolio returns, including long-minus-short
returns. A folder named `portfolios` under the `data` directory will
be automatically generated, and the returns will be stored there.
Long-short returns are determined based on the characteristic-return
relation. For instance, if asset growth and return are negatively
associated, the long-short return for decile portfolios will be
calculated as port1 minus port10.

There are various methods available for calculating portfolio returns,
such as:
- `ew_all_port5`: equal-weighted portfolios & break points based on
  all stocks & quintile portfolios
- `vw_nyse_port10_price5`: value-weighted portfolios & break points
  based on NYSE stocks & decile portfolios & keep stocks if price>=5
- `nodlst_ew_all_port10_price5_ex_fin`: returns are not delisting
  adjusted & equal-weighted & break points based on all stocks &
  decile portfolios & keep stocks if price>=5 & exclude financial
  industry

## Credit
Thanks Chen and Zimmermann for their contributions to open-source
asset pricing research.
