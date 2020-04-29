import argparse
import json
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument('-i', '--infile', help="the input file bam", type=str)
parser.add_argument('-o', '--outfile', help="the output file", type=str)
args = parser.parse_args()

j = json.load(open(args.infile))

summary = j['summary']

summary = pd.DataFrame({k:pd.Series(v, dtype=object) for k, v in summary.items()})

summary['left_fraction'] = (summary['after_filtering']/summary['before_filtering']).apply(lambda x:f"{x*100:.4f}%")
summary.iloc[2:, 2] = '-'

fractionDf = summary.loc[["q20_rate", "q30_rate", "gc_content"], "before_filtering":"after_filtering"].copy()
for column in fractionDf:
    fractionDf[column] =  fractionDf[column].apply(lambda x:f'{float(x)*100:.4f}%')
summary.loc[["q20_rate", "q30_rate", "gc_content"], "before_filtering":"after_filtering"] = fractionDf

summary.to_csv(args.outfile, sep='\t', index=True)
