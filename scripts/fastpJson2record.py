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

summary['left_fraction'] = (summary['after_filtering']/summary['before_filtering']).apply(lambda x:f"{x*100:.2f}%")
summary.iloc[2:, 2] = '-'

fractionDf = summary.loc[["q20_rate", "q30_rate", "gc_content"], "before_filtering":"after_filtering"].copy()
for column in fractionDf:
    fractionDf[column] =  fractionDf[column].apply(lambda x:f'{float(x)*100:.2f}%')
summary.loc[["q20_rate", "q30_rate", "gc_content"], "before_filtering":"after_filtering"] = fractionDf

summary.to_csv(args.outfile, sep='\t', index=True)

###读取fastp json 结果中的filtering_result部分(低质量，含N，太短，太长reads)
# d = {'one': pd.Series([1., 2., 3.], index=['a', 'b', 'c'])]}
filtering_result = j["filtering_result"]
fil_index,fil_res=[],[]
for a,b in filtering_result.items():
    fil_index.append(a)
    fraction = f"{b/j['summary']['before_filtering']['total_reads']*100:.2f}%"
    fil_res.append(str(b) + "(" + str(fraction) + ")" )
d = {'filtering_result': pd.Series(fil_res, fil_index)}
df_filtering_result = pd.DataFrame(d)
df_filtering_result.to_csv(args.outfile, sep='\t', mode='a', index=True)


####读取fastp json 结果中的duplication部分
dup_result = j["duplication"]
d = {'duplication_result': pd.Series([f"{dup_result['rate']*100:.2f}%"], index=["dup_rate"]) }
df_dup_result = pd.DataFrame(d)
df_dup_result.to_csv(args.outfile, sep='\t', mode='a',index=True, header=0)  #追加模式写入

####读取fastp json 结果中的adapter_cutting部分
ada_result = j["adapter_cutting"]
adapter_trimmed_reads = ada_result['adapter_trimmed_reads']
adapter_trimmed_bases = ada_result['adapter_trimmed_bases']
adapter_trimmed_reads_fraction =  adapter_trimmed_reads/j['summary']['before_filtering']['total_reads']
adapter_trimmed_bases_fraction =  adapter_trimmed_bases/j['summary']['before_filtering']['total_bases']
#v = [adapter_trimmed_reads, adapter_trimmed_bases,
#     f"{adapter_trimmed_reads_fraction*100:.2f}%",f"{adapter_trimmed_bases_fraction*100:.2f}%"]
#k = ["adapter_trimmed_reads", "adapter_trimmed_bases", "adapter_trimmed_reads_fraction", "adapter_trimmed_bases_fraction"]
ad_t_r = str(adapter_trimmed_reads) + "(" +  str(f"{adapter_trimmed_reads_fraction*100:.2f}%") +  ")"
ad_t_b = str(adapter_trimmed_bases) + "(" +  str(f"{adapter_trimmed_bases_fraction*100:.2f}%") +  ")"
v = [ad_t_r ,ad_t_b]
k = ["adapter_trimmed_reads", "adapter_trimmed_bases"]
d = {'adapter_cutting_result': pd.Series(v, index=k) }
df_ada_result = pd.DataFrame(d)
df_ada_result.to_csv(args.outfile, sep='\t', mode='a',index=True,header=0)  #追加模式写入

####读取fastp json 结果中的 部分







