import argparse
import json
import pandas as pd
import matplotlib 
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import os
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--infile', help="the depth_distribution.plot file from bamdst", type=str)
parser.add_argument('-s', '--sample', help="the sample", type=str)
parser.add_argument('-o', '--outdir', help="the outdir", type=str)
args = parser.parse_args()
argv=vars(args)
 
####************** 目标区域测序深度图**********************#####
infile = argv["infile"]
sample = argv["sample"]
outdir = argv["outdir"]
a1 = pd.read_csv(infile,sep='\t',header=None)[0][0:501]
b1 = pd.read_csv(infile,sep='\t',header=None)[2][0:501] * 100

x_values = a1
y_values = b1

plt.plot(x_values,y_values,color='r')

plt.fill_between(x_values, y_values, interpolate=True, color='blue', alpha=0.5)


# 设置图表标题并给坐标轴加上标签
plt.title('Sequence depth  distribution', fontsize=18)
plt.xlabel('Sequence depth', fontsize=14)
plt.ylabel('the fraction of target bases(%)', fontsize=12)

# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=12)

x_major_locator=MultipleLocator(100)
#把x轴的刻度间隔设置为1，并存在变量里
y_major_locator=MultipleLocator(0.5)
#把y轴的刻度间隔设置为10，并存在变量里
ax=plt.gca()
#ax为两条坐标轴的实例
ax.xaxis.set_major_locator(x_major_locator)
#把x轴的主刻度设置为50的倍数
ax.yaxis.set_major_locator(y_major_locator)
#把y轴的主刻度设置为10的倍数
plt.xlim(0,500)
#把x轴的刻度范围设置为-0.5到11，因为0.5不满一个刻度间隔，所以数字不会显示出来，但是能看到一点空白
plt.ylim(0,1.0)

# 设置每个坐标轴的取值范围
#plt.axis([0, 300, 0, 50])
plt.savefig(outdir + "/"  + sample + ".Sequence_depth_distribution.png")
plt.show()
plt.close()

####************** 测序深度累积曲线**********************#####

m1 = pd.read_csv(infile,sep='\t',header=None)[0][0:501]
n1 = pd.read_csv(infile,sep='\t',header=None)[4][0:501] * 100

x_values = m1
y_values = n1

plt.plot(x_values,y_values,color='r')

#plt.fill_between(x_values, y_values, interpolate=True, color='blue', alpha=0.5)


# 设置图表标题并给坐标轴加上标签
plt.title('Cumulative sequence depth  distribution', fontsize=18)
plt.xlabel('Cumulative sequence depth', fontsize=12)
plt.ylabel('the fraction of target bases(%)', fontsize=12)

# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=12)

x_major_locator=MultipleLocator(100)
#把x轴的刻度间隔设置为1，并存在变量里
y_major_locator=MultipleLocator(20)
#把y轴的刻度间隔设置为10，并存在变量里
ax=plt.gca()
#ax为两条坐标轴的实例
ax.xaxis.set_major_locator(x_major_locator)
#把x轴的主刻度设置为50的倍数
ax.yaxis.set_major_locator(y_major_locator)
#把y轴的主刻度设置为10的倍数
plt.xlim(0,500)
#把x轴的刻度范围设置为-0.5到11，因为0.5不满一个刻度间隔，所以数字不会显示出来，但是能看到一点空白
plt.ylim(0,100)

# 设置每个坐标轴的取值范围
#plt.axis([0, 300, 0, 50])
plt.savefig(outdir + "/"  + sample + ".Cumulative_sequence_depth_distribution.png")
plt.show()



