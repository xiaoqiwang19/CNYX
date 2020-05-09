import argparse
import json
import pandas as pd
import matplotlib 
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
import os

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--infile', help="the   fastp json file", type=str)
#parser.add_argument('-s', '--sample', help="the sample", type=str)
args = parser.parse_args()
argv=vars(args)
 
####**************raw 绘制质量分布图**********************#####
infile = argv["infile"]
sample = os.path.basename(infile).split(".")[0] ###适用于文件命名是用"."号连接形式，第一个域为样本名的格式
j = json.load(open(infile))
r1_q_mean = j["read1_before_filtering"]["quality_curves"]["mean"]
r2_q_mean = j["read2_before_filtering"]["quality_curves"]["mean"]
q_mean = r1_q_mean + r2_q_mean

x_values = range(1, 301)
y_values = q_mean

'''
scatter() 
x:横坐标 y:纵坐标 s:点的尺寸
'''

plt.scatter(x_values,y_values, s=10)
plt.axvline(150, linestyle="--", linewidth=1, color='r')#99表示横坐标
 
# 设置图表标题并给坐标轴加上标签
plt.title('quality distribution', fontsize=24)
plt.xlabel('reads position', fontsize=14)
plt.ylabel('quality value', fontsize=14)
 
# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=14)
 
# 设置每个坐标轴的取值范围
plt.axis([0, 300, 0, 40])
plt.savefig(sample + ".raw_quality_distribution.png")
plt.show()
plt.close()


####**************clean 绘制质量分布图**********************#####
r1_q_mean = j["read1_after_filtering"]["quality_curves"]["mean"]
r2_q_mean = j["read2_after_filtering"]["quality_curves"]["mean"]
q_mean = r1_q_mean + r2_q_mean

x_values = range(1, 301)
y_values = q_mean

'''
scatter() 
x:横坐标 y:纵坐标 s:点的尺寸
'''

plt.scatter(x_values, y_values, s=10)
plt.axvline(150, linestyle="--", linewidth=1, color='r')  # 99表示横坐标

# 设置图表标题并给坐标轴加上标签
plt.title('quality distribution', fontsize=24)
plt.xlabel('reads position', fontsize=14)
plt.ylabel('quality value', fontsize=14)

# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=14)

# 设置每个坐标轴的取值范围
plt.axis([0, 300, 0, 40])
plt.savefig(sample + ".clean_quality_distribution.png")
plt.show()
plt.close()


###*********************raw 碱基含量分布图**********************************#########

r1_A_c = [float(f'{i*100:.2f}')  for i in j["read1_before_filtering"]["content_curves"]["A"] ]
r1_T_c = [float(f'{i*100:.2f}')  for i in j["read1_before_filtering"]["content_curves"]["T"] ]
r1_G_c = [float(f'{i*100:.2f}')  for i in j["read1_before_filtering"]["content_curves"]["G"] ]
r1_C_c = [float(f'{i*100:.2f}')  for i in j["read1_before_filtering"]["content_curves"]["C"] ]
r1_N_c = [float(f'{i*100:.2f}')  for i in j["read1_before_filtering"]["content_curves"]["N"] ]

r2_A_c = [float(f'{i*100:.2f}')  for i in j["read2_before_filtering"]["content_curves"]["A"] ]
r2_T_c = [float(f'{i*100:.2f}')  for i in j["read2_before_filtering"]["content_curves"]["T"] ]
r2_G_c = [float(f'{i*100:.2f}')  for i in j["read2_before_filtering"]["content_curves"]["G"] ]
r2_C_c = [float(f'{i*100:.2f}')  for i in j["read2_before_filtering"]["content_curves"]["C"] ]
r2_N_c = [float(f'{i*100:.2f}')  for i in j["read1_before_filtering"]["content_curves"]["N"] ]

r_A_c = r1_A_c + r2_A_c
r_T_c = r1_T_c + r2_T_c
r_G_c = r1_G_c + r2_G_c
r_C_c = r1_C_c + r2_C_c
r_N_c = r1_N_c + r2_N_c

x_values = range(1, 301)
a_y_values = r_A_c
t_y_values = r_T_c
g_y_values = r_G_c
c_y_values = r_C_c
n_y_values = r_N_c

#print(x_values,a_y_values)
#print(type(a_y_values[1]))
plt.plot(x_values,a_y_values,color='b',label="A")
plt.plot(x_values,t_y_values,color="g",label="T")
plt.plot(x_values,g_y_values,color="r",label="G")
plt.plot(x_values,c_y_values,color="y",label="C")
plt.plot(x_values,n_y_values,color="purple",label="N")

plt.legend(loc="best",markerscale=2.,numpoints=2,scatterpoints=1,fontsize=8)

plt.axvline(150, linestyle="--", linewidth=1, color='r')#99表示横坐标

# 设置图表标题并给坐标轴加上标签
plt.title('base content distribution', fontsize=24)
plt.xlabel('position along reads', fontsize=14)
plt.ylabel('base percent', fontsize=14)

# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=14)

x_major_locator=MultipleLocator(50)
#把x轴的刻度间隔设置为1，并存在变量里
y_major_locator=MultipleLocator(10)
#把y轴的刻度间隔设置为10，并存在变量里
ax=plt.gca()
#ax为两条坐标轴的实例
ax.xaxis.set_major_locator(x_major_locator)
#把x轴的主刻度设置为50的倍数
ax.yaxis.set_major_locator(y_major_locator)
#把y轴的主刻度设置为10的倍数
plt.xlim(1,300)
#把x轴的刻度范围设置为-0.5到11，因为0.5不满一个刻度间隔，所以数字不会显示出来，但是能看到一点空白
plt.ylim(0,50)

# 设置每个坐标轴的取值范围
#plt.axis([0, 300, 0, 50])
plt.savefig(sample + ".raw_base_content_distribution.png")
plt.show()


###*********************clean 碱基含量分布图**********************************#########

r1_A_c = [float(f'{i*100:.2f}')  for i in j["read1_after_filtering"]["content_curves"]["A"] ]
r1_T_c = [float(f'{i*100:.2f}')  for i in j["read1_after_filtering"]["content_curves"]["T"] ]
r1_G_c = [float(f'{i*100:.2f}')  for i in j["read1_after_filtering"]["content_curves"]["G"] ]
r1_C_c = [float(f'{i*100:.2f}')  for i in j["read1_after_filtering"]["content_curves"]["C"] ]
r1_N_c = [float(f'{i*100:.2f}')  for i in j["read1_after_filtering"]["content_curves"]["N"] ]

r2_A_c = [float(f'{i*100:.2f}')  for i in j["read2_after_filtering"]["content_curves"]["A"] ]
r2_T_c = [float(f'{i*100:.2f}')  for i in j["read2_after_filtering"]["content_curves"]["T"] ]
r2_G_c = [float(f'{i*100:.2f}')  for i in j["read2_after_filtering"]["content_curves"]["G"] ]
r2_C_c = [float(f'{i*100:.2f}')  for i in j["read2_after_filtering"]["content_curves"]["C"] ]
r2_N_c = [float(f'{i*100:.2f}')  for i in j["read1_after_filtering"]["content_curves"]["N"] ]

r_A_c = r1_A_c + r2_A_c
r_T_c = r1_T_c + r2_T_c
r_G_c = r1_G_c + r2_G_c
r_C_c = r1_C_c + r2_C_c
r_N_c = r1_N_c + r2_N_c

x_values = range(1, 301)
a_y_values = r_A_c
t_y_values = r_T_c
g_y_values = r_G_c
c_y_values = r_C_c
n_y_values = r_N_c

#print(x_values,a_y_values)
#print(type(a_y_values[1]))
plt.plot(x_values,a_y_values,color='b',label="A")
plt.plot(x_values,t_y_values,color="g",label="T")
plt.plot(x_values,g_y_values,color="r",label="G")
plt.plot(x_values,c_y_values,color="y",label="C")
plt.plot(x_values,n_y_values,color="purple",label="N")

plt.legend(loc="best",markerscale=2.,numpoints=2,scatterpoints=1,fontsize=8)

plt.axvline(150, linestyle="--", linewidth=1, color='r')#99表示横坐标

# 设置图表标题并给坐标轴加上标签
plt.title('base content distribution', fontsize=24)
plt.xlabel('position along reads', fontsize=14)
plt.ylabel('base percent', fontsize=14)

# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=14)

x_major_locator=MultipleLocator(50)
#把x轴的刻度间隔设置为1，并存在变量里
y_major_locator=MultipleLocator(10)
#把y轴的刻度间隔设置为10，并存在变量里
ax=plt.gca()
#ax为两条坐标轴的实例
ax.xaxis.set_major_locator(x_major_locator)
#把x轴的主刻度设置为50的倍数
ax.yaxis.set_major_locator(y_major_locator)
#把y轴的主刻度设置为10的倍数
plt.xlim(1,300)
#把x轴的刻度范围设置为-0.5到11，因为0.5不满一个刻度间隔，所以数字不会显示出来，但是能看到一点空白
plt.ylim(0,50)

# 设置每个坐标轴的取值范围
#plt.axis([0, 300, 0, 50])
plt.savefig(sample + ".clean_base_content_distribution.png")
plt.show()


####碱基总体质量分布图




