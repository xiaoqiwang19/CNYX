# conding: utf-8
import os.path
import os
import yaml
import sys
import argparse
from argparse import RawTextHelpFormatter
import subprocess
import multiprocessing
from config import config
import datetime

class GATK_CombineGVCFs():
    """
    class type for GATK CombineGVCFs
    ============================
    shared things: self.java self.gatk self.reference ...
    """
    def __init__(self, sample_all: list, out_dir: str):
        """workdir: outputDir"""

        self.gatk = config.CFG['external_app.gatk']['exec']
        self.reference = config.CFG['database.hg19']['fasta']
        self.sample_all = sample_all
        self.out_dir = out_dir


    def  check_call(self,job_shell,stdout=None, stderr=None):
        subprocess.check_call(job_shell, shell=True, stdout=stdout, stderr=stderr)

    def run_CombineGVCFs(self, sample_all: list, out_file: str,
                              stdout=None, stderr=None):
        """
         CombineGVCFs
        """
        # config.makesure_file(in_file)
        # config.makesure_file_basedir(out_file)
        gatk_CombineGVCFs_file = out_file
        gatk_call_dir = os.path.split(out_file)[0]
        #sample = basename.split(".")[0]  # 适用于文件命名是用"."号连接形式，第一个域为样本名的格式
        vari_all_para_list = []
        for each in sample_all:
            vari_all_para_list.append(f'--variant 04.GATK_CALL/{each}/{each}.g.vcf.gz ')
        vari_all_para = " ".join(vari_all_para_list)

        java_shell = f"{self.gatk} " \
                     f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_call_dir}' " \
                     "CombineGVCFs  " \
                     f"-R {self.reference}   " \
                     f"{vari_all_para}" \
                     f"-O  {gatk_CombineGVCFs_file} "
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)
        return f"{gatk_CombineGVCFs_file}"

    def run(self):
        a = datetime.datetime.now()

        self.run_CombineGVCFs(self.sample_all,self.out_dir)
        b = datetime.datetime.now()
        print("CombineGVCFs 执行时间为 %d" %((b-a).seconds))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GATK CombineGVCFs ", formatter_class=RawTextHelpFormatter)
    parser.add_argument('--sample_all', help="list of all sample", required=True)
    parser.add_argument('--out_file', help="outfile of all sample CombineGVCFs", required=True)
    argv = vars(parser.parse_args())
    sample_all = argv['sample_all'].strip()
    out_file = argv['out_file'].strip()
    sample_all = sample_all.split(",")
    pp = GATK_CombineGVCFs(sample_all, out_file)
    pp.run()



