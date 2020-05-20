# conding: utf-8
import os.path
import os
import yaml
import sys
#from scripts.utils import getGlobalConfig
import argparse
from argparse import RawTextHelpFormatter
import subprocess
import multiprocessing
#sys.path.insert(0, "")  #根据文件存放的相对路径添加环境变量
from config import config
import datetime

class GATK_HaplotypeCaller():
    """
    class type for GATK HaplotypeCaller utils
    ============================
    shared things: self.java self.gatk self.reference ...
    if knowns changed, must reinstantiate this class
    """
    def __init__(self, input_file: str, out_dir: str, split_chr: str):
        """workdir: outputDir"""

        self.gatk = config.CFG['external_app.gatk']['exec']
        self.reference = config.CFG['database.hg19']['fasta']
        print("gatk:",self.gatk)
        print("reference:",self.reference)
        self.split_chr = split_chr
        self.input_file = input_file
        self.out_dir = out_dir


    def  check_call(self,job_shell,stdout=None, stderr=None):
        subprocess.check_call(job_shell, shell=True, stdout=stdout, stderr=stderr)

    def run_HaplotypeCaller_split(self, input_file: str, out_dir: str,
                              stdout=None, stderr=None):
        """
        HaplotypeCaller split chrom
        """
        #config.makesure_file(in_file)
        #config.makesure_file_basedir(out_file)
        gatk_hc_dir = out_dir
        basename, ext = os.path.splitext(os.path.basename(input_file))
        sample = basename.split(".")[0]  #适用于文件命名是用"."号连接形式，第一个域为样本名的格式

        chroms=['chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9', 'chr10',
                'chr11', 'chr12', 'chr13','chr14', 'chr15', 'chr16', 'chr17', 'chr18', 'chr19',
                'chr20', 'chr21', 'chr22', 'chrX', 'chrY', 'chrM']
        job_work=[]
        for each_chr in chroms:
            java_shell = f"{self.gatk} "\
                f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_hc_dir}' "\
                "HaplotypeCaller  "\
                f"-R  {self.reference}   "\
                "--emit-ref-confidence GVCF   "\
                f"-I {input_file}   "\
                f"-L {each_chr}    "\
                f"-O  {gatk_hc_dir}/{sample}.{each_chr}.g.vcf.gz "
            job_work.append(java_shell)
        print(job_work)
        p = multiprocessing.Pool(12)
        for each_job in job_work:
            p.apply_async(self.check_call, args=(each_job,))
        print('等待所有添加的进程运行完毕。。。')
        p.close()  # 在join之前要先关闭进程池，避免添加新的进程
        p.join()
        print('End!!,PID:%s' % os.getpid())

        variant_par = ""
        for each_chr in  chroms:
            variant_par += f" -I {gatk_hc_dir}/{sample}.{each_chr}.g.vcf.gz "
        print(variant_par)

        java_shell = f"{self.gatk} " \
                     f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_hc_dir}' " \
                    "MergeVcfs  "  \
                    f"{variant_par} -O {gatk_hc_dir}/{sample}.g.vcf.gz "
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)
        return f"{gatk_hc_dir}/{sample}.g.vcf.gz"

    def run_HaplotypeCaller_all(self, input_file: str, out_dir: str,
                              stdout=None, stderr=None):
        """
         HaplotypeCaller split chrom
         """
        # config.makesure_file(in_file)
        # config.makesure_file_basedir(out_file)
        gatk_hc_dir = out_dir
        basename, ext = os.path.splitext(os.path.basename(input_file))
        sample = basename.split(".")[0]  # 适用于文件命名是用"."号连接形式，第一个域为样本名的格式

        chroms = ['chr1', 'chr2', 'chr3', 'chr4', 'chr5', 'chr6', 'chr7', 'chr8', 'chr9', 'chr10',
              'chr11', 'chr12', 'chr13', 'chr14', 'chr15', 'chr16', 'chr17', 'chr18', 'chr19',
              'chr20', 'chr21', 'chr22', 'chrX', 'chrY', 'chrM']
        java_shell = f"{self.gatk} " \
                     f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_hc_dir}' " \
                     "HaplotypeCaller  " \
                     f"-R {self.reference}   " \
                     f"--emit-ref-confidence GVCF   " \
                     f"-I {input_file}   " \
                     f"-O  {gatk_hc_dir}/{sample}.g.vcf.gz "
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)
        return f"{gatk_hc_dir}/{sample}.g.vcf.gz"

    def run(self):
        a = datetime.datetime.now()
        print(self.input_file, self.out_dir, self.split_chr)
        split_chr = self.split_chr
        if split_chr == "yes":
            self.run_HaplotypeCaller_split(self.input_file,self.out_dir)
        else:
            self.run_HaplotypeCaller_all(self.input_file,self.out_dir)
        b = datetime.datetime.now()
        print("HaplotypeCaller 执行时间为 %d" %((b-a).seconds))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="gatk filter manner of VariantRecalibrator or hardflter ", \
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('--input_file', help="input file", required=True)
    parser.add_argument('--out_dir', help="outfile of dir", required=True)
    parser.add_argument('--split_chr',\
                        help="split chrom or not ",required=True, choices=["yes","no"])
    argv = vars(parser.parse_args())
    print(argv)
    input_file = argv['input_file'].strip()
    out_dir = argv['out_dir'].strip()
    split_chr = argv['split_chr'].strip()
    pp = GATK_HaplotypeCaller(input_file, out_dir, split_chr)
    pp.run()



