import os.path
import os
import yaml
import sys
from scripts.utils import getGlobalConfig
import argparse
from argparse import RawTextHelpFormatter
import subprocess
import multiprocessing

snakeDir = os.path.dirname(os.path.realpath(__file__))
#snakeFile = os.path.join(snakeDir, 'src', 'Snakefile')
os.environ['snakeDir'] = snakeDir

snakeDir=os.environ['snakeDir']
sys.path.insert(0, snakeDir)
print("snakeDir",snakeDir)
#projectDir=os.path.dirname(snakeDir)
#sample=config['sample']

# global Configs
globalConfig=getGlobalConfig.getGlobalConfig(snakeDir)
procedures=globalConfig['procedures']
softwares=getGlobalConfig.getSoftwares(globalConfig, snakeDir)
scripts=getGlobalConfig.getScripts(globalConfig, snakeDir)
globalFiles=getGlobalConfig.getGlobalFiles(globalConfig, snakeDir)


class GARK():
    """
    class type for GATK utils
    ============================
    shared things: self.java self.gatk self.reference ...
    if knowns changed, must reinstantiate this class

    """
    def __init__(self,raw_snp_file: str, raw_indel_file: str, out_dir: str,filter_type: str):
        """workdir: outputDir"""
        self.gatk = softwares['gatk']
        self.ref = globalFiles['ref']
        self.mi_kg = globalFiles["mi_kg"]
        self.kg_phase1_indel = globalFiles["kg_phase1_indel"]
        self.kg_phase1_snp = globalFiles["kg_phase1_snp"]
        self.dbsnp = globalFiles["dbsnp"]
        self.hapmap = globalFiles["hapmap"]
        self.kg_omni = globalFiles["kg_omni"]
        self.filter_type = filter_type
        self.raw_snp_file = raw_snp_file
        self.raw_indel_file =  raw_indel_file
        self.out_dir = out_dir

    def run_VariantRecalibrator_1(self, raw_snp_file: str, raw_indel_file: str, out_dir: str,
                              stdout=None, stderr=None):
        """
        VariantRecalibrator_snp
        """
        #config.makesure_file(in_file)
        #config.makesure_file_basedir(out_file)
        gatk_vqsr_dir = out_dir
        basename, ext = os.path.splitext(os.path.basename(raw_snp_file))
        sample = basename.split(".")[0]  #适用于文件命名是用"."号连接形式，第一个域为样本名的格式
        snp_recal = os.path.join(gatk_vqsr_dir, sample  + ".snp.recal")
        snp_tranches =  os.path.join(gatk_vqsr_dir, sample  + ".snp.tranches")
        snp_plots_R = os.path.join(gatk_vqsr_dir, sample  + ".snp.plots.R")
        java_shell = f"{self.gatk} "\
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
            f"VariantRecalibrator -R {self.ref} -V  {raw_snp_file} "\
            f"--resource hapmap,known=false,training=true,truth=true,prior=15.0:{self.hapmap} "\
            f"--resource omni,known=false,training=true,truth=false,prior=12.0:{self.kg_omni} "\
            f"--resource 1000G,known=false,training=true,truth=false,prior=10.0:{self.kg_phase1_snp} "\
            f"--resource dbsnp,known=true,training=false,truth=false,prior=2.0:{self.dbsnp} "\
            f"-an DP -an QD -an MQ -an MQRankSum -an ReadPosRankSum -an FS -an SOR "\
            f"-mode SNP -O {snp_recal} "\
            f"--tranches-file {snp_tranches}  "\
            f"--rscript-file {snp_plots_R} "
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)

        indel_recal = os.path.join(gatk_vqsr_dir, sample  + ".indel.recal")
        indel_tranches =  os.path.join(gatk_vqsr_dir, sample  + ".indel.tranches")
        indel_plots_R = os.path.join(gatk_vqsr_dir, sample  + ".indel.plots.R")
        java_shell = f"{self.gatk}  "\
                     f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
                     f"VariantRecalibrator -R {self.ref} -V  {raw_indel_file} "\
                     f"--resource mills,known=false,training=true,truth=true,prior=12.0:{self.mi_kg} "\
                     f"--resource dbsnp,known=true,training=false,truth=false,prior=2.0:{self.dbsnp} "\
                     f"-an DP -an QD -an MQ -an MQRankSum -an ReadPosRankSum -an FS -an SOR "\
                     f"-mode INDEL -O {indel_recal} "\
                     f"--tranches-file {indel_tranches}  "\
                     f"--rscript-file {indel_plots_R} "
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)

        clean_snp = os.path.join(gatk_vqsr_dir, sample + ".clean.snp.vcf.gz")
        #snp_tranches = os.path.join(gatk_vqsr_dir, sample + ".snp.tranches")
        #snp_recal = os.path.join(gatk_vqsr_dir, sample + ".snp.recal")
        java_shell = f"{self.gatk} "\
                     f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
                     f"ApplyVQSR " \
                     f"-R {self.ref} -V {raw_snp_file} "\
                     f"-O  {clean_snp} "\
                     f"-ts-filter-level 99.0 "\
                     f"--tranches-file {snp_tranches} "\
                     f"--recal-file {snp_recal} "\
                     f"-mode SNP "
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)

        clean_indel = os.path.join(gatk_vqsr_dir, sample + ".clean.indel.vcf.gz")
        java_shell = f"{self.gatk} "\
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
            f"ApplyVQSR -R {self.ref} -V {raw_indel_file} "\
            f"-O {clean_indel} "\
            f"--tranches-file  {indel_tranches} "\
            f"--recal-file {indel_recal} -mode INDEL "
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)
        return clean_snp, clean_indel

    def run_VariantRecalibrator_2(self, raw_snp_file: str, raw_indel_file: str, out_dir: str,\
                              stdout=None, stderr=None):
        """
         hardfilter
        """
        gatk_vqsr_dir = out_dir
        basename, ext = os.path.splitext(os.path.basename(raw_snp_file))
        sample = basename.split(".")[0]  #适用于文件命名是用"."号连接形式，第一个域为样本名的格式
        clean_snp = os.path.join(gatk_vqsr_dir, sample + ".clean.snp.vcf.gz")
        para_snp_filter = "QD < 2.0 || MQ < 40.0 || FS > 60.0 || SOR > 3.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0"
        java_shell = f"{self.gatk} " \
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
            f"VariantFiltration -V  {raw_snp_file} "\
            f"--filter-expression '{para_snp_filter}' "\
            f"--filter-name 'Filter_snp' "\
            f"-O  {clean_snp} "
        print(java_shell)
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)

        clean_indel = os.path.join(gatk_vqsr_dir, sample + ".clean.indel.vcf.gz")
        para_indel_filter = "QD < 2.0 || FS > 200.0 || SOR > 10.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0"
        java_shell = f"{self.gatk} "\
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
            f"VariantFiltration "\
            f"-V {raw_indel_file} "\
            f"--filter-expression '{para_indel_filter}' "\
            f"--filter-name 'Filter_indel' "\
            f"-O {clean_indel} "
        print(java_shell)
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)
        return clean_snp, clean_indel

    def run(self) :
        print(raw_snp_file, raw_indel_file, out_dir,filter_type)
        if filter_type == "vqsr":
            self.run_VariantRecalibrator_1(raw_snp_file, raw_indel_file, out_dir)
        else:
            self.run_VariantRecalibrator_2(raw_snp_file, raw_indel_file, out_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="gatk filter manner of VariantRecalibrator or hardflter ", \
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('--raw_snp_file', help="raw snp file", required=True)
    parser.add_argument('--raw_indel_file', help="raw indel file", required=True)
    parser.add_argument('--out_dir', help="outfile of dir", required=True)
    parser.add_argument('--filter_type',\
                        help="more than 30 samples for VariantRecalibrator and less than 30 samples for hardfiter ",\
                        required=True)
    argv = vars(parser.parse_args())
    raw_snp_file = argv['raw_snp_file'].strip()
    raw_indel_file = argv['raw_indel_file'].strip()
    out_dir = argv['out_dir'].strip()
    filter_type = argv['filter_type'].strip()
    pp = GARK(raw_snp_file, raw_indel_file, out_dir, filter_type)
    pp.run()


