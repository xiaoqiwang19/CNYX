import os.path
import os
import yaml
import sys
import argparse
from argparse import RawTextHelpFormatter
import subprocess
import multiprocessing
from config import config


class GARK():
    """
    class type for GATK utils
    ============================
    shared things: self.java self.gatk self.reference ...
    if knowns changed, must reinstantiate this class

    """
    def __init__(self,sample: str,filter_type: str):
        """workdir: outputDir"""
        self.gatk = config.CFG['external_app.gatk']['exec']
        self.ref = config.CFG['database.hg19']['fasta']
        self.mi_kg =  config.CFG['database.annotation']['Mills_and_1000G_gold_standard.indels.hg19.sites.vcf.gz']
        self.kg_phase1_indel = config.CFG['database.annotation']['1000G_phase1.indels.hg19.sites.vcf.gz']
        self.kg_phase1_snp = config.CFG['database.annotation']['1000G_phase1.snps.high_confidence.hg19.sites.vcf.gz']
        self.dbsnp = config.CFG['database.annotation']['dbsnp_138.hg19.vcf.gz']
        self.hapmap = config.CFG['database.annotation']['hapmap_3.3.hg19.sites.vcf.gz']
        self.kg_omni = config.CFG['database.annotation']['1000G_omni2.5.hg19.sites.vcf.gz']
        self.filter_type = filter_type
        self.raw_snp_file = raw_snp_file
        self.raw_indel_file =  raw_indel_file
        self.out_dir = out_dir
        #把该脚本写到总流程配置文件中
        self.gatk_multiple_sample_CombineGVCFs = "/public/wangxiaoqi/WES_PIPLINE/snakemake_dir/scripts/gatk_multiple_sample_CombineGVCFs.py"

    #VQSR
    def run_VariantRecalibrator_1(self, sample_list: str,  stdout=None, stderr=None):
        """
        CombineGVCFs  GenotypeGVCFs  SelectVariants VariantRecalibrator  ApplyVQSR
        """
        CombineGVCFs_vcf = "04.GATK_CALL/ALL/allsample.cohort.g.vcf.gz"
        CombineGVCFs_shell=f"python {gatk_multiple_sample_CombineGVCFs}  " \
                               f"--sample_all {sample_list} " \
                               f"--out_file {CombineGVCFs_vcf}"
        print(CombineGVCFs_shell)
        subprocess.check_call(CombineGVCFs_shell, shell=True, stdout=stdout, stderr=stderr)

        para04 = '"-Xmx2G -Djava.io.tmpdir=04.GATK_CALL/ALL"'
        GenotypeGVCFs_vcf = "04.GATK_CALL/ALL/allsample.cohort.raw.vcf.gz"
        GenotypeGVCFs_shell=f"{self.gatk}  --java-options  {para04}  GenotypeGVCFs " \
                            f"-R {self.ref} -V {CombineGVCFs_vcf}  -O  {GenotypeGVCFs_vcf}"
        print(GenotypeGVCFs_shell)
        subprocess.check_call(GenotypeGVCFs_shell, shell=True, stdout=stdout, stderr=stderr)

        para05='"-Xmx2G -Djava.io.tmpdir=05.GATK_SELECT/ALL"'
        SelectVariants_snp_vcf = "05.GATK_SELECT/ALL/allsample.cohort.raw.snp.vcf.gz"
        SelectVariants_snp_shell=f"{self.gatk}  --java-options {para05}  SelectVariants " \
                                 f"-R {self.ref} -V {GenotypeGVCFs_vcf}  -select-type SNP -O {SelectVariants_snp_vcf}"
        print(SelectVariants_snp_shell)
        subprocess.check_call(SelectVariants_snp_shell, shell=True, stdout=stdout, stderr=stderr)

        SelectVariants_indel_vcf = "05.GATK_SELECT/ALL/allsample.cohort.raw.indel.vcf.gz"
        SelectVariants_indel_shell=f"{self.gatk}  --java-options {para05}  SelectVariants -R {self.ref} " \
                                   f"-V {GenotypeGVCFs_vcf}  -select-type INDEL -O {SelectVariants_indel_vcf} "
        print("SelectVariants_indel_shell")
        subprocess.check_call(SelectVariants_indel_shell, shell=True, stdout=stdout, stderr=stderr)

        #config.makesure_file(in_file)
        #config.makesure_file_basedir(out_file)
        gatk_vqsr_dir = "06.GATK_FILTER/ALL"
        sample="allsample.cohort"
        snp_recal = os.path.join(gatk_vqsr_dir, sample  + ".snp.recal")
        snp_tranches =  os.path.join(gatk_vqsr_dir, sample  + ".snp.tranches")
        snp_plots_R = os.path.join(gatk_vqsr_dir, sample  + ".snp.plots.R")
        VariantRecalibrator_SNP_shell = f"{self.gatk} "\
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
            f"VariantRecalibrator -R {self.ref} -V  {SelectVariants_snp_vcf} "\
            f"--resource hapmap,known=false,training=true,truth=true,prior=15.0:{self.hapmap} "\
            f"--resource omni,known=false,training=true,truth=false,prior=12.0:{self.kg_omni} "\
            f"--resource 1000G,known=false,training=true,truth=false,prior=10.0:{self.kg_phase1_snp} "\
            f"--resource dbsnp,known=true,training=false,truth=false,prior=2.0:{self.dbsnp} "\
            f"-an DP -an QD -an MQ -an MQRankSum -an ReadPosRankSum -an FS -an SOR "\
            f"-mode SNP -O {snp_recal} "\
            f"--tranches-file {snp_tranches}  "\
            f"--rscript-file {snp_plots_R} "
        print(VariantRecalibrator_SNP_shell)
        subprocess.check_call(VariantRecalibrator_SNP_shell, shell=True, stdout=stdout, stderr=stderr)

        indel_recal = os.path.join(gatk_vqsr_dir, sample  + ".indel.recal")
        indel_tranches =  os.path.join(gatk_vqsr_dir, sample  + ".indel.tranches")
        indel_plots_R = os.path.join(gatk_vqsr_dir, sample  + ".indel.plots.R")
        VariantRecalibrator_INDEL_shell = f"{self.gatk}  "\
                     f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
                     f"VariantRecalibrator -R {self.ref} -V  {SelectVariants_indel_vcf} "\
                     f"--resource mills,known=false,training=true,truth=true,prior=12.0:{self.mi_kg} "\
                     f"--resource dbsnp,known=true,training=false,truth=false,prior=2.0:{self.dbsnp} "\
                     f"-an DP -an QD -an MQ -an MQRankSum -an ReadPosRankSum -an FS -an SOR "\
                     f"-mode INDEL -O {indel_recal} "\
                     f"--tranches-file {indel_tranches}  "\
                     f"--rscript-file {indel_plots_R} "
        print(VariantRecalibrator_INDEL_shell)
        subprocess.check_call(VariantRecalibrator_INDEL_shell, shell=True, stdout=stdout, stderr=stderr)

        clean_snp = os.path.join(gatk_vqsr_dir, sample + ".clean.snp.vcf.gz")
        ApplyVQSR_SNP_shell = f"{self.gatk} "\
                     f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
                     f"ApplyVQSR " \
                     f"-R {self.ref} -V {SelectVariants_snp_vcf} "\
                     f"-O  {clean_snp} "\
                     f"-ts-filter-level 99.0 "\
                     f"--tranches-file {snp_tranches} "\
                     f"--recal-file {snp_recal} "\
                     f"-mode SNP "
        print(ApplyVQSR_SNP_shell)
        subprocess.check_call(ApplyVQSR_SNP_shell, shell=True, stdout=stdout, stderr=stderr)

        clean_indel = os.path.join(gatk_vqsr_dir, sample + ".clean.indel.vcf.gz")
        ApplyVQSR_INDEL_shell = f"{self.gatk} "\
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_vqsr_dir}' "\
            f"ApplyVQSR -R {self.ref} -V {SelectVariants_indel_vcf} "\
            f"-O {clean_indel} "\
            f"--tranches-file  {indel_tranches} "\
            f"--recal-file {indel_recal} -mode INDEL "
        print(ApplyVQSR_INDEL_shell)
        subprocess.check_call(ApplyVQSR_INDEL_shell, shell=True, stdout=stdout, stderr=stderr)

        para06='"-Xmx2G -Djava.io.tmpdir=06.GATK_FILTER/ALL"'
        clean_vcf = "06.GATK_FILTER/ALL/allsample.cohort.clean.vcf.gz"
        MergeVcfs_shell = f"{self.gatk}  --java-options {para06} MergeVcfs -I {clean_snp} " \
                          f"-I {clean_indel}  -O {clean_vcf} "
        print(MergeVcfs_shell)
        subprocess.check_call(MergeVcfs_shell)
        return clean_vcf

    ############################hard_  filter###########################################
    def run_VariantRecalibrator_2(self, sample: str, stdout=None, stderr=None):
        """
        GenotypeGVCFs  SelectVariants VariantRecalibrator  ApplyVQSR
        """
        gatk_call_dir = os.path.join("04.GATK_CALL",sample)
        g_vcf = os.path.join(gatk_call_dir,sample + ".g.vcf.gz")
        GenotypeGVCFs_vcf = os.path.join(gatk_call_dir,sample + ".raw.vcf.gz")
        GenotypeGVCFs_shell=f"{self.gatk}  " \
                            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_call_dir}' " \
                            f"  GenotypeGVCFs  " \
                            f"-R {self.ref} -V {g_vcf}  -O  {GenotypeGVCFs_vcf}"
        print(GenotypeGVCFs_shell)
        subprocess.check_call(GenotypeGVCFs_shell, shell=True, stdout=stdout, stderr=stderr)

        gatk_SELECT_dir = os.path.join("05.GATK_SELECT",sample)
        SelectVariants_snp_vcf = os.path.join(gatk_SELECT_dir,sample + ".raw.snp.vcf.gz")
        SelectVariants_snp_shell=f"{self.gatk}  --java-options '-Xmx2G -Djava.io.tmpdir={gatk_SELECT_dir}' " \
                                 f"SelectVariants " \
                                 f"-R {self.ref} -V {GenotypeGVCFs_vcf}  -select-type SNP -O {SelectVariants_snp_vcf}"
        print(SelectVariants_snp_shell)
        subprocess.check_call(SelectVariants_snp_shell, shell=True, stdout=stdout, stderr=stderr)

        SelectVariants_indel_vcf = os.path.join(gatk_SELECT_dir,sample + ".raw.indel.vcf.gz")
        SelectVariants_indel_shell=f"{self.gatk}  --java-options '-Xmx2G -Djava.io.tmpdir={gatk_SELECT_dir}'" \
                                   f"  SelectVariants -R {self.ref} " \
                                   f"-V {GenotypeGVCFs_vcf}  -select-type INDEL -O {SelectVariants_indel_vcf} "
        print("SelectVariants_indel_shell")
        subprocess.check_call(SelectVariants_indel_shell, shell=True, stdout=stdout, stderr=stderr)

        gatk_filter_dir = os.path.join("06.GATK_FILTER",sample)
        clean_snp = os.path.join(gatk_filter_dir, sample + ".clean.snp.vcf.gz")
        para_snp_filter = "QD < 2.0 || MQ < 40.0 || FS > 60.0 || SOR > 3.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0"
        snp_hardfilter_shell = f"{self.gatk} " \
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_filter_dir}' "\
            f"VariantFiltration -V  {SelectVariants_snp_vcf} "\
            f"--filter-expression '{para_snp_filter}' "\
            f"--filter-name 'Filter_snp' "\
            f"-O  {clean_snp} "
        print(java_shell)
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)

        clean_indel = os.path.join(gatk_filter_dir, sample + ".clean.indel.vcf.gz")
        para_indel_filter = "QD < 2.0 || FS > 200.0 || SOR > 10.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0"
        indel_hardfilter_shell = f"{self.gatk} "\
            f"--java-options '-Xmx2G -Djava.io.tmpdir={gatk_filter_dir}' "\
            f"VariantFiltration "\
            f"-V {SelectVariants_indel_vcf} "\
            f"--filter-expression '{para_indel_filter}' "\
            f"--filter-name 'Filter_indel' "\
            f"-O {clean_indel} "
        print(java_shell)
        subprocess.check_call(java_shell, shell=True, stdout=stdout, stderr=stderr)


        clean_vcf = os.path.join(gatk_filter_dir, sample + ".clean.vcf.gz")
        MergeVcfs_shell = f"{self.gatk}  --java-options '-Xmx2G -Djava.io.tmpdir={gatk_filter_dir}'" \
                          f" MergeVcfs -I {clean_snp} " \
                          f"-I {clean_indel}  -O {clean_vcf} "
        print(MergeVcfs_shell)
        subprocess.check_call(MergeVcfs_shell)
        return clean_vcf

    def run(self) :
        sample_list = sample.split(",")
        if filter_type == "vqsr":
            self.run_VariantRecalibrator_1(sample_list)
        else:#如果用这种方式，后期改成多线程
            for eachsample in sample_list:
                self.run_VariantRecalibrator_2(eachsample)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="gatk filter manner of VariantRecalibrator or hardflter ", \
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('--sample', help="sample,if more than one sample, separated by commas ", required=True)
    #parser.add_argument('--outfile', help="outfile ", required=True)
    parser.add_argument('--filter_type',\
                        help="more than 30 samples for VariantRecalibrator and less than 30 samples for hardfiter ",\
                        required=True)
    argv = vars(parser.parse_args())
    sample = argv['sample'].strip()
    #outfile = argv['outfile'].strip()
    filter_type = argv['filter_type'].strip()
    pp = GARK(sample,filter_type)
    pp.run()


