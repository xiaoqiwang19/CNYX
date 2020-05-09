# coding: utf-8

"""

This module is for configure file reading as well as writing to other formats

designed for three basic configuration format:

- INI [default]
- JSON
- YAML

* Default to INI for simplicity, but json looks better, yaml are moderner.
* Try to keep the API same for all these formats.

one github repo which in MIT license showed similar functionality:
    https://github.com/ssato/python-anyconfig


* In the current folder, there will be ``default args`` for this pipeline

* In the project folders where all the analysis happen, there will be args to
override some of these ``default args``

"""

import argparse
import configparser
#import contextlib
import csv
import functools
import hashlib
import io
import json
import os
import os.path
import re
import pathlib
import subprocess
import sys
from functools import singledispatch
from glob import glob
from typing import List


#####################
# git commit getter #
#####################


def chdir(dest: str):
    "change back"
    curdir = os.path.abspath(os.curdir)
    assert os.path.isdir(dest), f"Not a dir: {dest}"
    os.chdir(dest)
    yield
    os.chdir(curdir)


def chdir_decorator(dest: str):
    "can be used as a function decorator"
    def wrapper(function):
        @functools.wraps(function)
        def inner_wrapper(*args, **kwargs):
            with chdir(dest):
                return function(*args, **kwargs)
        return inner_wrapper
    return wrapper


def get_git_commit(place=None):
    place = place or CFG['MP33']['BASE']
    makesure_dir(place)
    with chdir(place):
        try:
            res = subprocess.check_output("git rev-parse HEAD", shell=True)
            return res.decode().strip()
        except Exception as e:
            print(e)
            return "<get_git_commit_error>"

####################
# makesure helpers #
####################

def makesure_file(filename):
    if not os.path.isfile(filename):
        raise FileNotFoundError(filename)


def makesure_dir(dirname, *,
                 check_not_exist: bool = False,
                 mode = 0o777):
    """
    logics::

    1. create_a_folder_if_not_exist:
       - if `check_not_exist` is True:
             if `dirname` exists:
                 raise Error
             else:
                 create `dirname`
       - if `check_not_exist` is False:
             if `dirname` exists:
                 if `dirname` is a dir:
                     nothing happened
                 elif `dirname` is a file:
                     raise Error
             else:
                 create `dirname`
    """
    if check_not_exist:
        if os.path.exists(dirname):
            raise FileExistsError(f"Folder {dirname} should not exist")
        else:
            os.mkdir(dirname, mode=mode)
    else:
        if os.path.isfile(dirname):
            raise FileExistsError(f"file {dirname} should not exist, "\
                                  "because you are 'mkdir {dirname}'")
        elif os.path.isdir(dirname):
            pass
        else:
            os.mkdir(dirname, mode=mode)


def makesure_file_basedir(filename):
    # helper function
    makesure_dir(os.path.dirname(filename))


def makesure_dir_recursive(dirname):
    """
    mimic 'mkdir -p'
    """
    dirnames = []
    while not os.path.isdir(dirname):
        dirnames.append(dirname)
        dirname = os.path.dirname(dirname)
    for dirname in dirnames[::-1]:
        makesure_dir(dirname)


#########################
# basename munipulation #
#########################


def basename_ext(filename: str, dont_interpret_gzip: bool = False):
    """basename pick based on gzip or not"""
    # if not special
    if dont_interpret_gzip:
        return os.path.splitext(os.path.basename(filename))
    # else
    should_ignore_ext = {".gz", ".bz2"}
    special_ext = False
    for x in should_ignore_ext:
        if filename.endswith(x):
            special_ext = x
            filename = filename[:-len(special_ext)]  #  strip .gz
            break
    res, ext = os.path.splitext(os.path.basename(filename))
    if special_ext:
        return res, ext + special_ext
    else:
        return res, ext


###################################
# cfg obj to table and vice versa #
###################################


def cfg_to_table(ini_file, out_file):
    """
    read everything:
    1. section (in English)
    2. __chinese (in Chinese)
    3. key (in English)
    4. value (in English)
    5. comment (in English)
    """
    na = "--"
    makesure_file(ini_file)
    makesure_file_basedir(out_file)
    cfg = UnifiedConfigReader([ini_file])
    cfg_dict = _config_obj_to_dict(cfg)
    def get_description(filename):
        this_pat = re.compile(
            r"^(?P<key>[\S]+)\s*=\s*(?P<value>[\S]+)\s*##(?P<comment>.*)$"
        )
        section_pat = re.compile(
            r"^\[([.\w]+)\]$"
        )
        current_section = None
        res = {}
        with open(filename) as IN:
            for n, line in enumerate(IN, 1):
                this_m = this_pat.match(line.strip())
                section_m = section_pat.match(line.strip())
                if section_m:
                    current_section = section_m.group(1)
                if this_m:
                    assert current_section is not None, (n, line)
                    _d = this_m.groupdict()
                    key, value, comment = _d['key'], _d['value'], _d['comment']
                    res[(current_section, key, value)] = comment.split("##")[0].strip()
        return res

    __descriptions = get_description(ini_file)

    with open(out_file, 'w') as OUT:
        print("\t".join(
            ['section', '中文小节名', 'key', 'value', "description"]
        ), file=OUT)
        for section, items in cfg_dict.items():
            if items:
                for key, value in items.items():
                    if key == "__chinese":
                        continue
                    print("\t".join(
                        [section, items.get("__chinese", na),
                         key, value,
                         __descriptions[(section, key, value)]]
                    ), file=OUT)
            else:
                print("\t".join([section, na, na, na, na]), file=OUT)

    return {"cfg_dict": cfg_dict, "descriptions": __descriptions}


def table_to_cfg(table_file, out_file):
    na = "--"
    makesure_file(table_file)
    makesure_file_basedir(out_file)
    with open(table_file) as IN, open(out_file, 'w') as OUT:
        csv_reader = csv.DictReader(IN, delimiter="\t")
        seen_section = {}
        for line in csv_reader:
            if line['section'] not in seen_section:
                print(f"\n[{line['section']}]\n", file=OUT)
                seen_section[line['section']] = []
                if line['中文小节名'] != na:
                    print(f"__chinese = {line['中文小节名']}", file=OUT)
                else:
                    continue
            if line['key'] != na:
                assert line['key'] not in seen_section[line['section']], \
                    (seen_section, line)
                seen_section[line['section']].append(line['key'])
                assert line['value'] != na, line
                assert line['description'] != na, line
                print(f"{line['key']} = {line['value']}  "\
                      f"##{line['description']}",
                      file=OUT)


#############
# step_eval #
#############

def step_exec(filename: str, _globals={}, _locals={}, *,
              start_at: int = -1, logger=None, run: bool = True):
    """
    :arg int start_at: which lineno to start at
    """
    makesure_file(filename)
    with open(filename) as IN:
        lines = IN.readlines()
    steps = []
    step = ''
    for n, line in enumerate(lines, 1):
        if line.startswith("#"):
            continue
        if line.strip():
            step += line
        else:
            if step:
                steps.append((n - step.count("\n"), step))
            step = ''
    # last one is special
    if step:
        steps.append((n - step.count('\n') + 1, step))
    # run steps
    for n, step in steps:
        if n < start_at:
            if logger:
                logger.info(
                    f"step_exec: skipping step started at Line {n}({start_at}): '{step}'"
                )
            continue
        if logger:
            logger.info(f"step_exec: running step started at Line {n}({start_at}): '{step}'")
        if run:
            exec(step, _globals, _locals)
    return steps


###############
# unified API #
###############

ALL_FORMATS_LOWER = ("ini", "json", "yaml")

DEFAULT_FORMAT = "ini"

class UnifiedConfigReader:
    f"""
    try to maintain a unified reader for different configuration formats::

        {{ ALL_FORMATS_LOWER }} = {ALL_FORMATS_LOWER}

    1. first, this class will delegate configparser if format == "ini"
    2. then this class will support both json and yaml, mimic configparser's API

    reference: << Python cookbook 8.7 >>

    """
    def __init__(self, filenames: List[str], *, format: str = DEFAULT_FORMAT):
        assert format in ALL_FORMATS_LOWER,\
            f"Unknown configure file type: {format}"
        self.format = format
        assert isinstance(filenames, list), "You must specify a list"
        for filename in filenames:
            makesure_file(filename)
        self.filenames = filenames
        # init all parsers
        self.ini_parser = None
        if format == "ini":
            self.ini_parser = configparser.ConfigParser(
                interpolation=configparser.ExtendedInterpolation(),
                inline_comment_prefixes=("#",),
                allow_no_value=False)
            self.ini_parser.optionxform = lambda option: option
            for filename in filenames:
                # read config files one by one
                with open(filename) as IN:
                    self.ini_parser.read_file(IN)


    def varify_read(self, filename):
        """read a file only if we ALREADY have this option"""
        makesure_file(filename)
        if self.format == "ini":
            _cfg = UnifiedConfigReader([filename], format="ini")
            for section in _cfg.sections():
                assert section in self.sections(),\
                    f"I don't know this section {section}"
                for option in dict(_cfg[section]):
                    assert self.has_option(section, option),\
                        f"I don't know this option ${{{section}:{option}}}"
            self.read(filename)


    def __getattr__(self, name):
        """
        delegate to all functions
        """
        if self.format == "ini":
            # delegate to self.ini_parser
            assert self.ini_parser is not None,\
                'self.ini_parser should be created'
            # delegate to __ini_read_file
            if name == "read_file" or name == "read":
                return self.__ini_read_file
            elif name == "read_project_config":
                return self.__read_project_config
            return getattr(self.ini_parser, name)

    def __setstate__(self, state):
        self.__dict__ = state

    def __getstate__(self):
        return self.__dict__

    def __getitem__(self, thing):
        """
        delegate to subscriptability
        """
        if self.format == "ini":
            # delegate to ini's []
            assert self.ini_parser is not None,\
                'self.ini_parser should be created'
            return self.ini_parser[thing]


    def __ini_read_file(self, *args, **kwargs):
        """
        if other file(s) are read by calling read_file/read,
        self.__ini_read_file is called instead
        """
        # print(f"Reading #{len(self.filenames)+1} config file(s): {args[0]}", file=sys.stderr)
        if isinstance(args[0], list):
            for x in args[0]:
                assert x not in self.filenames, f"Reread the same file [{x}]?"
            self.filenames.extend(args[0])
        else:
            assert args[0] not in self.filenames,\
                f"Reread the same file [{args[0]}]?"
            self.filenames.append(args[0])
        return self.ini_parser.read(*args, **kwargs)


    # def __read_project_config(self, filename):
    #     """project.ini is different, because we want to define other variables only in projec.ini"""
    #     filename = os.path.abspath(filename)
    #     print(f"reading project config file {filename}", file=sys.stderr)
    #     makesure_file(filename)
    #     self['MP33']['PROJECT_DIR'] = os.path.dirname(filename)  # DEFINE another variable
    #     self.ini_parser.read(filename)

#########################
# convert configuration #
#########################

def _config_obj_to_dict(obj):
    """transfer config obj to a dict"""
    out_dict = {}
    for _section in obj.sections():
        out_dict[_section] = dict(obj.items(_section))
    return out_dict


@singledispatch
def to_serializable(obj):
    return str(obj)


def _dict_to_json(config_dict, outstream=sys.stdout):
    """dict to json"""
    print(json.dumps(config_dict, ensure_ascii=False,
                     indent=4, default=to_serializable),
          file=outstream)


def obj_to_json(config, *, outstream=sys.stdout):
    """output a config obj to json"""
    _dict_to_json(_config_obj_to_dict(config), outstream)


def convert_configuration(input_format, output_format, *,
                          infiles, outstream=sys.stdout):
    """convert configuration file(s) to help checking, now support:

    1. ini(s) -> json
    """
    assert input_format in ALL_FORMATS_LOWER
    assert output_format in ALL_FORMATS_LOWER
    if input_format == "ini" and output_format == "json":
        reader = UnifiedConfigReader(infiles, format="ini")
        obj_to_json(reader, outstream=outstream)


def json_to_obj(in_file: str):
    with open(in_file) as IN:
        return json.load(IN)


##############
# global CFG #
##############

def _collect_all_default_configs(basedir: str = os.path.dirname(
        os.path.abspath(__file__)),
                                 format: str = DEFAULT_FORMAT):
    """
    collect all config files inside a ``basedir`` [default to current
    file folder]
    """
    assert format in ALL_FORMATS_LOWER, f"Unknown format: {format}"
    return glob(f"{basedir}/*.{format}")


"""
``CFG`` is a global variable stands for the *current* config obj,
a UnifiedConfigReader instance

default to all files inside the config folder, which have `DEFAULT_FORMAT` ext

if any external config files need to be read, just use::

    from MP33.config import config
    config.CFG.read(external_config_files)
"""

CFG = UnifiedConfigReader(_collect_all_default_configs())  # all DEFAULT_FORMAT

##### define MP33 global variables, this is dangerous
#CFG['MP33']['BASE'] = str(pathlib.PurePath(os.path.abspath(__file__))\
#                          .parent.parent)

############################
# external config override #
############################

"""
this global variable will override CFG's value if specified

PATH-like syntax: colon seperated

latter file will override former file
"""
_CONFIG_OTHER_FILES = os.environ.get("_MP33_CONFIG_OTHER_FILES")
if _CONFIG_OTHER_FILES:
    for file in _CONFIG_OTHER_FILES.split(":"):
        makesure_file(file)
        CFG.read_file(file)

############
# argparse #
############

_RAW_DESCRIPTION = """
A config module to parse the configure file

* Designed to support all of 'ini', 'json' and 'yaml', but now only 'ini' is \
implemented.

"""


def md5sum_files(filenames, outstream=sys.stdout):
    for filename in filenames:
        makesure_file(filename)
        md5 = hashlib.md5()
        with open(filename, 'rb') as IN:
            md5.update(IN.read())
        print(f"{md5.hexdigest()}  {filename}", file=outstream)


def _parse_normal_args(parser=None, *, test_args=None):
    """entry of this script's main
    parser should be None
    """
    parser = parser or \
             argparse.ArgumentParser(formatter_class=\
                                     argparse.RawDescriptionHelpFormatter,
                                     description=_RAW_DESCRIPTION)
    parser.add_argument("--in-format", "--from", dest="in_format",
                        help="config file format, case insensitive",
                        choices=ALL_FORMATS_LOWER, type=str.lower)
    parser.add_argument("--out-format", "--to", dest="out_format",
                        help="config file output format, "\
                        "case insensitive", choices=ALL_FORMATS_LOWER,
                        type=str.lower)
    parser.add_argument("infiles", help="input file(s)", nargs="+")
    parser.add_argument("outfile", help="out file, can be -", nargs="?")
    args = parser.parse_args(test_args or sys.argv[1:])  # fix this one
    # check if and of are not the same
    assert args.in_format != args.out_format, "You should not specify two "\
                             "identical formats to convert to"
    # check input file exists
    for infile in args.infiles:
        makesure_file(infile)
    # if not specify outfile, then - is used
    if not args.outfile:
        args.outfile = "-"
    return args



##############################
# get sampleid from bam file #
##############################

def get_sampleid(string):
    """
    If given /your/file/place/A_B_C_D20180117_shit.1.2.3.bam,
     should return A_B_C_D20180117_shit as sample's id
    """
    basename, ext = basename_ext(string)
    return basename.split(".")[0]


if __name__ == '__main__':
    ARGS = _parse_normal_args()

    if ARGS.outfile == "-":
        convert_configuration(ARGS.in_format, ARGS.out_format,
                              infiles=ARGS.infiles, outstream=sys.stdout)
    else:
        with open(ARGS.outfile, 'w') as OUT:
            convert_configuration(ARGS.in_format, ARGS.out_format,
                                  infiles=ARGS.infiles, outstream=OUT)

    print(f"ALL DONE from {__file__}.")
