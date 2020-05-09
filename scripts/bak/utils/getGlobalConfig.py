import yaml
import os
import argparse


# global Configs
def getGlobalConfig(snakeDir):
    globalConfig = yaml.load(open(os.path.join(snakeDir, 'conf/globalConfig.yaml')),
                             Loader=yaml.BaseLoader)
    return globalConfig


def getSoftwares(globalConfig, snakeDir):
    projectDir = os.path.dirname(snakeDir)
    softwares = {}
    _softwares = globalConfig['softwares']
    _softDir = _softwares.get('softDir', '') or os.path.join(projectDir, 'softwares')
    _java = _softwares.get('java', '').replace('{softDir}', _softDir)
    for k, v in _softwares.items():
        softwares[k] = v.replace('{softDir}', _softDir).replace('{java}', _java)
    return softwares


def getScripts(globalConifg, snakeDir):
    scripts = {}
    for k, v in globalConifg['scripts'].items():
        if v.startswith('/'):
            scripts[k] = v
        else:
            scripts[k] = os.path.join(snakeDir, v)
    return scripts


def getGlobalFiles(globalConfig, snakeDir):
    projectDir = os.path.dirname(snakeDir)
    globalFiles = {}
    for k, v in globalConfig['globalFiles'].items():
        if v.startswith('/'):
            globalFiles[k] = v
        else:
            globalFiles[k] = os.path.join(projectDir, v)
    return globalFiles


funDict = {'scripts': getScripts, 'globalFiles': getGlobalFiles, 'softwares': getSoftwares}


def formatGlobalConfig(snakeDir):
    globalConfig = getGlobalConfig(snakeDir)
    for k in funDict:
        globalConfig[k] = funDict[k](globalConfig, snakeDir)
    return globalConfig


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-k1', "--firstKey")
    parser.add_argument('-k2', "--secondKey")
    args = parser.parse_args()

    snakeDir = os.environ['snakeDir']

    confDict = getGlobalConfig(snakeDir)
    conf = funDict[args.firstKey](confDict, snakeDir)[args.secondKey]

    print(conf)
