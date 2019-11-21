#!/usr/bin/python3
'''The program creates an openehr composition according to one of heather's templates.
Read from "input" file the path where the phenopackets are located.
Other files needed for each phenopacket translation:
    -phenopacket json file
    -target file [optional]: In order to perform the final check it neeeds a target file against which it
compares the composition created.The target file needs to be in the same dir as the pheno-file and have the same
name but with extension .target
    -ctxinfo file: file with ctxinfo information to fill in the composition. It needs to be in the same dir as the pheno-file and have the same
name but with extension .ctxinfo
    -context file: file with context information to fill in the composition. It needs to be in the same dir as the pheno-file and have the same
name but with extension .context
'''
import json

import logging
import argparse

import pathlib

from routines2compo.FindPhenopackets import find_phenopackets
from routines2compo.CheckComposition import check_composition
from routines2compo.Convert2Composition import convert2composition


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--loglevel',help='the logging level:DEBUG,INFO,WARNING,ERROR or CRITICAL',default='WARNING')
    parser.add_argument('--pathfile',help='file with the paths to the phenopackets',type=str)
    parser.add_argument('--check',action='store_true', help='4 debugging: check the composition obtained against a target')
    args=parser.parse_args()

    loglevel=getattr(logging, args.loglevel.upper(),logging.WARNING)
    if not isinstance(loglevel, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(filename='./phenopacket_2_compositions_structured.log',filemode='w',level=loglevel)

    inputfile="input"
    if args.pathfile:
        inputfile=args.pathfile

    print(f'inputfile is {inputfile}')
#    print (args.check)
    if args.check:
        check=True
        print ('Check is set to true')

    #read input file with path where compositions are located
    try:
        with open(inputfile,'r') as f:
            paths=f.readlines()
            paths=[x.strip() for x in paths]

            print ('paths read:')
            for path in paths:
                print(path)
    except:
        print(f"problem with input file {inputfile}")
        exit(1)


    #find a list of all the phenopackets
    list_of_phenopackets={}
    for path in paths:
        newlist=find_phenopackets(path)
        list_of_phenopackets.update(newlist)
    print("phenopackets found:")
    for listc in list_of_phenopackets:
        for file in list_of_phenopackets[listc]:
            print (listc+"/"+file)


    #Convert each phenopacket into a composition and serialize
    for listc in list_of_phenopackets:
        for file in list_of_phenopackets[listc]:
            filename=listc+"/"+file
            #convert to json composition
            outputfile='./COMPOSITION_FROM'+file
            jsonconverted=convert2composition(filename,outputfile)
            print (f'New composition file created: {outputfile}')
#        with open('../phenowholeinput.json','r') as f:
#            jsoninput = json.load(f)
#        jsonconverted=jsoninput
            print (f'check is {check}')
            #convert to phenopacket and serialize on file the result
            if check:
                targetfile=filename[:-4]+'target'
                file=pathlib.Path(targetfile)
                if file.exists():
                    check_composition(jsonconverted,targetfile)
                else:
                    print ('A .target file is needed if check flag is on')
                    logging.error('A target is needed when the check flag has been set to true. It must \
                        have the same name as the input file but extension .target')
            logging.debug(json.dumps(jsonconverted,sort_keys=True,indent=4))





if __name__ == '__main__':
    main()