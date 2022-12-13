#!/usr/bin/python3
'''check the obtained json against its target'''
import json
from json_tools import diff

# import copy
import collections

from typing import Any,Callable

import logging

def check_composition(obtainedjson:json,targetfile:str)->None:
    with open(targetfile,'r') as f:
        targetjson = json.load(f)
#    one=ordered(flatten(obtainedjson))
#    two=ordered(flatten(targetjson))
    one=flatten(obtainedjson)
    two=flatten(targetjson)
    # print (obtainedjson)
    # print(one)
    # print('\n\n')
    # print (two)
    logging.info("Phenopacket: diff between obtained and target jsons")
    logging.info(json.dumps((diff(one,two)),indent=4))
    return



def flatten(d:dict, parent_key:str='', sep:str='_')->dict:
    items = []
    if d is not None:
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
    return dict(items)



def ordered(obj:Any)->Any:
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

