#!/usr/bin/python3
'''Find all json compositions given a path'''
import logging
import os
def find_phenopackets(path:str)->dict:
    jsons = {}
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.json'):
                absroot=os.path.abspath(root)
                if absroot in jsons.keys():
                    logging.debug(f'file found {jsons[absroot]}')
                    jsons[absroot].append(file)
                else:
                    jsons[absroot]=[file]
                    logging.info(f'file found {jsons[absroot]}')
    return jsons