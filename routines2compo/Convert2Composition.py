#!/usr/bin/python3
'''convert from a phenopacket (interpretation,cohort,family,phenopacket) to
an interpretation template composition or a cohort template composition
To convert from family and phenopacket we create arbitrary fields in an interpretation
template composition'''
import json
import logging
import sys
import pathlib

from google.protobuf import message
from google.protobuf.json_format import Parse, MessageToJson
from interpretation_pb2 import Interpretation
from phenopackets_pb2 import Phenopacket,Family,Cohort

def convert2composition(filename:str,outputfile:str)->json:
    #ff=parameter to toggle insertion of info not coming from the phenopacket
    #useful to make easier the final comparison between the result and the target
    ff=True
    with open(filename,'r') as f:
        jsonp = json.load(f)
        #check needed files existance
        filectxinfo=filename[:-4]+'ctxinfo'
        file=pathlib.Path(filectxinfo)
        if not file.exists():
            print (f'A .ctxinfo file is needed for each phenopacket file[{filename}]')
            logging.error(f'A ctxinfo json file is needed for each phenopacket file[{filename}]. It must \
                        have the same name as the input file but extension .ctxinfo')
            sys.exit(1)
        filecontext=filename[:-4]+'context'
        file=pathlib.Path(filecontext)
        if not file.exists():
            print (f'A .context file is needed for each phenopacket file [{filename}]')
            logging.error(f'A context json file is needed for each phenopacket file[{filename}]. It must \
                        have the same name as the input file but extension .context')
            sys.exit(1)
        #parsing
        if 'resolutionStatus' in jsonp: #interpretation
            print(f"{filename} is an Interpretation")
            logging.info(f"{filename} is an Interpretation")
            #check if it's a legit phenopacket
            try:
                readmessage(filename,Interpretation())
            except Exception as e:
                print (f'file {filename} unrecognized as Interpretation phenopacket')
                logging.error(f'file {filename} unrecognized as Interpretation phenopacket')
                sys.exit(1)
            myjson=convert2interpretationreport(jsonp,filectxinfo,filecontext,ff)

        elif 'members' in jsonp: #cohort
            print(f"{filename} is a Cohort")
            logging.info("{filename} is a Cohort")
            #check if it's a legit phenopacket
            try:
                readmessage(filename,Cohort())
            except Exception as e:
                print (f'file {filename} unrecognized as Cohort phenopacket')
                logging.error(f'file {filename} unrecognized as Cohort phenopacket')
                sys.exit(1)
            myjson=convert2cohortreport(jsonp,filectxinfo,filecontext,ff)
        with open(outputfile,'w') as g:
            json.dump(myjson,g,sort_keys=True,indent=4)
        return myjson

def convert2interpretationreport(jsonint:json,filectxinfo:str,filecontext:str,ff:bool)->json:
    myjson={}
    myjson.update(insertctx(filectxinfo))
    interpretation_report={}
    interpretation_report.update(insertcontext(filecontext))
    interpretation={}
    #id
    interpretation['id']=[convertId(jsonint['id'],ff)]
    #resolution_status
    resdata={}
    resdata['|value']=jsonint['resolutionStatus']
    if ff:
        resdata['|code']='at0007'
        resdata['|ordinal']=0
    interpretation['resolution_status']=[resdata]
    #phenopacket or family
    if 'phenopacket' in jsonint:
        interpretation['phenopacket']=[convertPheno(jsonint['phenopacket'],ff)]
    elif 'family' in jsonint:
        interpretation['family']=[convertFamily(jsonint['family'],ff)]
    #diagnosis
    interpretation['diagnosis']=convertDiagnosis(jsonint['diagnosis'],ff)
    #metadata
    interpretation['metadata']=[convertMeta(jsonint['metaData'],ff)]
#
    interpretation_report['interpretation']=[interpretation]
    myjson['interpretation_report']=interpretation_report
    return myjson

def convertId(idf:str,ff:bool)->json:
    iddata={}
    iddata['|id']=idf
    if ff:
        iddata['|issuer']='Issuer'
        iddata['|assigner']='Assigner'
        iddata['|type']='Prescription'
    return iddata

def convert2cohortreport(jsoncoh:json,filectxinfo:str,filecontext:str,ff:bool)->json:
    myjson={}
    myjson.update(insertctx(filectxinfo))
    cohort_report={}
    cohort_report.update(insertcontext(filecontext))
    cohort={}
    #id
    cohort['id']=[convertId(jsoncoh['id'],ff)]
    #description
    if 'description' in jsoncoh:
        cohort['description']=[jsoncoh['description']]
    #members
    cohort['phenopacket']=convertMembers(jsoncoh['members'],ff)
    #htsfiles
    if 'htsFiles' in jsoncoh:
        cohort['htsfile']=convertHtsFiles(jsoncoh['htsFiles'],ff)
    #metadata
    cohort['metadata']=[convertMeta(jsoncoh['metaData'],ff)]
    cohort_report['cohort']=[cohort]
    myjson['cohort_report']=cohort_report
    return myjson

def convertMembers(jsonmember:list,ff:bool)->list:
    mems=[]
    for mem in jsonmember:
        mems.append(convertPheno(mem,ff))
    return mems

def insertctx(filectxinfo:str)->json:
    with open(filectxinfo,'r') as f:
        jsonp = json.load(f)
    return jsonp

def insertcontext(filecontext:str)->json:
    with open(filecontext,'r') as f:
        jsonp = json.load(f)
    return jsonp


def readmessage(string:str,type:message)->message:
    with open(string, 'r') as jsfile:
        round_trip = Parse(message=type, text=jsfile.read())
        return round_trip

def convertPheno(jsonint:json,ff:bool)->json:
    jp={}
    #id
    jp['id']=[convertId(jsonint['id'],ff)]
    #subject
    if 'subject' in jsonint:
        jp['subject']=[jsonint['subject']['id']]
    #phenotypic_features
    if 'phenotypicFeatures' in jsonint:
        jp['phenotypic_feature']=convertPhenotypicfeatures(jsonint['phenotypicFeatures'],ff)
    #biosamples
    if 'biosamples' in jsonint:
        jp['biosample']=convertBiosamples(jsonint['biosamples'],ff)
    #genes
    if 'genes' in jsonint:
        jp['gene']=convertGenes(jsonint['genes'],ff)
    #variants
    if 'variants' in jsonint:
        jp['variant']=convertVariants(jsonint['variants'],ff)
    #diseases
    if 'disease' in jsonint:
        jp['disease']=convertDiseases([jsonint['disease']],ff)
    if 'diseases' in jsonint:
       jp['disease']=convertDiseases(jsonint['diseases'],ff)
    #hts_files
    if 'htsFiles' in jsonint:
        jp['htsfile']=convertHtsFiles(jsonint['htsFiles'],ff)
    #metadata
    if 'metaData' in jsonint:
        jp['metadata']=[convertMeta(jsonint['metaData'],ff)]
        #workaround to buggy phenopacket(member) in cohort that has a void metadata
        if len(list(jp['metadata'][0].keys()))==0:
            del jp['metadata']
    return jp

def convertFamily(jsonint:json,ff:bool)->json:
    jf={}
    #id
    jf['id']=[convertId(jsonint['id'],ff)]
    #proband
    jf['proband']=[convertPheno(jsonint['proband'],ff)]
    #relatives
    if 'relatives' in jsonint:
        relatives=[]
        for rel in jsonint['relatives']:
            relatives.append(convertPheno(rel,ff))
        jf['relative']=relatives
    #pedigree
    jf['pedigree']=[convertPedigree(jsonint['pedigree'],ff)]
    #hts_files
    if 'htsFiles' in jsonint:
        jf['htsfile']=convertHtsFiles(jsonint['htsFiles'],ff)
    #metadata
    jf['metadata']=[convertMeta(jsonint['metaData'],ff)]
    return jf



def convertMeta(jsonmeta:json,ff:bool)->json:
    #workaround to buggy phenopacket(member) in cohort that has a void metadata
    if len(list(jsonmeta.keys()))==0:
        return {}
    metadata={}
    #created
    metadata['created']=[jsonmeta['created']]
    #created_by
    metadata['created_by']=[jsonmeta['createdBy']]
    #submitted_by
    if 'submittedBy' in jsonmeta:
        metadata['submitted_by']=[jsonmeta['submittedBy']]
    #resources
    resources=[]
    for res in jsonmeta['resources']:
        resource={}
        resource['id']=[convertId(res['id'],ff)]
        resource['name']=[res['name']]
        resource['url']=[res['url']]
        resource['version']=[res['version']]
        resource['namespace_prefix']=[res['namespacePrefix']]
        resource['iri-prefix']=[res['iriPrefix']]
        resources.append(resource)
    metadata['resource']=resources
    if 'externalReferences' in jsonmeta:
        external_references=[]
        for ext in jsonmeta['externalReferences']:
            external_ref={}
            external_ref['id']=[convertId(ext['id'],ff)]
            if 'description' in ext:
                external_ref['description']=[ext['description']]
            external_references.append(external_ref)
        metadata['external_reference']=external_references
    if 'updates' in jsonmeta:
        updates=[]
        for upd in jsonmeta['updates']:
            update={}
            update['timestamp']=[upd['timestamp']]
            update['comment']=[upd['comment']]
            if 'updatedBy' in upd:
                update['updated_by']=[upd['updatedBy']]
            updates.append(update)
        metadata['update']=updates
    if 'phenopacketSchemaVersion' in jsonmeta:
        metadata['phenopacket_schema_version']=[jsonmeta['phenopacketSchemaVersion']]
    return metadata

def convertIdcode(idt:str,label:str)->json:
    values=idt.split(':')
    ptype={}
    ptype['|code']=values[1]
    ptype['|terminology']=values[0]
    ptype['|value']=label
    return ptype


def convertPhenotypicfeatures(jsonphenot:list,ff)->list:
    phenotypic_features=[]
    for phen in jsonphenot:
        phenotypic_feature={}
        phenotypic_feature['type']=[convertIdcode(phen['type']['id'],phen['type']['label'])]
        if 'description' in phen:
            phenotypic_feature['description']=[phen['description']]
        if 'negated' in phen:
            phenotypic_feature['negated']=[phen['negated']]
        else:
            phenotypic_feature['negated']=[False]
        if 'severity' in phen:
            phenotypic_feature['severity']=[convertIdcode(phen['severity']['id'],phen['severity']['label'])]
        if 'modifiers' in phen:
            modifiers=[]
            for modi in phen['modifiers']:
                modifier=convertIdcode(modi['id'],modi['label'])
                modifiers.append(modifier)
            phenotypic_feature['modifier']=modifiers
        if 'classOfOnset' in phen:
            phenotypic_feature['onset']=[convertIdcode(phen['classOfOnset']['id'],phen['classOfOnset']['label'])]
        if 'evidence' in phen:
            evidences=[]
            for evid in phen['evidence']:
                evidence={}
                evidence['evidence_code']=[convertIdcode(evid['evidenceCode']['id'],evid['evidenceCode']['label'])]
                if 'reference' in evid:
                    ref={}
                    ref['id']=[convertId(evid['reference']['id'],ff)]
                    if 'description' in evid['reference']:
                        ref['description']=[evid['reference']['description']]
                    evidence['external_reference']=[ref]
                evidences.append(evidence)
            phenotypic_feature['evidence']=evidences
        phenotypic_features.append(phenotypic_feature)
    return phenotypic_features


def convertBiosamples(jsonbio:list,ff:bool)->list:
    biosamples=[]
    for bio in jsonbio:
        biosample={}
        biosample['id']=[convertId(bio['id'],ff)]
        if 'individualId' in bio:
            biosample['individual_id']=[convertId(bio['individualId'],ff)]
        if 'description' in bio:
            biosample['description']=[bio['description']]
        biosample['sampled_tissue']=[convertIdcode(bio['sampledTissue']['id'],bio['sampledTissue']['label'])]
        if 'phenotypicFeatures' in bio:
            biosample['phenotypiFeatures']=convertPhenotypicfeatures(bio['phenotypicFeatures'])
        if 'taxonomy' in bio:
            biosample['taxonomy']=[convertIdcode(bio['taxonomy']['id'],bio['taxonomy']['label'])]
        if 'ageOfIndividualAtCollection' in bio:
            biosample['individual_age_at_collection']=[{'duration_value':[bio['ageOfIndividualAtCollection']['age']]}]
        if 'histologicalDiagnosis' in bio:
            biosample['histological_diagnosis']=[convertIdcode(bio['histologicalDiagnosis']['id'],bio['histologicalDiagnosis']['label'])]
        if 'tumorProgression' in bio:
            biosample['tumor_progression']=[convertIdcode(bio['tumorProgression']['id'],bio['tumorProgression']['label'])]
        if 'tumorGrade' in bio:
            biosample['tumor_grade']=[convertIdcode(bio['tumorGrade']['id'],bio['tumorGrade']['label'])]
        if 'diagnosticMarkers' in bio:
            diamarks=[]
            for dm in bio['diagnosticMarkers']:
                diamarks.append(convertIdcode(dm['id'],dm['label']))
            biosample['diagnostic_markers']=diamarks
        if 'procedure' in bio:
            procedure={}
            procedure['code']=[convertIdcode(bio['procedure']['code']['id'],bio['procedure']['code']['label'])]
            if 'bodySite' in bio['procedure']:
                procedure['body_site']=[convertIdcode(bio['procedure']['bodySite']['id'],bio['procedure']['bodySite']['label'])]
            biosample['procedure']=[procedure]
        if 'htsFiles' in bio:
            biosample['htsfile']=convertHtsFiles(bio['htsFiles'])
        if 'variants' in bio:
            biosample['variants']=convertVariants(bio['variants'])
        if 'isControlSample' in bio:
            biosample['is_control_sample']=[bio['isControlSample']]
        else:
            biosample['is_control_sample']=[False]
        biosamples.append(biosample)
    return biosamples


def convertHtsFiles(jsonhts:list,ff:bool)->list:
    hts_files=[]
    for hts in jsonhts:
        htsfile={}
        htsfile['uri']=[hts['uri']]
        htsfile['htsFormat']=[{'|code':hts['htsFormat']}]
        htsfile['genome_assembly']=[hts['genomeAssembly']]
        if 'description' in hts:
            htsfile['description']=[hts['description']]
        if 'individualToSampleIdentifiers' in hts:
            key=list(hts['individualToSampleIdentifiers'].keys())[0]
            value=hts['individualToSampleIdentifiers'][key]
            htsfile['individual_identifier']=[convertId(key,ff)]
            htsfile['sample_identifier']=[convertId(value,ff)]
        hts_files.append(htsfile)
    return hts_files

def convertVariants(jsonv:list,ff)->list:
    zygo=False
    variant={}
    for vart in jsonv:
        if 'zygosity' in vart and not zygo:
            variant['zygosity']=[convertIdcode(vart['zygosity']['id'],vart['zygosity']['label'])]
        if 'hgvsAllele' in vart:
            hgvs={}
            if 'id' in vart['hgvsAllele']:
                hgvs['id']=[convertId(vart['hgvsAllele']['id'],ff)]
            hgvs['hgvs']=[vart['hgvsAllele']['hgvs']]
            variant['hgvsallele']=[hgvs]
        if 'vcfAllele' in vart:
            vcfa={}
            if 'id' in vart['vcfAllele']:
                vcfa['id']=[convertId(vart['vcfAllele']['id'],ff)]
            vcfa['genome_assembly']=[vart['vcfAllele']['genomeAssembly']]
            vcfa['chr']=[vart['vcfAllele']['chr']]
            vcfa['pos']=[vart['vcfAllele']['pos']]
            vcfa['re']=[vart['vcfAllele']['ref']]
            vcfa['alt']=[vart['vcfAllele']['alt']]
            vcfa['info']=[vart['vcfAllele']['info']]
            variant['vcfallele']=[vcfa]
        if 'spdiAllele' in vart:
            spdi={}
            if 'id' in vart['spdiAllele']:
                spdi['id']=[convertId(vart['spdiAllele']['id'],ff)]
            spdi['seq_id']=[convertId(vart['spdiAllele']['seqId'],ff)]
            spdi['position']=[vart['spdiAllele']['position']]
            spdi['deleted_sequence']=[vart['spdiAllele']['deletedSequence']]
            spdi['inserted_sequence']=[vart['spdiAllele']['insertedSequence']]
            variant['spdiallele']=[spdi]
        if 'iscnAllele' in vart:
            iscn={}
            if 'id' in vart['iscnAllele']:
                iscn['id']=[convertId(vart['iscnAllele']['id'],ff)]
            iscn['iscn']=[vart['iscnAllele']['iscn']]
            variant['iscnallele']=[iscn]
    variants=[variant]
    return variants


def convertGenes(jsongenes:list,ff:bool)->list:
    genes=[]
    for ge in jsongenes:
        gene={}
        gene['gene_symbol']=[convertIdcode(ge['id'],ge['symbol'])]
        genes.append(gene)
    return genes

def convertDiseases(jsondiseases:list,ff:bool)->list:
    diseases=[]
    for dis in jsondiseases:
        disease={}
        disease['term']=[convertIdcode(dis['term']['id'],dis['term']['label'])]
        if 'ageOfOnset' in dis:
            disease['onset']=[{'duration_value':[dis['ageOfOnset']['age']]}]
        if 'tumorStage' in dis:
            tumor_stage=[]
            for tum in dis['tumorStage']:
                tumor_stage.append(convertIdcode(tum['id'],tum['label']))
            disease['tumor_stage']=tumor_stage
        diseases.append(disease)
    return diseases

def convertDiagnosis(diag:json,ff:bool)->json:
    diagnoses=[]
    for dia in diag:
        diagnosis={}
        diagnosis['disease']=convertDiseases([dia['disease']],ff)
        diagnosis['genomic_interpretation']=[convertGenomicInterpretations(dia['genomicInterpretations'],ff)]
        diagnoses.append(diagnosis)
    return diagnoses


def convertGenomicInterpretations(genom:json,ff:bool)->json:
    genint={}
    statusGI=False
    statusGENE=False
    statusVARIANT=False
    #pack all the interpretations into one
    for geno in genom:
        if not statusGI:
            gist={}
            if ff:
                gist['|code']='at0005'
                gist['|ordinal']=0
            gist['|value']=geno['status']
            genint['genomicinterpretation_status']=[gist]
            statusGI=True
        if 'gene' in geno:
            if not statusGENE:
                genint['gene']=convertGenes([geno['gene']],ff)
                statusGENE=True
            else:
                gendict=convertGenes([geno['gene']],ff)[0]
                genint['gene'][0].update(gendict)
        if 'variant' in geno:
            if not statusVARIANT:
                genint['variant']=convertVariants([geno['variant']],ff)
                statusVARIANT=True
            else:
                gendict=convertVariants([geno['variant']],ff)[0]
                genint['variant'][0].update(gendict)
    return genint

def convertPedigree(jsonped:json,ff:bool)->json:
    pedigree={}
    persons=[]
    for per in jsonped['persons']:
        person={}
        person['family_id']=[convertId(per['familyId'],ff)]
        person['individual_id']=[convertId(per['individualId'],ff)]
        person['paternal_id']=[convertId(per['paternalId'],ff)]
        person['maternal_id']=[convertId(per['maternalId'],ff)]
        sex={}
        if ff:
            sex['|code']='at0009'
            sex['|ordinal']=0
        sex['|value']=per['sex']
        person['sex']=[sex]
        person['affected_status']=[{'|code':per['affectedStatus']}]
        persons.append(person)
    pedigree['person']=persons
    return pedigree