import json
import os
import re
import random
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--ner_data_count_per_dataset',type=int,default = 100)
parser.add_argument('--re_data_count_per_dataset',type=int,default = 100)

args = parser.parse_args()
args.ner_dataset = 3
args.re_dataset = 3
dataset_names = ['kelm','tacgen','TREx']
def ner_data_process(cur_id = 0):
    all_instances = []
    task = "ner"
    entity_type_dict = {}
    for dataset_ind,dataset in enumerate(dataset_names):
        file_dir = dataset+'_data/'
        file_list = os.listdir(file_dir)
        ##demo

        
        for file_name in file_list:
            if 'ner' not in file_name:
                continue
            file_path = file_dir+file_name
            with open(file_path,'r',encoding='utf-8') as ner_f:
                lines = ner_f.readlines()
            for line in lines:
                line = json.loads(line)
                entities = line["entities"]
                cur_entity_type_set = set()
                for cur_entity_type,v in entities.items():
                    cur_type_des = v['des']

                    entity_type_dict[cur_entity_type] = cur_type_des
                    cur_entity_type_set.add(cur_entity_type)
                    
                    cur_type_entities = v['instance']
                    output = ""
                    instruction = f'please extract the "{cur_entity_type}" entity in following sentence,output as format : '
                    instruction += "{entity_name, instance of, entity_type}\n"
                    definition = f'"{cur_entity_type}" refers to {cur_type_des},'
                    explanation = ""
                   
                    
                    for item in cur_type_entities:
                        explanation += f'"{item["value"]}" refers to {item["des"]},so the "{item["value"]}" is a "{cur_entity_type}" type entity.'
                        output += '{'
                        output += f"{item['value']},instance of,{cur_entity_type}"
                        output += '}'

                    instance = {"id":str(cur_id),"sentence":line['text'],"output":output,"ent_type":{"head_entity":[""],"tail_entity":[""]},"instruction":instruction,"definition":definition,"explanation":explanation,"correction":"corection:"}
                    cur_id += 1
                    all_instances.append(instance)
                all_entity_type = list(entity_type_dict.keys())
                null_entity = random.choice(all_entity_type)
                if null_entity not in cur_entity_type_set:
                    output = "{"
                    output += f"None,instance of,{null_entity}"
                    output += '}'
                    instruction = f'please extract the "{null_entity}" entity in following sentence,output as format : '
                    instruction += "{entity_name, instance of, entity_type}\n"
                    definition = f'"{null_entity}" refers to {entity_type_dict[null_entity]},'
                    explanation = f'In this sentence there is no entity of {null_entity} type.' 
                    instance = {"id":str(cur_id),"sentence":line['text'],"output":output,"ent_type":{"head_entity":[""],"tail_entity":[""]},"instruction":instruction,"definition":definition,"explanation":explanation,"correction":"corection:"}
                    cur_id += 1
                    all_instances.append(instance)
                    
                   
                if cur_id > (dataset_ind+1)* args.ner_data_count_per_dataset:
                    print(f'cur_data_id:{cur_id} \n {dataset} {task} data load down ..')
                    break
            if cur_id > (dataset_ind+1) * args.ner_data_count_per_dataset:
                break
    with open("/root/IE_LLM/Tk-Instruct/train_data/ner/train.json",'w',encoding='utf-8') as f:
        json.dump({"Task":"ner","Positive Examples":[],"Negative Examples":[],"Instances":all_instances},f)
    return cur_id


def re_data_process(cur_id = 0):
    all_instances = []
    relation_type_dict = {}
    
    task = "re"
    for dataset_ind,dataset in enumerate(dataset_names):
        file_dir = dataset+'_data/'
        file_list = os.listdir(file_dir)
        ##demo

        
        for file_name in file_list:
            if 'ner'  in file_name:
                continue
            file_path = file_dir+file_name
            with open(file_path,'r',encoding='utf-8') as ner_f:
                lines = ner_f.readlines()
            for line in lines:
                line = json.loads(line)
                
                relations = line["relations"]
                cur_relation_type_set = set()
                for cur_relation_type,v in relations.items():
                    
                    cur_type_des = v['des']
                    cur_relation_type_set.add(cur_relation_type)
                    relation_type_dict[cur_relation_type] = cur_type_des
                    cur_type_relations = v['instance']
                    head_entity_type_set,tail_entity_type_set = set(),set()
                    output = ""
                    instruction = f'please extract the "{cur_relation_type}" relation in following sentence ,output as format: '
                    instruction += "{head entity, relation type, tail entity}\n"
                    definition = f'"{cur_relation_type}" refers to {cur_type_des},'
                    explanation = ""
                   
                    
                    for item in cur_type_relations:
                        explanation += f'the "{cur_relation_type}" can describe a relation between "{item["obj_type"]}" entity and "{item["subj_type"]}" entity. "{item["object"]}" is a "{item["obj_type"]}" entity and the "{item["subject"]}" is a "{item["subj_type"]}" entity. "{item["object"]}" and "{item["subject"]}" has a "{cur_relation_type}" relation.'
                        output += "{"
                        output += f"{item['object']},{cur_relation_type},{item['subject']}"
                        output += "}"
                        head_entity_type_set.add(item['obj_type'])
                        tail_entity_type_set.add(item['subj_type'])
                        
                    instance = {"id":str(cur_id),"sentence":line['text'],"output":output,"ent_type":{"head_entity":list(head_entity_type_set),"tail_entity":list(tail_entity_type_set)}
                                ,"instruction":instruction,"definition":definition,"explanation":explanation,"correction":"correction:"}
                    cur_id += 1
                    all_instances.append(instance)
                
                relation_types = list(relation_type_dict.keys())
                null_relation = random.choice(relation_types)
                if null_relation not in cur_relation_type_set:
                    output = "{"
                    output += f"None,{null_relation},None"
                    output += '}'
                    instruction = f'please extract the "{null_relation}" relation in following sentence ,output as format: '
                    instruction += "{head entity, relation type, tail entity}\n"
                    definition = f'"{null_relation}" refers to {relation_type_dict[null_relation]},'
                    explanation = f"In this sentence there is no entity of {null_relation} type."
                    instance = {"id":str(cur_id),"sentence":line['text'],"output":output,"ent_type":{"head_entity":[],"tail_entity":[]}
                                ,"instruction":instruction,"definition":definition,"explanation":explanation,"correction":"correction:"}
                    cur_id += 1
                    all_instances.append(instance)
                if cur_id > (dataset_ind + 1+ args.ner_dataset) * args.re_data_count_per_dataset:
                    print(f'cur_data_id:{cur_id} \n {dataset} {task} data load down ..')
                    break    
            if cur_id > (dataset_ind + 1 + args.ner_dataset) * args.re_data_count_per_dataset:        
                break    
            
    with open("/root/IE_LLM/Tk-Instruct/train_data/re/train.json",'w',encoding='utf-8') as f:
        json.dump({"Task":task,"Positive Examples":[],"Negative Examples":[],"Instances":all_instances},f)

cur_id = ner_data_process(0)
print(cur_id)
re_data_process(cur_id)

#re_data_process()
def create_relation_correction(model_pred,ground_truth,ent_type):
    #ent_des : {"object":set(),"subject":set()}
    correction = "correction:"
    format_error = "There is an error in the output format, please output as the format:(head_entity, relation, tail_entity)."
    completery_error = ""
    
    p = re.compile(r'({.*?})')
    model_pred = re.findall(p, model_pred)
    ground_truth = re.findall(p,ground_truth)
    
    if len(model_pred) == 0:
        return correction + format_error
    
    gold_relation_list = []
    for gold_triple in ground_truth:
        gold_triple = gold_triple[1:-1]
        temp = gold_triple.split(',')
        gold_relation_list.append((temp[0],temp[2]))
        
    if '([None],[None])' in gold_relation_list:
        no_relation = True
    else:
        no_relation = False
    
    relation_type = ground_truth[0][1:-1].split(',')[1]
    head_ent_type = (" or ").join(ent_type["head_entity"])
    tail_ent_type = (" or ").join(ent_type["tail_entity"])

    correction_judge = {"correct":"<answer> is correct.\n","uncorrect":"<answer> is not correct.Because "}
    correction_error_exp = {"reverse_error":"The head entity and the tail entity are reversed, please exchange the order of them.\n",
                            "head_error":{"judge":"the head entity <answer[0]> of relation <rel_type> is not correct,",
                                          "type_error":"the head entity type of <rel_type> relation can be <head_entity_type>,<answer[0]> is not a <head_entity_type> type entity.please change the head entity.",
                                          "uncomplete_error":"<answer[0]> is not a complete entity ,please expand the boundry of the <answer[0]> .",
                                          "redundant_error":"<answer[0]>  contains redundant words ,please narrow the boundry of  the <answer[0]>."},
                            "head_correct":"the head entity <answer[0]> of relation <rel_type> is  correct.",
                            "tail_error":{"judge":"the tail entity <answer[2]> of relation <rel_type> is not correct.",
                                          "type_error":"the head tail type of <rel_type> relation can be <tail_entity_type>,<answer[2]> is not a <tail_entity_type> type entity.please change the tail entity.\n",
                                          "uncomplete_error":"<answer[2]> is not a complete entity ,please expand the boundry of the <answer[2]> .\n",
                                          "redundant_error":"<answer[2]>  contains redundant words ,please narrow the boundry of  the <answer[2]>.\n"},
                            "tail_correct":"the tail entity <answer[2]> of relation <rel_type> is correct.\n"
                            }
    
    if len(model_pred) < len(ground_truth):
        completery_error = "there has other <rel_type> type entity,please extract it"
    
    format_problem = False
    
    for item in model_pred:
        item = item[1:-1]
        try:
            answer = item.split(',')
            assert(len(answer)==3)
            assert(answer[1] == relation_type)
        except:
            format_problem = True   
            continue
        
        cur_answer = (answer[0],answer[2])
        reverse_answer = (answer[2],answer[0])
        if cur_answer in gold_relation_list:
            correction += correction_judge["correct"].replace("<answer>",f'({item})')
        else:
            correction += correction_judge["uncorrect"].replace("<answer>",f'({item})')
            if reverse_answer in gold_relation_list:
                correction += correction_error_exp["reverse_error"]
            else:
                head_judge ,tail_judge = "",""
                head_entity = answer[0]
                tail_entity = answer[2]
                for element in gold_relation_list:
                    if head_entity == element[0]:
                        head_judge = "correct"
                        if tail_entity == element[1]:
                            tail_judge = "correct"
                        elif tail_entity in element[1]:
                            tail_judge = "uncomplete_error"
                        elif element[1] in tail_entity:
                            tail_judge = "redundant_error"
                        else:
                            tail_judge = "type_error"
                        break
                    elif tail_entity == element[1]:
                        tail_judge = "correct"
                        if head_entity in element[0]:
                            head_judge = "uncomplete_error"
                        elif element[0] in head_entity:
                            head_judge = "redundant_error"
                        else:
                            head_judge = "type_error"
                        break
                    elif head_entity in element[0] and (head_judge == "type_error" or head_judge==""):

                        head_judge = "uncomplete_error"
                        if tail_entity in element[1]:
                            tail_judge = "uncomplete_error"
                        elif element[1] in tail_entity:
                            tail_judge = "redundant_error"
                        else:
                            tail_judge = "type_error"
                            
                    elif tail_entity in element[1] and (tail_judge == "type_error" or tail_judge==""):
                        tail_judge = "uncomplete_error"
                        if element[0] in head_entity:
                            head_judge = "redundant_error"
                        else:
                            head_judge = "type_error"
                    elif element[0] in head_entity and (head_judge == "type_error" or head_judge==""):
                        head_judge = "redundant_error"
                        if element[1] in tail_entity:
                            tail_judge = "redundant_error"
                        else:
                            tail_judge = "type_error"
                    elif element[1] in tail_entity and (tail_judge == "type_error" or tail_judge==""):
                        tail_judge = "redundant_error"
                        head_judge = "type_error"
                    else:
                        if len(head_judge) == 0:
                            head_judge = "type_error"
                            tail_judge = "type_error"
                 
                if head_judge == "correct":
                    correction += correction_error_exp["head_correct"].replace("<answer[0]>",answer[0]).replace("<rel_type>",relation_type) 
                else:
                    correction +=  correction_error_exp["head_error"]["judge"].replace("<answer[0]>",answer[0]).replace("<rel_type>",relation_type)
                    correction +=  correction_error_exp["head_error"][head_judge].replace("<answer[0]>",answer[0]).replace("<rel_type>",relation_type).replace("<head_entity_type>",head_ent_type)
                if tail_judge == "correct":
                    correction += correction_error_exp["tail_correct"].replace("<answer[2]>",answer[2]).replace("<rel_type>",relation_type) 
                else:
                    correction +=  correction_error_exp["tail_error"]["judge"].replace("<answer[2]>",answer[2]).replace("<rel_type>",relation_type)
                    correction +=  correction_error_exp["tail_error"][tail_judge].replace("<answer[2]>",answer[2]).replace("<rel_type>",relation_type).replace("<tail_entity_type>",tail_ent_type)
                       
    if format_problem:
        correction += format_error
    correction += completery_error.replace("<rel_type>",relation_type) 
    return correction
                            
                    
def create_entity_correction(model_pred,ground_truth):
    correction = "correction:"
    format_error = "There is an error in the output format, please output (entity_name, instance of, entity_type)."
    completery_error = ""
    
    p = re.compile(r'({.*?})')
    model_pred = re.findall(p, model_pred)
    ground_truth = re.findall(p,ground_truth)
    
    if len(model_pred) == 0:
        return correction + format_error
    
    
    gold_entity_list = []
    for gold_triple in ground_truth:
        gold_triple = gold_triple[1:-1]
        gold_entity_list.append(gold_triple.split(',')[0])
    if '[None]' in gold_entity_list:
        no_entity = True
    else:
        no_entity = False

    type_error = "<answer[0]> is not a <ent_type> type entity.please change this answer."
    uncomplete_error = "<answer[0]> is not a complete entity ,please expand the boundry of <answer[0]>."
    redundant_error =  "<answer[0]>  contains redundant words ,please narrow the boundry of  <answer[0]>."
    ent_type = ground_truth[0][1:-1].split(',')[2]
    
    if len(model_pred) < len(ground_truth):
        completery_error = "there has other <ent_type> type entity,please extract it"
    
    format_problem = False
    
    for item in model_pred:
        try:
            item = item[1:-1]
            answer = item.split(',')
            assert(len(answer)==3)  
            assert(answer[1] == "instance of") 
            assert(answer[2] == ent_type)
        except:
            format_problem = True
            continue
        if no_entity:
            cur_correction = f"({answer[0]},{answer[1]},{answer[2]}) is not correct," + type_error.replace('<answer[0]>',answer[0]).replace('<ent_type>',ent_type)
            correction += cur_correction
            continue
        cur_correction = f"({answer[0]},{answer[1]},{answer[2]}) is not correct," + type_error.replace('<answer[0]>',answer[0]).replace('<ent_type>',ent_type)
        
        for gold_entity in gold_entity_list:
            if answer[0] == gold_entity:
                cur_correction = f"({answer[0]},{answer[1]},{answer[2]}) is correct.\n" 
                break
            elif answer[0] in gold_entity:
                cur_correction = f"({answer[0]},{answer[1]},{answer[2]}) is not correct,"  + uncomplete_error.replace('<answer[0]>',answer[0])
            elif gold_entity in answer[0]:
                cur_correction = f"({answer[0]},{answer[1]},{answer[2]}) is not correct," + redundant_error.replace('<answer[0]>',answer[0])
                
        correction += cur_correction
        correction += '\n'
                            
        
        
    if format_problem:
        correction += format_error
    correction += completery_error.replace('<ent_type>',ent_type)
    return correction

'''
model_pred = "{hamlet,instance of,Hamlet ( place )}{ham,instance of,Hamlet}{aa,instance of,Hamlet ( place } {aaa bb,instance of,Hamlet ( place }"
ground_truth = "{hamlet,instance of,Hamlet ( place )}{aaa,instance of,Hamlet ( place )}"
ent_type = {"head_entity":["b","c"],"tail_entity":["d","e"]}
correction = create_relation_correction(model_pred,ground_truth,ent_type)     
#correction = create_entity_correction(model_pred,ground_truth)
print(correction)   
 '''              
#ner correction
'''
 {
                    (<answer> is correct.|
                     <answer> is not correct.
                            (<answer[0]> is not a <ent_type> type entity.please change this answer.|
                            <answer[0]> is not a complete entity ,please expand the boundry of <answer[0]>.|
                            <answer[0]>  contains redundant words ,please narrow the boundry of  <answer[0]>.))
                    }
                    [there has other <ent_type> type entity,please extract it]
                    
                    


{
                    (<answer> is correct.|
                    <answer> is not correct.
                            (
                            The head entity and the tail entity are reversed, please exchange the order of them.|
                            (the head entity <answer[0]> of relation <rel_type> is  correct.|
                            the head entity <answer[0]> of relation <rel_type> is not correct.)
                                    (the head entity of <rel_type> can be a <obj_type> type entity,<answer[0]> is not a <obj_type> type entity.please change head entity|
                                    <answer[0]> is not a complete entity ,please expand the boundry of <answer[0]>.|
                                    <answer[0]>  contains redundant words ,please narrow the boundry of <answer[0]>.)
                            (the tail entity <answer[2]> of relation <rel_type> is  correct.|
                            the tail entity <answer[2]> of relation <rel_type> is not correct.)
                                    (the tail entity of <rel_type> can be a <subj_type> type entity,<answer[2]> is not a <subj_type> type entity.please change tail entity|
                                    <answer[2]> is not a complete entity ,please expand the boundry of <answer[2]>.|
                                    <answer[2]>  contains redundant words ,please narrow the boundry of <answer[2]>.)
                            )
                    )
                    }
                    [there has other <rel_type> type relation,please extract it]
                    
                    
instruction:[Hamlet ( place ) refers to small settlement in a rural area] ,please extract the  Hamlet ( place ) entity in following sentence \n
input: The hamlet of Kayupovo is located in Russia.

analysis:Kayupovo refers to human settlement in Belokataysky District, Republic of Bashkortostan, Russia, so Kayupovo is a Hamlet ( place ) entity.
output:(Kayupovo,instance of,Hamlet ( place ))

第一轮：
输入：
instruction:[Hamlet ( place ) refers to small settlement in a rural area] ,please extract the  Hamlet ( place ) entity in following sentence \n
input: The hamlet of Kayupovo is located in Russia.
输出：
answer:(hamlet,instance of,Hamlet ( place ))
计算loss时的参考：(Kayupovo,instance of,Hamlet ( place ))

第二轮：
输入：
instruction:[Hamlet ( place ) refers to small settlement in a rural area] ,please extract the  Hamlet ( place ) entity in following sentence \n
input: The hamlet of Kayupovo is located in Russia.
answer：(hamlet,instance of,Hamlet ( place ))
Is the answer above correct ? if there are some error in it , how can it be corrected ?
输出：
correction:hamlet is not correct,hamlet is not a Hamlet(place) entity.please change this answer.[there has other Hamlet ( place ) entity,please extract it]
answer:(Russia,instance of,Hamlet ( place ))
计算loss参考：
hamlet is not correct,hamlet is not a Hamlet(place) entity.please change this answer.
answer:(Kayupovo,instance of,Hamlet ( place ))

第三轮：
输入：
instruction:[Hamlet ( place ) refers to small settlement in a rural area] ,please extract the  Hamlet ( place ) entity in following sentence \n
input: The hamlet of Kayupovo is located in Russia.
answer：(hamlet,instance of,Hamlet ( place ))

hamlet is not correct,hamlet is not a Hamlet(place) entity.please change this answer.
answer:(Kayupo,instance of,Hamlet ( place ))
Is the answer above correct ? if there are some error in it , how can it be corrected ?
输出：
correction:Kayupo is not correct,Kayupo isn't a complete entity ,please expand the boundry of Kayupo. [there has other Hamlet ( place ) entity,please extract it]
answer:(of Kayupovo,instance of ,Hamlet ( place ))

计算loss参考：
Kayupo is not correct,Kayupo isn't a complete entity ,please expand the boundry of Kayupo.
answer:(Kayupovo,instance of,Hamlet ( place ))

第四轮:
instruction:[Hamlet ( place ) refers to small settlement in a rural area] ,please extract the  Hamlet ( place ) entity in following sentence \n
input: The hamlet of Kayupovo is located in Russia.
answer：(hamlet,instance of,Hamlet ( place ))

hamlet is not correct,hamlet is not a Hamlet(place) entity.please change this answer.
answer:(Kayupo,instance of,Hamlet ( place ))

Kayupo is not correct,Kayupo isn't a complete entity ,please expand the boundry of Kayupo.
answer(of Kayupovo,instance of ,Hamlet ( place ))

Is the answer above correct ? if there are some error in it , how can it be corrected ?
输出：
correction:Kayupo is not correct,of Kayupovo  contains redundant words ,please narrow the boundry of Kayupo.[there has other Hamlet ( place ) entity,please extract it]
answer(Kayupovo,instance of ,Hamlet ( place ))

计算loss参考：
Kayupo is not correct,of Kayupovo  contains redundant words ,please narrow the boundry of Kayupo.
answer(Kayupovo,instance of ,Hamlet ( place ))



"text": "Batna (Arabic: ) is a wilaya of Algeria",
"relations":

"instance": [{"object": "Algeria", "subject": "Batna", "obj_type": "sovereign state", "subj_type": "province of Algeria", "obj_type_des": "political organization with a centralized independent government", "subj_type_des": "type of administrative entity", "obj_des": "country in North Africa", "subj_des": "province of Algeria"}, 
{"object": "Algeria", "subject": "Batna", "obj_type": "sovereign state", "subj_type": "province of Algeria", "obj_type_des": "political organization with a centralized independent government", "subj_type_des": "type of administrative entity", "obj_des": "country in North Africa", "subj_des": "province of Algeria"}]}, 
"located in the administrative territorial entity": {"des": "the item is located on the territory of the following administrative entity. Use P276 for specifying locations that are non-administrative places and for items about events. Use P1382 if the item falls only partially into the administrative entity.",
"instance": [{"object": "Algeria", "subject": "Batna", "obj_type": "sovereign state", "subj_type": "province of Algeria", "obj_type_des": "political organization with a centralized independent government", "subj_type_des": "type of administrative entity", "obj_des": "country in North Africa", "subj_des": "province of Algeria"},
{"object": "Algeria", "subject": "Batna", "obj_type": "sovereign state", "subj_type": "province of Algeria", "obj_type_des": "political organization with a centralized independent government", "subj_type_des": "type of administrative entity", "obj_des": "country in North Africa", "subj_des": "province of Algeria"}]},
"contains the administrative territorial entity": {"des": "(list of) direct subdivisions of an administrative territorial entity",
"instance": [{"object": "Batna", "subject": "Algeria", "obj_type": "province of Algeria", "subj_type": "sovereign state", "obj_type_des": "type of administrative entity", "subj_type_des": "political organization with a centralized independent government", "obj_des": "province of Algeria", "subj_des": "country in North Africa"}]}}}
第一轮：
输入
instruction:country is a relation refers to sovereign state that this item is in (not to be used for human beings), please extract the  country relation in following sentence \n
input:Batna (Arabic: ) is a wilaya of Algeria
输出：
answer：（Arabic,country,Batna）(A,country,B)
loss：
answer：（Algeria,country,Batna）

第二轮：
输入
instruction:country is a relation refers to sovereign state that this item is in (not to be used for human beings), please extract the  country relation in following sentence \n
input:Batna (Arabic: ) is a wilaya of Algeria
answer:(Algeria,country,Batna）(A,country,B)
Is the answer above correct ? if there are some error in it , how can it be corrected ?
输出：
correction:(Algeria,country,Batna）is not correct.
the head entity [Algeria] of this answer is not correct,[ the head entity of [country] can be a [sovereign state] type entity,[不是。。]]|[rebujmm]





'''