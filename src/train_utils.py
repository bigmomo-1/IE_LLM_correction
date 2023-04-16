 
import torch
#from deepspeed import is_deepspeed_zero3_enabled
import random
import transformers
import os
import re
import numpy as np
import functools
from transformers.utils.import_utils import BACKENDS_MAPPING

def seed_worker(_):
        """
        Helper function to set worker seed during Dataloader initialization.
        """
        worker_seed = torch.initial_seed() % 2**32
        set_seed(worker_seed)

def create_relation_correction(model_pred,ground_truth,ent_type):
    #ent_des : {"object":set(),"subject":set()}
    correction = "correction:"
    format_error = "There is an error in the output format, please output as the format:{head_entity, relation, tail_entity}."
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
    if "(None,None)" in gold_relation_list:
        no_raletion = True
    else:
        no_raletion = False
    #correction += "In this sentence there is no entity of a type, so the output is (None, instance of, type)"
        

    
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
            if no_raletion:
                correction += f'In this sentence there is no "{answer[1]}" relation.'
                continue
        
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
    format_error = "There is an error in the output format, please output {entity_name, instance of, entity_type}."
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
    if 'None' in gold_entity_list:
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
            cur_correction = f"({answer[0]},{answer[1]},{answer[2]}) is not correct,In this sentence there is no entity of {answer[2]} type." 
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


def set_seed(seed: int):
    """
    Helper function for reproducible behavior to set the seed in `random`, `numpy`, `torch` and/or `tf` (if installed).
    Args:
        seed (`int`): The seed to set.
    """
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    # ^^ safe to call this function even if cuda is not available

def enable_full_determinism(seed: int):
    """
    Helper function for reproducible behavior during distributed training. See
    - https://pytorch.org/docs/stable/notes/randomness.html for pytorch
    - https://www.tensorflow.org/api_docs/python/tf/config/experimental/enable_op_determinism for tensorflow
    """
    # set seed first
    set_seed(seed)

    # Enable PyTorch deterministic mode. This potentially requires either the environment
    # variable 'CUDA_LAUNCH_BLOCKING' or 'CUBLAS_WORKSPACE_CONFIG' to be set,
    # depending on the CUDA version, so we set them both here
    os.environ["CUDA_LAUNCH_BLOCKING"] = "1"
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":16:8"
    torch.use_deterministic_algorithms(True)

    # Enable CUDNN deterministic mode
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False



def requires_backends(obj, backends):
    if not isinstance(backends, (list, tuple)):
        backends = [backends]

    name = obj.__name__ if hasattr(obj, "__name__") else obj.__class__.__name__

    checks = (BACKENDS_MAPPING[backend] for backend in backends)
    failed = [msg.format(name) for available, msg in checks if not available()]
    if failed:
        raise ImportError("".join(failed))


def find_executable_batch_size(
    function: callable = None, starting_batch_size: int = 128, auto_find_batch_size: bool = False
):
    """
    Args:
    A basic decorator that will try to execute `function`. If it fails from exceptions related to out-of-memory or
    CUDNN, the batch size is cut in half and passed to `function` `function` must take in a `batch_size` parameter as
    its first argument.
        function (`callable`, *optional*)
            A function to wrap
        starting_batch_size (`int`, *optional*)
            The batch size to try and fit into memory
        auto_find_batch_size (`bool`, *optional*)
            If False, will just execute `function`
    """
    if function is None:
        return functools.partial(
            find_executable_batch_size,
            starting_batch_size=starting_batch_size,
            auto_find_batch_size=auto_find_batch_size,
        )

    if auto_find_batch_size:
        requires_backends(find_executable_batch_size, "accelerate")
        from accelerate.utils import find_executable_batch_size as accelerate_find_executable_batch_size

        return accelerate_find_executable_batch_size(function=function, starting_batch_size=starting_batch_size)

    return functools.partial(function, batch_size=starting_batch_size)

