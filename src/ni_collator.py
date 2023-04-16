import logging
import random
import string
from transformers.data.data_collator import *

logger = logging.getLogger(__name__)


@dataclass
class DataCollatorForIE:

    tokenizer: PreTrainedTokenizerBase
    model: Optional[Any] = None
    padding: Union[bool, str, PaddingStrategy] = True
    max_source_length: Optional[int] = None
    max_target_length: Optional[int] = None
    pad_to_multiple_of: Optional[int] = None
    label_pad_token_id: int = -100
    return_tensors: str = "pt"
    add_task_name: bool = False
    add_task_instruction: bool = True
    add_type_definition: bool = False
    add_explanation: bool = False
    add_correction: bool = False
    num_pos_examples: int = 0
    num_neg_examples: int = 0
    try_all: bool = False
    text_only: bool=False
    

    def __call__(self, batch, return_tensors=None):

        if return_tensors is None:
            return_tensors = self.return_tensors

        sources ,targets = [],[]
        for instance in batch:
            if self.try_all:
                all_valid_encodings = [
                    # instruction only
                    {"add_task_name": False, "add_task_instruction":True, "add_type_definition": False, "add_explanation":False,"add_correction":False,"num_pos_examples": 0, "num_neg_examples": 0}, 
                    # instruction + defination
                    {"add_task_name": False, "add_task_instruction":True, "add_type_definition": True, "add_explanation":False,"add_correction":False,"num_pos_examples": 0, "num_neg_examples": 0}, 
                    # instruction + defination + explanation
                    {"add_task_name": False, "add_task_instruction":True, "add_type_definition": True, "add_explanation":True,"add_correction":False,"num_pos_examples": 0, "num_neg_examples": 0}, 
                    # instruction + defination + correction
                    {"add_task_name": False, "add_task_instruction":True, "add_type_definition": True, "add_explanation":False,"add_correction":True,"num_pos_examples": 0, "num_neg_examples": 0}, 
                    # instruction + defination + pos examples + explanation
                    {"add_task_name": False, "add_task_instruction":True, "add_type_definition": True, "add_explanation":True,"add_correction":False,"num_pos_examples": 2, "num_neg_examples": 0}, 
                    # instruction + defination + pos examples + correction
                    {"add_task_name": False, "add_task_instruction":True, "add_type_definition": True, "add_explanation":False,"add_correction":True,"num_pos_examples": 2, "num_neg_examples": 0}, 
                    # instruction + defination + pos examples + neg examples  
                    #{"add_task_name": False, "add_task_definition": True, "num_pos_examples": 2, "num_neg_examples": 2, "add_explanation": False},
                    # instruction + pos (w. explanation) 
                    #{"add_task_name": False, "add_task_definition": True, "num_pos_examples": 2, "num_neg_examples": 0, "add_explanation": True}, 
                ]
                #encoding_schema = random.choice(all_valid_encodings)
                encoding_schema_list = all_valid_encodings
            else:
                encoding_schema = {"add_task_name": self.add_task_name, "add_task_instruction":self.add_task_instruction, "add_type_definition": self.add_type_definition, "add_explanation":self.add_explanation,"add_correction":self.add_correction,"num_pos_examples": self.num_pos_examples, "num_neg_examples": self.num_neg_examples}
                encoding_schema_list = [encoding_schema]
            for encoding_schema in encoding_schema_list:
                add_task_name = encoding_schema['add_task_name']
                add_task_instruction = encoding_schema['add_task_instruction']
                add_type_definition = encoding_schema['add_type_definition']
                add_explanation = encoding_schema['add_explanation']
                add_correction = encoding_schema['add_correction']
                num_pos_examples = encoding_schema['num_pos_examples']
                num_neg_examples = encoding_schema['num_neg_examples']
                 
                task_input = ""
                # add the input first.
                
                ##????
                #task_input += "Now complete the following example -\n"
                task_input += instance['Instance']['instruction']
                
                task_input += f"Input: {instance['Instance']['sentence'].strip()}"
                if not task_input[-1] in string.punctuation:
                    task_input += "."
                task_input += "\n"
                #task_input += "Output: "
                
                #task_name = ""
                #if add_task_name:
                #    task_name += instance["Task"] + ". "
                explanation = ""
                if add_explanation:
                    explanation = "analysis:"+instance['Instance']['explanation']
                
                correction = ""
                input_context = ""
                correction_instruction = ""
                if add_correction:
                    correction = instance['Instance']['correction']
                    try:
                        input_context = instance['Instance']['input_context']
                        correction_instruction = instance['Instance']['correction_instruction']
                    except:
                        pass
                        #print("now is the epoch 0 prohcessing")
                definition = ""
                if add_type_definition:
                    definition = instance['Instance']["definition"].strip()
                    if not definition[-1] in string.punctuation:
                        definition += "."
                
                # try to add positive examples.
                pos_examples = []
                for idx, pos_example in enumerate(instance["Positive Examples"][:num_pos_examples]):
                    pos_example_str = f" Positive Example {idx+1} -\n"
                    pos_example_str += f"Input: {pos_example['Instance']['sentence'].strip()}"
                    if not pos_example_str[-1] in string.punctuation:
                        pos_example_str += "."
                    pos_example_str += "\n"
                    pos_example_str += f" Output: {pos_example['Instance']['output'].strip()}"
                    if not pos_example_str[-1] in string.punctuation:
                        pos_example_str += "."
                    pos_example_str += "\n" 
                    if add_explanation and "explanation" in pos_example:
                        pos_example_str += f" Explanation: {pos_example['Instance']['explanation'].strip()}"
                        if not pos_example_str[-1] in string.punctuation:
                            pos_example_str += "."
                        pos_example_str += "\n"
                    pos_example_str += "\n"
                    if len(self.tokenizer(definition + " ".join(pos_examples) + pos_example_str + task_input)["input_ids"]) <= self.max_source_length:
                        pos_examples.append(pos_example_str)
                    else:
                        break
                
                # try to add negative examples.
                neg_examples = []
                for idx, neg_example in enumerate(instance["Negative Examples"][:num_neg_examples]):
                    neg_example_str = f" Negative Example {idx+1} -\n"
                    neg_example_str += f"Input: {neg_example['Instance']['sentence'].strip()}"
                    if not neg_example_str[-1] in string.punctuation:
                        neg_example_str += "."
                    neg_example_str += "\n"
                    neg_example_str += f" Output: {neg_example['Instance']['output'].strip()}"
                    if not neg_example_str[-1] in string.punctuation:
                        neg_example_str += "."
                    neg_example_str += "\n"
                    if add_explanation and "explanation" in neg_example:
                        neg_example_str += f" Explanation: {neg_example['Instance']['explanation'].strip()}"
                        if not neg_example_str[-1] in string.punctuation:
                            neg_example_str += "."
                        neg_example_str += "\n"
                    neg_example_str += "\n"
                    if len(self.tokenizer(definition + " ".join(pos_examples) + " ".join(neg_examples) + neg_example_str + task_input)["input_ids"]) <= self.max_source_length:
                        neg_examples.append(neg_example_str)
                    else:
                        break 
                
                source = "".join(pos_examples) + "".join(neg_examples) + definition + task_input + input_context + correction_instruction
                print(source)
                tokenized_source = self.tokenizer(source)["input_ids"]
                if len(tokenized_source) <= self.max_source_length:
                    sources.append(source)
                else:
                    sources.append(self.tokenizer.decode(tokenized_source[:self.max_source_length], skip_special_tokens=True))
                
                
                if "output" in instance['Instance'] and instance["Instance"]["output"]:
                    target = explanation + correction + "\noutput:" + instance["Instance"]["output"]
                    #print(target)
                    tokenized_target = self.tokenizer(target)["input_ids"]
                    if len(tokenized_target) <= self.max_target_length:
                        targets.append(target)
                    else:
                        targets.append(self.tokenizer.decode(tokenized_target[:self.max_target_length], skip_special_tokens=True))
                    
                    
                    
                    

        if self.text_only:
            model_inputs = {"inputs": sources}
        else:
            model_inputs = self.tokenizer(
                sources, 
                max_length=self.max_source_length, 
                padding=self.padding,
                return_tensors=self.return_tensors, 
                truncation=True,
                pad_to_multiple_of=self.pad_to_multiple_of)

        if len(targets) > 0:
            if self.text_only:
                model_inputs["labels"] = targets
            else:
                with self.tokenizer.as_target_tokenizer():
                    labels = self.tokenizer(
                        targets,
                        max_length=self.max_target_length,
                        padding=self.padding,
                        return_tensors=self.return_tensors,
                        truncation=True,
                        pad_to_multiple_of=self.pad_to_multiple_of
                    )
                label_mask = labels["attention_mask"].bool()
                model_inputs["labels"] = labels["input_ids"].masked_fill(~label_mask, self.label_pad_token_id)
        else:
            model_inputs["labels"] = None

        # prepare decoder_input_ids
        if self.model is not None and hasattr(self.model, "prepare_decoder_input_ids_from_labels") and not self.text_only:
            decoder_input_ids = self.model.prepare_decoder_input_ids_from_labels(labels=model_inputs["labels"])
            model_inputs["decoder_input_ids"] = decoder_input_ids
            
        return model_inputs