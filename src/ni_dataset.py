# coding=utf-8
# Copyright 2020 The TensorFlow Datasets Authors and the HuggingFace Datasets Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Lint as: python3
"""Natural Instruction V2 Dataset."""


import json
import os
import random
import datasets

logger = datasets.logging.get_logger(__name__)

_CITATION = """
@article{wang2022benchmarking,
  title={Benchmarking Generalization via In-Context Instructions on 1,600+ Language Tasks},
  author={Wang, Yizhong and Mishra, Swaroop and Alipoormolabashi, Pegah and Kordi, Yeganeh and others},
  journal={arXiv preprint arXiv:2204.07705},
  year={2022}
}
"""

_DESCRIPTION = """
Natural-Instructions v2 is a benchmark of 1,600+ diverse language tasks and their expert-written instructions. 
It covers 70+ distinct task types, such as tagging, in-filling, and rewriting. 
These tasks are collected with contributions of NLP practitioners in the community and 
through an iterative peer review process to ensure their quality. 
"""


class IEConfig(datasets.BuilderConfig):
    def __init__(self, *args,  task_list = None,max_num_instances_per_task=None, max_num_instances_per_eval_task=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_list: str = task_list
        self.max_num_instances_per_task: int = max_num_instances_per_task
        self.max_num_instances_per_eval_task: int = max_num_instances_per_eval_task


class IE_Data(datasets.GeneratorBasedBuilder):
    """NaturalInstructions Dataset."""

    VERSION = datasets.Version("1.0.0")
    BUILDER_CONFIG_CLASS = IEConfig
    BUILDER_CONFIGS = [
        IEConfig(name="default", description="Default config for IE data")
    ]
    DEFAULT_CONFIG_NAME = "default"

    def _info(self):
        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features=datasets.Features(
                {
                    "id": datasets.Value("string"),
                    "Task": datasets.Value("string"),
                    "Positive Examples": [{
                        "input": datasets.Value("string"),
                        "output": datasets.Value("string"),
                        #"explanation": datasets.Value("string")
                    }],
                    "Negative Examples": [{
                        "input": datasets.Value("string"),
                        "output": datasets.Value("string"),
                        #"explanation": datasets.Value("string")
                    }],
                    "Instance": {
                        "id": datasets.Value("string"),
                        "sentence":datasets.Value("string"),
                        "output": datasets.Value("string"),
                        "ent_type":{
                            "head_entity":[datasets.Value("string")],
                            "tail_entity":[datasets.Value("string")]
                            },
                        "instruction":datasets.Value("string"),
                        "definition":datasets.Value("string"),
                        "explanation":datasets.Value("string"),
                        "correction":datasets.Value("string")
                       
                    }
                }
            )
        )

    def _split_generators(self, dl_manager):
        """Returns SplitGenerators."""
        if self.config.data_dir is None  :
            raise ValueError("data_dir should be set!!")

        data_dir = self.config.data_dir
        task_list = self.config.task_list if self.config.task_list != None else ['ner','re']
        '''
           ,
            datasets.SplitGenerator(
                name=datasets.Split.TEST,
                gen_kwargs={
                    "path": data_dir, 
                     "task_list": task_list, 
                    "mode":"test",
                    "max_num_instances_per_task": self.config.max_num_instances_per_eval_task,
                    "subset": "test"
                }),'''
        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={
                    "path": data_dir, 
                    "task_list": task_list, 
                    "mode":"train",
                    "max_num_instances_per_task": self.config.max_num_instances_per_task,
                    "subset": "train"
                })  ,
            datasets.SplitGenerator(
                name=datasets.Split.VALIDATION,
                gen_kwargs={
                    "path": data_dir, 
                    "task_list": task_list, 
                    "mode":"validation",
                    "max_num_instances_per_task": self.config.max_num_instances_per_eval_task,
                    "subset": "dev"
                })
            ]
            
       

    def _generate_examples(self, path=None, task_list=None, mode=None, max_num_instances_per_task=None, subset=None):
        """Yields examples."""
        if mode == "validation" or mode == "test":
            for task_name in task_list:
                input_path = f'{path}/{task_name}/{mode}/'
                file_paths = os.listdir(input_path)
                for file_path in file_paths:
                    temp = file_path.split('.')[0]
                    cur_task_name = f'{task_name}_{temp}'
                    logger.info(f"Generating {mode} {cur_task_name} data ")
                    
                    with open(input_path+file_path,'r', encoding="utf-8") as f:
                        s = f.read()
                        task_data = json.loads(s)
                        task_data["Task"] = cur_task_name
                        all_instances = task_data.pop("Instances")
                        if subset == "test":
                            # for testing tasks, 100 instances are selected for efficient evaluation and they are label-balanced.
                            # we put them in the first for reproducibility.
                            # so, we use them here
                            instances = all_instances[:100]
                        else:
                            instances = all_instances
                        if max_num_instances_per_task is not None and max_num_instances_per_task >= 0:
                            random.shuffle(instances)
                            instances = instances[:max_num_instances_per_task]
                        for idx, instance in enumerate(instances):
                            example = task_data.copy()
                            example["id"] = instance["id"]
                            example["Instance"] = instance
                            yield f"{cur_task_name}_{idx}", example
            
        else:
            
            for task_name in task_list:
                input_path = f'{path}/{task_name}/{mode}.json'
                logger.info(f"Generating tasks from ={input_path} ")
                with open(input_path,'r', encoding="utf-8") as f:
                    s = f.read()
                    task_data = json.loads(s)
                    task_data["Task"] = task_name
                    all_instances = task_data.pop("Instances")
                    if subset == "test":
                        # for testing tasks, 100 instances are selected for efficient evaluation and they are label-balanced.
                        # we put them in the first for reproducibility.
                        # so, we use them here
                        instances = all_instances[:100]
                    else:
                        instances = all_instances
                    if max_num_instances_per_task is not None and max_num_instances_per_task >= 0:
                        random.shuffle(instances)
                        instances = instances[:max_num_instances_per_task]
                    for idx, instance in enumerate(instances):
                        example = task_data.copy()
                        example["id"] = instance["id"]
                        example["Instance"] = instance
                        yield f"{task_name}_{idx}", example

