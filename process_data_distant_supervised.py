import pymongo
import json
import argparse
import os
import requests
import time


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient.tacgen
database_20M = mydb.database_0M_des
database_40M = mydb.database_40M_des
database_60M = mydb.database_70M_des
#database_100M = mydb.database_100M
entity_db = mydb.entity_db
#mytable.delete_many({})
parser = argparse.ArgumentParser()
parser.add_argument('--database',type=int,default = 0)
parser.add_argument('--bkpt',type=int,default = 0)
parser.add_argument('--end',type=int,default= 10)
parser.add_argument('--log',type=int,default= 5)
parser.add_argument('--dataset',type=str,default= "None")
args = parser.parse_args()
proxies = {
        'http': 'http://127.0.0.1:7890',  # 代理服务器地址，这里假设是本地1087端口
        'https': 'http://127.0.0.1:7890',
    }
#mytable.drop_index('id_1') 
#db.test_database.create_index([("id",1)],background=True)
#db.test_database.create_index([("labels",1)],background=True)
collection_name = f'database_{args.database}M_des'

mytable = myclient.tacgen.get_collection(collection_name)
mytable.create_index([("id",1)],background=True)
relation_desdb = mydb.relation_des
relation_desdb.create_index([("relation",1)],background=True)


def insert_documents():
    #json_list = []
    repeate_num = 0
    st_time = time.time()
    add_des = 0
    with open("latest-all.json",'r',encoding='utf-8') as f:
        for i in range(0,args.bkpt):
            f.readline()
            if i % 1000000 == 0:
                end_time = time.time()
                print(f"cur:{i},time_consuming:{(end_time-st_time)/60}")
                st_time = time.time()
        end_time = time.time()
        print(f'reading the line {args.bkpt} consuming time:{(end_time-st_time)/60}')
        for ind in range(args.bkpt,args.end):
            if ind % 1000000 == 0:
                end_time = time.time()
                #print(f"cur_description:{add_des},time_consuming:{(end_time-st_time)/60}")
                print(f"cur_description:{ind},time_consuming:{(end_time-st_time)/60}")
                st_time = time.time()
            try:
                line = f.readline()
                
            except:
                print(f'cur_line:{ind}')

            #try:
                #for item in line["claims"]["P31"]:
                    #instance_of_list.append(item["mainsnak"]["datavalue"]["value"]["id"])
            #except:
                #instance_of_list.append("item")
            
            line = json.loads(line.strip()[:-1])
            '''
            myquery = {'id':line["id"]}
            try:
                des = {"en": line["descriptions"]["en"]}
                newvalues = { "$set": { "descriptions":des } }
            except:
                newvalues = { "$set": { "descriptions":None } }
            try:
                mytable.update_one(myquery,newvalues)
                add_des += 1
            except:
                entity = mytable.find({'id':line["id"]})
                if entity == None:
                    try:
                        mytable.insert_one(line) 
                    except:
                        pass
            '''           
           
            
            try:
                new_line = {"type":line["type"],"id":line["id"],"labels":{'en':line["labels"]["en"]},
                    "claims":{"P31":line["claims"]["P31"]},"descriptions":{"en": line["descriptions"]["en"]}}  
            except:
                new_line = line
            try:
                mytable.insert_one(new_line) 
            except:
                repeate_num += 1
                pass
            
            #except:
            #    error_num += 1
            #    pass
            

        #repeat_num += 1
    #print('error_num'+str(error_num))
    print('repeate_num'+str(repeate_num))

    print('load down ..')
    #
    #mydict = { "name": "RUNOOB", "alexa": "10000", "url": "https://www.runoob.com" }
    
    #x = mycol.insert_one(mydict) 
#insert_documents()
def find_entity_des_in_muti_database(name):
    temp= relation_desdb.find_one({'relation':name})
    try:
        return temp['des']
    except:
        return "error"
def find_in_muti_database(id):
    temp = database_20M.find_one({"id":id})
    if temp == None:
        temp = database_40M.find_one({"id":id})
    if temp == None:
        temp = database_60M.find_one({"id":id})
    return temp
def find_relation_type(relation_id):
    relation = find_in_muti_database(relation_id)
    if relation == None:
        store = find_by_api(relation_id)
        try:
            new_line = {"id":relation_id,"labels":{'en':store["entities"][relation_id]['labels']['en']},"claims":{"P31":None},"descriptions":{"en": store["entities"][relation_id]["descriptions"]["en"]}}
            database_20M.insert_one(new_line)
            return (store["entities"][relation_id]['labels']['en']['value'],store["entities"][relation_id]['descriptions']['en']['value'])
        except:
            return ("error","error")
    else:
        try:
            return (relation['labels']['en']['value'],relation['descriptions']['en']['value'])
        except:
            return ("error","error")
        
    
def  find_by_api(id):
    print("search_api....")
    url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={id}&format=json"
    connection_time = 0
    while True:
        connection_time += 1
        query = "success"
        try:
            response = requests.get(url=url, proxies=proxies)
            time.sleep(1.5)
        except :
            query = "fail"
            print("fail")
            time.sleep(3)
        if query == "success":
            break
        elif connection_time > 2:
            return "error"
    store = json.loads(response.text)
    return store
                   
def find_entity_type(entity_id):
    entity = find_in_muti_database(entity_id)
    if entity == None:
        store = find_by_api(entity_id)
        try:
            entity_des = store["entities"][entity_id]["descriptions"]["en"]["value"]
            if "P31" in store["entities"][entity_id]["claims"]:
                entity_type = store["entities"][entity_id]["claims"]["P31"]
                instance_of_list = entity_type
                new_line = {"id":entity_id,"labels":{'en':store["entities"][entity_id]['labels']['en']},"claims":{"P31":entity_type},"descriptions":{"en": store["entities"][entity_id]["descriptions"]["en"]}}
                database_20M.insert_one(new_line)
            else:
                try:
                    
                    new_line = {"id":entity_id,"labels":{'en':store["entities"][entity_id]['labels']['en']},"claims":{"P31":None},"descriptions":{"en":store["entities"][entity_id]["descriptions"]["en"]}}
                except:
                    new_line = {"id":entity_id,"labels":{'en':store["entities"][entity_id]['labels']['en']},"claims":{"P31":None},"descriptions":{"en":None}}
                database_20M.insert_one(new_line)
                return ("error","error","error")
            
        except:
            return ("error","error","error")
    else:
        try:
            entity_des = entity["descriptions"]["en"]["value"]
            instance_of_list = entity['claims']['P31']
        except :
            return ("error","error","error")
        if entity_des == None or instance_of_list == None:
            return ("error","error","error")
            
 
        
    info = "error"
    type_des = "error"
    for item in instance_of_list:
        try:
            type_id = item["mainsnak"]["datavalue"]["value"]["id"]
            type_item = find_in_muti_database(type_id)
        except:
            return ("error","error","error")
        if type_item == None:     
            # 访问
            store = find_by_api(type_id)
            try:
                if "P31" in store["entities"][type_id]["claims"]:
                    entity_type = store["entities"][type_id]["claims"]["P31"]
                    new_line = {"id":type_id,"labels":{'en':store["entities"][type_id]['labels']['en']},"claims":{"P31":entity_type},"descriptions":{"en": store["entities"][type_id]["descriptions"]["en"]}}
                    #print(new_line)                  
                else:
                    new_line = {"id":type_id,"labels":{'en':store["entities"][type_id]['labels']['en']},"claims":{"P31":None},"descriptions":{"en":store["entities"][type_id]["descriptions"]["en"]}}   
                database_20M.insert_one(new_line)
                info = store["entities"][type_id]['labels']['en']['value']
                type_des = store["entities"][type_id]['descriptions']['en']['value']
            except:
                info = "error"
                type_des = "error"
        else:
            try:
                info = type_item["labels"]["en"]["value"]
                type_des = type_item["descriptions"]["en"]["value"]
            except:
                info = "error"
                type_des = "error"
        
        if info != "error" and type_des != "error":
            return (info,type_des,entity_des)
    return (info,type_des,entity_des)

#insert_documents()
    
def find_entity_name(entity_name):
    entity = entity_db.find_one({'name':entity_name})
    try:
        return entity['id']
    except:
        #url =f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={entity_name}&language=en&limit=1&format=json"
        #store = requests.get(url=url,proxies=proxies).text
        #store = json.loads(store)
        #try:
        #    return store['serach'][0]['id']
        #except:
        return "error"
   
def process_TRex():
    path = "/root/IE_LLM/TREx/"
    file_list = os.listdir(path)
    for file_name in file_list[400:]:
        ner_list,re_list = list(),list()
        cur_sent_id = 0
        with open(path+file_name,"r",encoding="utf-8") as f:
            lines = json.load(f)
        print(len(lines))
        lines = lines[args.bkpt:]
        st_time = time.time()
        for ind,line in enumerate(lines):
            if ind!=0 and ind % 5000 == 0:
                print(f'current_process_line:{ind}     re_list_length:{len(re_list)}   ner_list_length:{len(ner_list)}')
                with open(f"/root/IE_LLM/TREx_data/end{args.bkpt+ind}_re_{file_name}.json",'w',encoding='utf-8') as f:
                    for item in re_list:
                        json.dump(item,f)
                        f.write('\n')
                with open(f"/root/IE_LLM/TREx_data/end{args.bkpt+ind}_ner_{file_name}.json",'w',encoding='utf-8') as f:
                    for item in ner_list:
                        json.dump(item,f)
                        f.write('\n')
                end_time = time.time()
                print(f"time:{(end_time-st_time)/60}")
                re_list,ner_list = [],[]

            cur_ner_dict,cur_re_dict = {},{}
            entity_dict = {}
            text_offset = [0]
            words_boundry = line['words_boundaries']
            doc_text = line['text']
            texts = doc_text.split('. ')
            cur_offset = 0
            for text_ind,text in enumerate(texts):
                if not text.endswith('.'):
                    text += '. '
                if text_ind < len(texts)-1:
                    text_offset.append(text_offset[text_ind]+len(text))
                #print(text)

            '''
            for boundry in words_boundry:
                if boundry[1]-boundry[0] == 1 and doc_text[boundry[0]] == '.':
                    if boundry[1] == len(doc_text) or 
                    texts.append(doc_text[cur_offset:boundry[1]])
                    print(doc_text[cur_offset:boundry[1]])
                    text_offset.append(cur_offset)
                    cur_offset = boundry[1]
            '''

                    
            
            entities = line['entities']
            triples = line['triples']

            for entity in entities:
                boundry = entity['boundaries']
                for sent_id,offset in enumerate(text_offset):
                    if boundry[0] < offset:
                        sent_id = sent_id - 1
                        break
                
                cur_start,cur_end =boundry[0]-text_offset[sent_id],boundry[1]-text_offset[sent_id]
                find_entity_name = texts[sent_id][cur_start:cur_end]
                entity_name = entity['surfaceform']
                
                try:
                    assert(find_entity_name==entity_name)
                except:
                    if entity_name not in find_entity_name:
                        continue
                    #print(f'entity_name:{entity_name}')
                    #print(f'find_entity_name:{find_entity_name}')      
                uri = entity['uri']
                entity_id = uri.split('/')[-1]
                if entity_id.startswith('Q'):
                    (entity_type,type_des,entity_des) = find_entity_type(entity_id)
                    if entity_type != 'error' and type_des != "error" and entity_des != "error":
                        entity_dict[boundry[0]] = {'sent_id':sent_id,"ent_type":entity_type,"ent_des":entity_des,"type_des":type_des}
                        if sent_id not in cur_ner_dict:
                            cur_ner_dict[sent_id] = list()
                        cur_ner_dict[sent_id].append((entity_name,entity_type,type_des,entity_des))
                    else:
                        pass
                '''
                elif entity_id.startswith('P'):
                    try:
                        new_line = {"id":entity_id,"labels":{"en":{"language": "en","value":entity_name}},"claims":{"P31":None}}
                        _entity = entity_db.find_one({"id":entity_id})
                        if _entity == None:
                            TREx_db.insert_one(new_line)
                    except:
                        pass
                '''

            
            for triple in triples:
                
                predicate_uri = triple['predicate']['uri']
                try:
                    obj = triple['object']
                    obj_sent_id = entity_dict[obj['boundaries'][0]]['sent_id']
                    obj_type = entity_dict[obj['boundaries'][0]]['ent_type']
                    obj_des = entity_dict[obj['boundaries'][0]]['ent_des']
                    obj_type_des = entity_dict[obj['boundaries'][0]]['type_des']
                    subj = triple['subject']
                    subj_sent_id = entity_dict[subj['boundaries'][0]]['sent_id']
                    subj_type = entity_dict[subj['boundaries'][0]]['ent_type']
                    subj_des = entity_dict[subj['boundaries'][0]]['ent_des']
                    subj_type_des = entity_dict[subj['boundaries'][0]]['type_des']
                    try:
                        assert(obj_sent_id==subj_sent_id)
                        sent_id = obj_sent_id
                    except:
                        continue
                except:
                    continue
                predicate_id = predicate_uri.split('/')[-1]
                (relation,relation_des) = find_relation_type(predicate_id)
                if relation != "error" and relation_des != "error":
                    if sent_id not in cur_re_dict:
                        cur_re_dict[sent_id] = list()
                    cur_re_dict[sent_id].append((obj['surfaceform'],relation,subj['surfaceform'],obj_type,subj_type,relation_des,obj_type_des,subj_type_des,obj_des,subj_des))
            
            
            for k,v in cur_ner_dict.items():
                _entity_dict = {}
                for item in v:
                    if item[1] not in _entity_dict:
                        _entity_dict[item[1]] = {"des":item[2],"instance":[]}
                    _entity_dict[item[1]]["instance"].append({"value":item[0],"des":item[-1]})
                ner_list.append({'sent_id':int(k) + cur_sent_id,'text':texts[k],'entities':_entity_dict})
            for k,v in cur_re_dict.items():
                _re_dict = {}
                for item in v:
                    #print(item[1])
                    if item[1] not in _re_dict:
                        _re_dict[item[1]] = {"des":item[5],"instance":[]}
                    _re_dict[item[1]]["instance"].append( {"object":item[0],"subject":item[2],"obj_type":item[3],"subj_type":item[4],
                                                                           "obj_type_des":item[6],"subj_type_des":item[7],"obj_des":item[8],"subj_des":item[9]})
                re_list.append({'sent_id':int(k) + cur_sent_id,'text':texts[k],'relations':_re_dict})
            cur_sent_id += len(cur_ner_dict)
        if len(ner_list) > 0 or len(re_list) > 0:
            print(f'current_process_line:{ind}     re_list_length:{len(re_list)}   ner_list_length:{len(ner_list)}')
            with open(f"/root/IE_LLM/TREx_data/end{args.bkpt+ind}_re_{file_name}.json",'w',encoding='utf-8') as f:
                for item in re_list:
                    json.dump(item,f)
                    f.write('\n')
            with open(f"/root/IE_LLM/TREx_data/end{args.bkpt+ind}_ner_{file_name}.json",'w',encoding='utf-8') as f:
                for item in ner_list:
                    json.dump(item,f)
                    f.write('\n')
            end_time = time.time()
            print(f"time:{(end_time-st_time)/60}")
            re_list,ner_list = [],[]

def process_KeLM_tacgen(dataset):
    db = myclient.tacgen
    

    data_dir = "/root/IE_LLM/"
    file_list = []

    if dataset == "kelm":
        file_name = data_dir + "kelm_generated_corpus.jsonl"
        file_list.append(file_name)
    elif dataset == "tacgen":
        modes = ["train"]
        for mode in modes:
            file_name = data_dir + f"/tacgen/{mode}.tsv"
            file_list.append(file_name)

    for file_name in file_list:
        ner_list,re_list = list(),list()
        st_time = time.time()
        with open(file_name,'r',encoding='utf-8') as f:
            lines = f.readlines()
        print(len(lines))
        lines = lines[args.bkpt:args.end]
        for line_ind,line in enumerate(lines):
            if line_ind!=0 and line_ind % args.log == 0:
                print(f'current_process_line:{line_ind}     re_list_length:{len(re_list)}   ner_list_length:{len(ner_list)}')
                with open(f"/root/IE_LLM/{dataset}_data/end{args.bkpt+line_ind}_re.json",'w',encoding='utf-8') as f:
                    for item in re_list:
                        json.dump(item,f)
                        f.write('\n')
                with open(f"/root/IE_LLM/{dataset}_data/end{args.bkpt+line_ind}_ner.json",'w',encoding='utf-8') as f:
                    for item in ner_list:
                        json.dump(item,f)
                        f.write('\n')
                end_time = time.time()
                print(f"time_cusuming:{(end_time-st_time)/60}")
                st_time = time.time()
                re_list,ner_list = [],[]

            cur_re_dict,cur_ner_dict = dict(),dict()
            line = json.loads(line)
            triples = line['triples']
            if dataset == "kelm":
                text = line['gen_sentence']
            elif dataset == 'tacgen':
                text = line['sentence']
            for triple in triples:
                if len(triple) > 3:
                    triple = triple[-3:]
                if triple[1] == "instance of":
                    ent_id = find_entity_name(triple[0])
                    type_id = find_entity_name(triple[-1])
                    if ent_id != "error" and type_id != "error":
                        (_,_,entity_des) = find_entity_type(ent_id)
                        (_,_,type_des) = find_entity_type(type_id)
                        if entity_des != "error" and type_des != "error":
                            if triple[-1] not in cur_ner_dict:
                                cur_ner_dict[triple[-1]] = {"des":type_des,"instance":[]}
                            cur_ner_dict[triple[-1]]["instance"].append({"value":triple[0],"des":entity_des})
                else:
                    relation_des = find_entity_des_in_muti_database(triple[1])
                    
                    obj_ent_id = find_entity_name(triple[0])
                    subj_ent_id = find_entity_name(triple[-1])
                    
                    if obj_ent_id != "error" and subj_ent_id != "error":
                        (obj_type,obj_type_des,obj_des) = find_entity_type(obj_ent_id)
                        (subj_type,subj_type_des,subj_des) = find_entity_type(subj_ent_id)
                        if obj_type != "error" and subj_type != "error":
                            if triple[1] not in cur_re_dict:
                                cur_re_dict[triple[1]] = {"des":relation_des,"instance":[]}
                            cur_re_dict[triple[1]]['instance'].append({"object":triple[0],"subject":triple[2],"obj_type":obj_type,"subj_type":subj_type,
                                                                        "obj_type_des":obj_type_des,"subj_type_des":subj_type_des,"obj_des":obj_des,"subj_des":subj_des})
                            #cur_re_dict.get(triple[1],[]).append()
                            #print(cur_re_dict)
            
            if len(cur_re_dict) > 0:
                re_list.append({'sent_id':line_ind,'text':text,'relations':cur_re_dict})
            if len(cur_ner_dict) > 0:
                ner_list.append({'sent_id':line_ind,'text':text,'entities':cur_ner_dict})
        if len(ner_list) > 0 or len(re_list) > 0:
            print(f'current_process_line:{line_ind}     re_list_length:{len(re_list)}   ner_list_length:{len(ner_list)}')
            with open(f"/root/IE_LLM/{dataset}_data/end{args.bkpt+line_ind}_re.json",'w',encoding='utf-8') as f:
                for item in re_list:
                    json.dump(item,f)
                    f.write('\n')
            with open(f"/root/IE_LLM/{dataset}_data/end{args.bkpt+line_ind}_ner.json",'w',encoding='utf-8') as f:
                for item in ner_list:
                    json.dump(item,f)
                    f.write('\n')
            end_time = time.time()
            print(f"time_cusuming:{(end_time-st_time)/60}")
            st_time = time.time()
            re_list,ner_list = [],[]
def process_conceptNet():
    pass 
#mytable = myclient.tacgen.tacgen_entity
#mytable.create_index([('name',1)],background=True)
#process_TRex()
process_KeLM_tacgen(args.dataset)

'''
now the raw data has processed as following:

entity_file:
{ sent_id | text | entities } ->  entities:[ (entity , entity_type) | (entity , entity_type)]

relation_file:
{ sent_id | text | relations } ->  relations:[ (object , relation, subject, object_type, subject_type) | (....)]


'''
def convert_data_to_standard():
    ner_instances,re_instances=[],[]
    with open("entity.json",'r',encoding='utf-8') as ent_f:
        lines = ent_f.readlines()
    for line in lines:
        entity_dict = {}
        line = json.loads(line)
        entities = line['entities']
        text = line['text']
        for item in entities:
            entity_dict.get(item[1],[]).append(item[0])
        for k,v in entity_dict.items():
            temp_instance = {}
            temp_instance['instruction'] = f"please help me to extract the {k} entity in this sentence"
            temp_instance['input'] = text
            temp_instance['output'] = " ".join(v)
            temp_instance['correction'] = ""
            temp_instance['analysis'] = ""

def relation_des():
    myquery = {"id":{"$regex":"^P"}}
    relations = database_60M.find(myquery)
    for relation in relations:
        try:
            relation_name = relation["labels"]["en"]["value"]
            relation_description = relation["descriptions"]["en"]["value"]
            relation_desdb.insert_one({"relation":relation_name,"des":relation_description,"id":relation["id"]})
            
        except:
            pass
        
#relation_des()
        
#process_TRex()