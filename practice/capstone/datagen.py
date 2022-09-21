import argparse
import ast
import configparser
import json
import os
import random
import re
import time
import uuid


def parsing():
    config = configparser.ConfigParser()
    config.read("default.ini")
    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_save_files", default=config['def_val']['path_to_save_files'], type=str, nargs="?")
    parser.add_argument("--files_count", default=int(config['def_val']['files_count']), type=int)
    parser.add_argument("--file_name", default=config['def_val']['file_name'], type=str)
    parser.add_argument("--suffix", default=config['def_val']['suffix'], type=str,
                        choices=['count', 'random', 'uuid'])
    parser.add_argument("--data_schema", default=config['def_val']['data_schema'], type=str)
    parser.add_argument("--data_lines", default=int(config['def_val']['data_lines']), type=int)
    parser.add_argument("--clear_path", default=bool(config['def_val']['clear_path']), type=bool)
    parser.add_argument("--multiprocessing", default=int(config['def_val']['multiprocessing']), type=int)

    return parser.parse_args()


def path_save_files_arg(args: argparse.Namespace):
    path = args.__dict__['path_to_save_files']
    if os.path.exists(path):
        if os.path.isdir(path):
            print("exists at ", path)
        else:
            # TODO logging and exit that it is file not dir
            print("file not dir")
    else:
        os.makedirs(path, exist_ok=True)
        print("created at ", path)

    return path


def files_count_arg(args: argparse.Namespace):
    if args.__dict__["files_count"] < 0:
        # TODO logging i exit
        pass
    else:
        return int(args.__dict__["files_count"])


# def file_name_arg():


def data_schema_arg(args: argparse.Namespace):
    data_schema = args.__dict__["data_schema"]
    if data_schema[-5:] == ".json":
        file = open(data_schema, "r")
        data_schema = json.load(file)
    else:
        # TODO try exceptions for ast literal eval
        data_schema = ast.literal_eval(data_schema)
    # dict
    return data_schema


def clear_path_arg(args: argparse.Namespace, path):
    clear_path = args.__dict__["clear_path"]
    if clear_path:
        file_name = args.__dict__["file_name"]
        for file in os.listdir(path):
            if re.search(file_name, file):
                os.remove(os.path.join(path, file))


parsed_args = parsing()
path_to_save_files = path_save_files_arg(parsed_args)
num_of_saving_files = files_count_arg(parsed_args)
data_schema = data_schema_arg(parsed_args)
clear_path_arg(parsed_args, path_to_save_files)
# TODO multiprocessing somehow xd

file_name = parsed_args.__dict__["file_name"]
suffix = parsed_args.__dict__["suffix"]
data_lines = parsed_args.__dict__["data_lines"]
if num_of_saving_files > 1:
    for i in range(1, num_of_saving_files + 1):
        if suffix == "count":
            this_file_name = file_name + str(i)
        elif suffix == "random":
            rand_num = random.randint(1, max(num_of_saving_files, 9999))
            this_file_name = file_name + str(rand_num)
            while this_file_name in os.listdir(path_to_save_files):
                rand_num = random.randint(1, max(num_of_saving_files + 1, 9999))
                this_file_name = file_name + str(rand_num)
        else:
            suf_uuid = str(uuid.uuid4())
            this_file_name = file_name + suf_uuid

        this_file_name += ".json"
        new_file = open(os.path.join(path_to_save_files, this_file_name), "w+")

        # TODO lines < 0 or 1 - exit
        for _ in range(data_lines):
            current_line = {}
            for key in data_schema.keys():
                print(data_schema[key])
                # TODO REFACTOR IT - check first right side, then check every possibility of the left sides
                if data_schema[key][:9] == "timestamp":
                    if len(data_schema) > 10:
                        # TODO create logging warning that value was provided for timestamp, but has no effect
                        pass
                    current_line[key] = time.time()
                elif data_schema[key][:3] == "str":
                    pass




