import argparse
import ast
import configparser
import json
import multiprocessing
import os
import random
import re
import time
import uuid
from functools import partial


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
            print("Directory exists at ", path)
        else:
            # TODO logging and exit that it is file not dir
            print("Given path is a file, not a directory")
    else:
        os.makedirs(path, exist_ok=True)
        print("Created directory at ", path)

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


def multiprocessing_arg(args: argparse.Namespace):
    num_of_processes = int(args.__dict__["multiprocessing"])
    if num_of_processes > os.cpu_count():
        num_of_processes = os.cpu_count()
    return num_of_processes


def create_file_names(num_of_saving_files, file_name, suffix):
    files_to_create = []
    list_of_taken_numbers = []
    max_num = max(num_of_saving_files, 9999)

    if num_of_saving_files >= 1:
        for i in range(1, num_of_saving_files + 1):

            if suffix == "count":
                this_file_name = file_name + str(i)

            elif suffix == "random":
                rand_num = random.randint(1, max_num)
                while rand_num in list_of_taken_numbers:
                    rand_num = random.randint(1, max_num)
                list_of_taken_numbers.append(rand_num)

                this_file_name = file_name + str(rand_num)

            else:
                suf_uuid = str(uuid.uuid4())
                this_file_name = file_name + suf_uuid

            this_file_name += ".jsonl"
            files_to_create.append(this_file_name)

    return files_to_create


def create_file_with_data(this_file_name, path_to_save_files, data_schema, data_lines):
    new_file = open(os.path.join(path_to_save_files, this_file_name), "w+")
    str_of_dicts = ""

    # TODO lines < 0 or 1 - exit
    for _ in range(data_lines):
        current_line = {}
        for key in data_schema.keys():
            # print(data_schema[key])
            # TODO REFACTOR IT - check first right side, then check every possibility of the left sides
            if ":" in data_schema[key]:
                left, right = data_schema[key].split(":")
                if right == "":
                    if left == "timestamp":
                        current_line[key] = time.time()

                elif right == "rand":
                    if left == "str":
                        current_line[key] = str(uuid.uuid4())
                    elif left == "int":
                        current_line[key] = random.randint(0, 10000)

                elif right[0] == "[" and right[-1] == "]":
                    choices = ast.literal_eval(right)
                    current_line[key] = random.choice(choices)
                    # print(current_line[key])

                elif right[:5] == "rand(":
                    if left != "int":
                        # TODO exit cause rand() can be used only with int
                        pass
                    values = right[5: -1]
                    left_val, right_val = values.split(",")
                    left_val = int(left_val.strip())
                    right_val = int(right_val.strip())
                    current_line[key] = random.randint(min(left_val, right_val), max(left_val, right_val))

                elif left == "timestamp":
                    # TODO warning info that it doesnt matter kek
                    current_line[key] = time.time()

                elif left != "str":
                    right_val = ast.literal_eval(right)
                    if type(right_val).__name__ == left:
                        current_line[key] = right_val

                else:
                    current_line[key] = right

        new_file.write(json.dumps(current_line) + "\n")

    json.dump(str_of_dicts, new_file)


def create_files(files_to_create, path_to_save_files, data_schema, data_lines, num_of_processes):
    with multiprocessing.Pool(num_of_processes) as pool:
        hm = pool.map(partial(create_file_with_data, path_to_save_files=path_to_save_files, data_schema=data_schema,
                              data_lines=data_lines), files_to_create)


if __name__ == '__main__':

    parsed_args = parsing()
    st = time.time()

    path_to_save_files = path_save_files_arg(parsed_args)
    num_of_saving_files = files_count_arg(parsed_args)
    data_schema = data_schema_arg(parsed_args)
    num_of_processes = multiprocessing_arg(parsed_args)

    clear_path_arg(parsed_args, path_to_save_files)

    file_name = parsed_args.__dict__["file_name"]
    suffix = parsed_args.__dict__["suffix"]
    data_lines = parsed_args.__dict__["data_lines"]

    files_to_create = create_file_names(num_of_saving_files, file_name, suffix)
    create_files(files_to_create, path_to_save_files, data_schema, data_lines, num_of_processes)

    end = time.time()
    # print(end-st)


