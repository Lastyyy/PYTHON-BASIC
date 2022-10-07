#!/usr/bin/env python3

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
import logging
from functools import partial
import datagen_exceptions as exc


def parsing():
    config = configparser.ConfigParser()
    config.read("default.ini")

    parser = argparse.ArgumentParser(prog='datagen', description='Data generation tool meant to create multiple '
                                                                 'files filled with expected data.',
                                     exit_on_error=False)

    logging.info("Parsing the arguments")

    try:
        config = configparser.ConfigParser()
        config.read("default.ini")
        parser.add_argument("path_to_save_files", default=config['def_val']['path_to_save_files'], type=str, nargs="?",
                            help=f"Input a path to the directory where you want your files with data to be saved. "
                                 f"Path can be both relational or absolute. "
                                 f"Default value = \"{config['def_val']['path_to_save_files']}\"")
        parser.add_argument("--files_count", default=int(config['def_val']['files_count']), type=int,
                            help=f"Number of files that you want to be created. If 0, the output will be printed to the"
                                 f" console. Default value = {config['def_val']['files_count']}")
        parser.add_argument("--file_name", default=config['def_val']['file_name'], type=str,
                            help=f"Core of the name of the files that will be created. "
                                 f"Default value = \"{config['def_val']['file_name']}\"")
        parser.add_argument("--suffix", default=config['def_val']['suffix'], type=str,
                            choices=['count', 'random', 'uuid'], help=f"Suffix added to file_name to files created. "
                                                                      f"Default value = "
                                                                      f"\"{config['def_val']['suffix']}\"."
                                                                      f" All options: count, random, uuid")
        parser.add_argument("--data_schema", default=config['def_val']['data_schema'], type=str,
                            help=f"Json schema, based on which the data will be created. Schema can be provided by "
                                 f"command line or as a path to json file. "
                                 f"Default value = \"{config['def_val']['data_schema']}\"")
        parser.add_argument("--data_lines", default=int(config['def_val']['data_lines']), type=int,
                            help=f"Number of lines with data in each file. "
                                 f"Default value = {int(config['def_val']['data_lines'])}")
        parser.add_argument("--clear_path", action='store_true',
                            help='If used, all jsonl files in the directory specified by path_to_save_files, '
                                 'containing file_name in their name will be deleted before creating new ones.')
        parser.add_argument("--multiprocessing", default=int(config['def_val']['multiprocessing']), type=int,
                            help=f"Number of processes used to create files. "
                                 f"Default value = {int(config['def_val']['multiprocessing'])}")

    except ValueError:
        raise exc.InvalidDefaultConfiguration()

    return parser.parse_args()


def path_save_files_arg(args: argparse.Namespace):
    path = args.__dict__['path_to_save_files']
    logging.info(f"Checking path to the directory where the files will be placed...")
    if os.path.exists(path):
        if os.path.isdir(path):
            logging.info(f"Directory exists: {path}")
        else:
            raise exc.PathIsFile(path)
    else:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Created directory: {path}")

    return path


def files_count_arg(args: argparse.Namespace):
    logging.info("Checking the number of files to be created")
    if args.__dict__["files_count"] < 0:
        raise exc.ValueNegative("Number of files to be created")
    else:
        return int(args.__dict__["files_count"])


def data_schema_arg(args: argparse.Namespace):
    logging.info("Checking the data schema...")
    data_schema = args.__dict__["data_schema"]

    if data_schema[-5:] == ".json":
        if not os.path.exists(data_schema) or not os.path.isfile(data_schema):
            raise exc.NonexistentSchemaFile(data_schema)
        file = open(data_schema, "r")
        try:
            data_schema = json.load(file)
        except SyntaxError as e:
            raise exc.IncorrectSchema(e.msg.capitalize())

    else:
        try:
            logging.info(str(data_schema))
            data_schema = json.loads(data_schema)
        except json.decoder.JSONDecodeError as e:
            raise exc.IncorrectSchema(e.msg.capitalize())

    logging.info("Given data schema is correct")
    return data_schema


def clear_path_arg(args: argparse.Namespace, path):
    clear_path = args.__dict__["clear_path"]
    if clear_path:
        file_name = args.__dict__["file_name"]
        logging.info(f"Deleting all jsonl files containing '{file_name}' in their name from {path}")
        for file in os.listdir(path):
            if re.search(file_name, file) and re.search(".jsonl", file):
                os.remove(os.path.join(path, file))


def multiprocessing_arg(args: argparse.Namespace):
    num_of_processes = int(args.__dict__["multiprocessing"])
    if num_of_processes > os.cpu_count():
        num_of_processes = os.cpu_count()
    if num_of_processes > 1:
        logging.info(f"Changing number of processes that will be used to create files from 1 to {num_of_processes}")
    return num_of_processes


def create_file_names(num_of_saving_files, file_name, suffix):
    logging.info("Creating a list of names for the files that will be created")

    files_to_create = []
    list_of_taken_numbers = []
    max_num = max(num_of_saving_files, 9999)

    if num_of_saving_files > 1:
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

    elif num_of_saving_files == 1:
        files_to_create.append(file_name + ".jsonl")

    return files_to_create


def check_warnings(data_schema, warnings):
    for key in data_schema.keys():
        if ":" in data_schema[key]:
            left, right = data_schema[key].split(":")

            if right != "" and left == "timestamp":
                warnings["timestamp_with_value_warning"][0] = True
                warnings["timestamp_with_value_warning"].append(key)

            elif left not in ["str", "int", "timestamp", "float"]:
                warnings["data_without_type_warning"][0] = True
                warnings["data_without_type_warning"].append(key)

        elif data_schema[key] != "timestamp":
            warnings["data_without_type_warning"][0] = True
            warnings["data_without_type_warning"].append(key)

    return warnings


def create_data_line(data_schema):
    current_line = {}
    # checking every key: value from the data_schema
    for key in data_schema.keys():
        # checking if the type of the value is correctly given, for example "int:rand"
        if ":" in data_schema[key]:
            # splitting the value from "int:rand" to left = int, right = rand
            left, right = data_schema[key].split(":")

            # type given without a value case
            if right == "":
                # timestamp without a value case
                if left == "timestamp":
                    current_line[key] = time.time()
                elif left == "str":
                    current_line[key] = ""
                elif left == "int" or left == "float":
                    current_line[key] = None

            # timestamp with a value case - value is ignored
            # the warning will be written later in the check_warnings function
            elif left == "timestamp":
                current_line[key] = time.time()

            # value = rand(x, y) case, only working when left == int
            elif right[:5] == "rand(":
                if left != "int":
                    raise exc.WrongTypeRandRange
                values = right[5: -1]
                left_val, right_val = values.split(",")
                left_val = int(left_val.strip())
                right_val = int(right_val.strip())
                current_line[key] = random.randint(min(left_val, right_val), max(left_val, right_val))

            # other cases of 'rand' values
            elif right == "rand":
                if left == "str":
                    current_line[key] = str(uuid.uuid4())
                elif left == "int":
                    current_line[key] = random.randint(0, 10000)

            # list with choices case
            elif right[0] == "[" and right[-1] == "]":
                try:
                    choices = ast.literal_eval(right)
                    current_line[key] = random.choice(choices)
                except SyntaxError:
                    raise exc.WrongListOfChoices(key)

            # plain values - float values possible too
            elif left != "str":
                try:
                    right_val = ast.literal_eval(right)
                    if type(right_val).__name__ == left:
                        current_line[key] = right_val
                    else:
                        raise ValueError
                except (ValueError, SyntaxError):
                    if left != 'int' and left != 'float':
                        raise exc.IncorrectType(left)
                    else:
                        raise exc.IncorrectValue(right, left)

            else:
                current_line[key] = right

        elif data_schema[key] == "timestamp":
            current_line[key] = time.time()

    return current_line


def create_file_with_data(this_file_name, path_to_save_files, data_schema, data_lines):
    if os.path.exists(os.path.join(path_to_save_files, this_file_name)):
        logging.warning(f"File {this_file_name} wasn't created, because file with this name already exists. To clear "
                        f" output directory from all files containing your file name, use --clear_path flag.")
    else:
        new_file = open(os.path.join(path_to_save_files, this_file_name), "w+")
        str_of_dicts = ""

        for _ in range(data_lines):
            current_line = create_data_line(data_schema)
            new_file.write(json.dumps(current_line) + "\n")

        json.dump(str_of_dicts, new_file)


def create_files(files_to_create, path_to_save_files, data_schema, data_lines, num_of_processes):
    with multiprocessing.Pool(num_of_processes) as pool:
        pool.map(partial(create_file_with_data, path_to_save_files=path_to_save_files, data_schema=data_schema,
                         data_lines=data_lines), files_to_create)


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        parsed_args = parsing()

        warnings = {"timestamp_with_value_warning": [False], "data_without_type_warning": [False]}

        num_of_saving_files = files_count_arg(parsed_args)

        # if num_of_saving_files == 0 then do not check the path and do not try to clear it
        if num_of_saving_files > 0:
            path_to_save_files = path_save_files_arg(parsed_args)
            clear_path_arg(parsed_args, path_to_save_files)

        data_schema = data_schema_arg(parsed_args)
        num_of_processes = multiprocessing_arg(parsed_args)

        file_name = parsed_args.__dict__["file_name"]
        if "/" in file_name:
            raise exc.ForbiddenCharInFileName()

        suffix = parsed_args.__dict__["suffix"]

        data_lines = parsed_args.__dict__["data_lines"]
        if data_lines < 1:
            raise exc.ValueNegative("Data lines value")

        if num_of_saving_files == 0:
            logging.info("Creating the data and printing it out...")
            for _ in range(data_lines):
                print(create_data_line(data_schema))
            logging.info("Printing out data completed")
        else:
            files_to_create = create_file_names(num_of_saving_files, file_name, suffix)
            logging.info("Creating data and putting it to the files is starting... ")
            create_files(files_to_create, path_to_save_files, data_schema, data_lines, num_of_processes)

            warnings = check_warnings(data_schema, warnings)
            if warnings["timestamp_with_value_warning"][0]:
                logging.warning(f"Values for keys: {warnings['timestamp_with_value_warning'][1:]} were ignored, "
                                f"because timestamp type doesn't take any value!")
            if warnings["data_without_type_warning"][0]:
                logging.warning(f"Keys: {warnings['data_without_type_warning'][1:]} were ignored, "
                                f"because the types for their values weren't given!")

            logging.info("Creating files and filling them with data finished")
    except argparse.ArgumentError as arg_err:
        logging.error(arg_err.message.capitalize())
        exit(1)
    except exc.DatagenBaseException:
        exit(1)


if __name__ == '__main__':
    main()
