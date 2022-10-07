import ast
import json
import re
import sys
from uuid import UUID

import pytest
import datagen
import tempfile
import os


def check_values_in_output_file(data_schema, output_dict):
    for key in data_schema:
        if ":" in data_schema[key]:
            left, right = data_schema[key].split(":")
            assert key in output_dict.keys()
            output_val = output_dict[key]

            if right == "":
                if left == "timestamp":
                    assert type(output_val) == float
                elif left == "str":
                    assert output_val == ""
                elif left == "int" or left == "float":
                    assert output_val is None

            elif left == "timestamp":
                assert type(output_val) == float

            elif right[:5] == "rand(":
                values = right[5: -1]
                left_val, right_val = values.split(",")
                left_val = int(left_val.strip())
                right_val = int(right_val.strip())
                assert min(left_val, right_val) <= output_val <= max(left_val, right_val)

            elif right == "rand":
                if left == "str":
                    try:
                        uuid_obj = UUID(output_val, version=4)
                    except ValueError:
                        pytest.fail("Given value isn't correct UUID value")
                    except AttributeError:
                        pytest.fail("There is not an expected random string in the output file (uuid4)")
                elif left == "int":
                    assert 0 <= output_val <= 10000

            elif right[0] == "[" and right[-1] == "]":
                try:
                    choices = ast.literal_eval(right)
                    assert output_val in choices
                except SyntaxError:
                    pytest.fail(f"List of values for '{key}' is incorrect")

            # float values possible too
            elif left != "str":
                try:
                    right_val = ast.literal_eval(right)
                    if type(right_val).__name__ == left:
                        assert output_val == right_val
                except (ValueError, SyntaxError):
                    pytest.fail(f"Incorrect value for a given type, expected {left}")

            else:
                assert output_val == right

        elif data_schema[key] == "timestamp":
            assert key in output_dict.keys()
            assert type(output_dict[key]) == float


@pytest.fixture()
def dict_data_schema():
    data_schema_dict = {"str_rand": "str:rand", "int_rand": "int:rand", "str_list": "str:['a', 'b']",
                        "int_list": "int:[0, 1]", "int_rand_range": "int:rand(0, 99)", "str_val": "str:a1b2",
                        "int_val": "int:123", "str_empty": "str:", "int_empty": "int:"}
    return data_schema_dict


@pytest.mark.parametrize("param_to_test, test_1, test_2, test_3", [
    [["files_count", (2, True), ("2a", False), (2.1, False)],
     ["file_name", (12345, True), ("123/45", False), ("tmp_name", True)],
     ["data_lines", (1234, True), (1234.1, False), ("2a", False)],
     ["multiprocessing", (3, True), (10, True), (2.1, False)]],
])
def test_dif_data_types(param_to_test, test_1, test_2, test_3):
    tmp_dir = tempfile.TemporaryDirectory()
    params_list = [param_to_test, test_1, test_2, test_3]

    for parameter_being_checked, val_1, val_2, val_3 in params_list:
        values_list = [val_1, val_2, val_3]
        for value, should_work in values_list:

            for file in os.listdir(tmp_dir.name):
                os.remove(os.path.join(tmp_dir.name, file))

            sys.argv = ['datagen', tmp_dir.name, '--' + parameter_being_checked + '=' + str(value)]
            if not should_work:
                with pytest.raises(SystemExit):
                    datagen.main()
            else:
                try:
                    datagen.main()
                except Exception:
                    pytest.fail("Script exited unexpectedly")


@pytest.mark.parametrize("param_data_schema",
                         ["{\"name\": \"str:Daenerys\", \"surname\": \"str:['Targaryen', 'Mother of Dragons']\", "
                          "\"age\": \"int:23456789\"}",
                          "{\"slayer of the Night King\": \"str:['Arya', 'A girl with no name']\", "
                          "\"when was it\": \"timestamp\"}",
                          "{\"city\": \"str:rand\", \"year\": \"rand(99, 299)\", \"coordinates\": \"int:rand\", "
                          "\"card\": \"9090-9090-0909-1232\"}"])
def test_dif_data_schemas(param_data_schema):
    tmp_dir = tempfile.TemporaryDirectory()
    sys.argv = ['datagen', tmp_dir.name, '--data_schema=' + param_data_schema, '--file_name=data', '--files_count=1',
                '--data_lines=1']
    datagen.main()
    output_file = open(os.path.join(tmp_dir.name, "data.jsonl"), 'r')
    output_file.seek(0)
    output = output_file.readline()
    output_dict = json.loads(output)
    param_data_schema = ast.literal_eval(param_data_schema)

    check_values_in_output_file(param_data_schema, output_dict)


def test_data_schema_from_json(dict_data_schema):
    tmp_dir = tempfile.TemporaryDirectory()
    data_schema_file = tempfile.NamedTemporaryFile(suffix=".json", mode="w+")
    json.dump(dict_data_schema, data_schema_file)
    data_schema_file.seek(0)

    sys.argv = ['datagen', tmp_dir.name, '--data_schema=' + data_schema_file.name,
                '--file_name=data', '--files_count=1', '--data_lines=1']
    datagen.main()
    assert "data.jsonl" in os.listdir(tmp_dir.name)

    output_file = open(os.path.join(tmp_dir.name, "data.jsonl"), 'r')
    output_file.seek(0)
    output = output_file.readline()
    output_dict = json.loads(output)

    check_values_in_output_file(dict_data_schema, output_dict)


def test_clear_path_action():
    tmp_dir = tempfile.TemporaryDirectory()
    for i in range(1, 13):
        f = open(os.path.join(tmp_dir.name, 'data' + str(i * 50) + '.jsonl'), 'w')

    sys.argv = ['datagen', tmp_dir.name,
                '--clear_path', '--file_name=data', '--files_count=7', '--suffix=count']
    datagen.main()
    assert 7 == len(os.listdir(tmp_dir.name))

    sys.argv = ['datagen', tmp_dir.name,
                '--clear_path', '--file_name=test', '--files_count=3', '--suffix=count']
    datagen.main()
    assert 10 == len(os.listdir(tmp_dir.name))

    f = open(os.path.join(tmp_dir.name, 'data111.txt'), 'w')
    sys.argv = ['datagen', tmp_dir.name,
                '--clear_path', '--file_name=data', '--files_count=5', '--suffix=count']
    datagen.main()
    assert 9 == len(os.listdir(tmp_dir.name))


def test_saving_file():
    tmp_dir = tempfile.TemporaryDirectory()
    sys.argv = ['datagen', tmp_dir.name, '--files_count=1', '--file_name=data', '--suffix=count']
    datagen.main()
    assert 'data.jsonl' in os.listdir(tmp_dir.name)

    sys.argv = ['datagen', tmp_dir.name, '--files_count=2', '--file_name=test', '--suffix=count']
    datagen.main()
    assert 'test1.jsonl' in os.listdir(tmp_dir.name) and 'test2.jsonl' in os.listdir(tmp_dir.name)


def test_num_of_files_with_multiprocessing():
    tmp_dir = tempfile.TemporaryDirectory()
    sys.argv = ['datagen', tmp_dir.name, '--files_count=999', '--multiprocessing=2']
    datagen.main()
    assert 999 == len(os.listdir(tmp_dir.name))
    for file in os.listdir(tmp_dir.name):
        os.remove(os.path.join(tmp_dir.name, file))
    sys.argv = ['datagen', tmp_dir.name, '--files_count=3456', '--multiprocessing=1337', '--data_lines=1']
    datagen.main()
    assert 3456 == len(os.listdir(tmp_dir.name))


@pytest.mark.parametrize("param_data_schema",
                         ["{\"timestamp_warning\": \"timestamp:troll\", \"age\": \"int:23456789\"}",
                          "{\"no_type_warning\": \"['Arya', 'Troll']\", \"da\": \"timestamp\"}"])
def test_warnings(caplog, param_data_schema):
    tmp_dir = tempfile.TemporaryDirectory()
    sys.argv = ['datagen', tmp_dir.name, '--data_schema=' + param_data_schema, '--file_name=data', '--files_count=1',
                '--data_lines=1']
    datagen.main()
    if re.search("timestamp:troll", param_data_schema):
        assert "timestamp_warning" in caplog.text
    elif re.search("no_type_warning", param_data_schema):
        assert "no_type_warning" in caplog.text

    """     
    # testing dif types for files_count
    sys.argv = ['datagen', tmp_dir.name, '--files_count=2']
    datagen.main()
    self.assertEqual(2, len(os.listdir(tmp_dir.name)))
    sys.argv = ['datagen', tmp_dir.name, '--files_count=1.2']
    self.assertRaisesRegexp(SystemExit, "1", datagen.main)
    sys.argv = ['datagen', tmp_dir.name, '--files_count=abc']
    self.assertRaisesRegexp(SystemExit, "1", datagen.main)

    # testing dif types for file_name
    sys.argv = ['datagen', tmp_dir.name, '--file_name=tmp_name', '--files_count=1']
    datagen.main()
    self.assertTrue('tmp_name.jsonl' in os.listdir(tmp_dir.name))
    sys.argv = ['datagen', tmp_dir.name, '--file_name=123/45', '--files_count=1']
    self.assertRaisesRegexp(SystemExit, "1", datagen.main)
    sys.argv = ['datagen', tmp_dir.name, '--file_name=12345', '--files_count=2']
    datagen.main()
    self.assertTrue('123451.jsonl' in os.listdir(tmp_dir.name) and '123452.jsonl' in os.listdir(tmp_dir.name))

    # testing dif types for data_lines
    sys.argv = ['datagen', tmp_dir.name, '--data_lines=1234', '--files_count=1']
    datagen.main()
    with open(tmp_dir.name + '/data.jsonl', 'r') as tmp_file:
        for count, line in enumerate(tmp_file):
            pass
    self.assertEqual(count, 1234)
    sys.argv = ['datagen', tmp_dir.name, '--data_lines=1234.1', '--files_count=1']
    self.assertRaisesRegexp(SystemExit, "1", datagen.main)
    sys.argv = ['datagen', tmp_dir.name, '--data_lines=1a2b', '--files_count=1']
    self.assertRaisesRegexp(SystemExit, "1", datagen.main)

    # testing dif types for multiprocessing
    sys.argv = ['datagen', tmp_dir.name, '--multiprocessing=3']
    try:
        datagen.main()
    except Exception:
        self.fail('Script exited unexpectedly')
    sys.argv = ['datagen', tmp_dir.name, '--multiprocessing=3.1']
    self.assertRaisesRegexp(SystemExit, "1", datagen.main)
    sys.argv = ['datagen', tmp_dir.name, '--multiprocessing=3a2']
    self.assertRaisesRegexp(SystemExit, "1", datagen.main)
    """

    '''
        try:

            uuid_obj = UUID(str_rand, version=4)
        except ValueError:
            pytest.fail("Given value isn't correct UUID value")
        except AttributeError:
            pytest.fail("There is not an expected random string in the output file (uuid4)")

        try:
            int_rand = re.search('\"int_rand\": .*?[,}]', output).group(0)
            assert 0 <= int(int_rand[12:-1]) <= 10000
            re.search('\"str_list\": \"[ab]\"', output).group(0)
            re.search('\"int_list\": [01]', output).group(0)
            re.search('\"str_val\": \"a1b2\"', output).group(0)
            re.search('\"int_val\": 123', output).group(0)
            re.search('\"str_empty\": \"\"', output).group(0)
            re.search('\"int_empty\": null', output).group(0)
            int_rand_range = re.search('\"int_rand_range\": .*?[,}]', output).group(0)
            assert 0 <= int(int_rand_range[18:-1]) <= 99
        except AttributeError:
            pytest.fail("One of the expected values in output file isn't correct")
            '''

