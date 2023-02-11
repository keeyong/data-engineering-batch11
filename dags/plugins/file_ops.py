import json
from glob import glob
import logging
import subprocess


def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def write_to_jsonl_file(file_name, results):
    with open(file_name, 'a+') as outfile:
        for entry in results:
            json.dump(entry, outfile)
            outfile.write('\n')

def run_cmd_and_get_result(cmd):
    tokens = cmd.split(" ")
    ret = subprocess.check_output(tokens)
    return ret

def run_cmd_with_direct(cmd, output_filename):
    tokens = cmd.split(" ")
    with open(output_filename, "w") as outfile:
        subprocess.call(tokens, stdout=outfile)

def run_cmds_with_semicolon(command):
    output = subprocess.check_output(command, shell=True)
    return output

def load_all_jsons_into_list(path_to_json):

    configs = []
    for f_name in glob(path_to_json+ '/*.py'):
        # logging.info(f_name)
        with open(f_name) as f:
            dict_text = f.read()
            try:
                dict = eval(dict_text)
            except Exception as e:
                logging.info(str(e))
                raise
            else:
                configs.append(dict)

    return configs

def find(table_name, table_confs):
    """
    scan through table_confs and see if there is a table matching table_name
    """
    for table in table_confs:
        if table.get("table") == table_name:
            return table

    return None
