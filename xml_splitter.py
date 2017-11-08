import re
import os
from lxml import etree
import argparse
import sys

parser = argparse.ArgumentParser(
    description='This script is used to split a large xml file into several small xml parts.')
parser.add_argument(
    'input_file', help='The input xml file that needed to parsed. Ex: data.xml', type=str)
parser.add_argument(
    'root_tag', help='The tag that represents individual accounts. Ex: envelope', type=str)
parser.add_argument(
    'wrapper_tag', help='The tag that will be used to wrap the slitted accounts. Ex: statementProduction', type=str)
parser.add_argument(
    'count', help='Amount of root tag element that will be kept in a single file. Ex: 1000', type=int)
parser.add_argument(
    'output_file', help='The input xml file that needed to parsed. Ex: data.xml', type=str)
args = parser.parse_args()


def we_are_frozen():
    # All of the modules are built-in to the interpreter, e.g., by py2exe
    return hasattr(sys, "frozen")


def module_path():
    encoding = sys.getfilesystemencoding()
    if we_are_frozen():
        return os.path.dirname(unicode(sys.executable, encoding))
    return os.path.dirname(unicode(__file__, encoding))


def file_name_generator(output_path, output_file):
    file_counter = 0
    while (True):
        file_counter += 1
        yield output_path+os.path.sep+output_file + str(file_counter) + '.xml'


def generate_next_file(output_path, output_file):
    generator = file_name_generator(output_path, output_file)
    while (True):
        current_file_name = next(generator)
        f = open(current_file_name, 'w+')
        print "Created new file: " + output_path+current_file_name
        yield f


def validate_arguments():
    output_path = os.path.dirname(args.output_file)
    output_file = os.path.basename(args.output_file).split('.')[0]
    root_tag = '{*}' + args.root_tag
    input_file = args.input_file

    if not os.path.isabs(input_file):
        input_file = os.path.realpath('.')+os.path.sep+input_file

    if not os.path.isfile(input_file):
        sys.stderr.write(
            "ERROR: Not a Valid Input File! File: " + input_file + "\n")
        sys.exit(1)

    if not os.stat(input_file).st_size:
        sys.stderr.write("ERROR: Empty input file: " + input_file)
        sys.exit(1)

    if not output_path:
        output_path = module_path()

    if output_path and not os.path.exists(output_path):
        os.makedirs(output_path)

    return (input_file, root_tag, output_path, output_file)


if __name__ == '__main__':
    elem_counter = 0

    input_file, root_tag, output_path, output_file = validate_arguments()

    file_generator = generate_next_file(output_path, output_file)
    f = next(file_generator)
    f.write("<" + args.wrapper_tag + ">\n")
    for event, elem in etree.iterparse(input_file, tag=root_tag):
        f.write(re.sub(r"xmlns=\".*?\"", "", etree.tostring(elem)))
        if elem_counter > args.count - 1:
            elem_counter = 0
            f.write("</" + args.wrapper_tag + ">\n")
            f.close()
            f = next(file_generator)
            f.write("<" + args.wrapper_tag + ">\n")
        elem_counter += 1
        elem.clear()

    f.write("</" + args.wrapper_tag + ">\n")
    f.close()
