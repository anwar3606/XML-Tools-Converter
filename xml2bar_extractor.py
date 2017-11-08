import argparse
import json
import os
import sys
import time
import multiprocessing

import re

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from lxml import etree

parser = argparse.ArgumentParser(
    description='This script is used for converting a xml file into character delimited file like csv.')
parser.add_argument(
    'input_file', help='The input xml file that needed to parsed. Ex: data.xml', type=str)
parser.add_argument(
    'template_file', help='This file have to contains all the extraction logic in a JSON file.  Ex: abc.json', type=str)
parser.add_argument(
    'output_file', help='The output file location. Ex: abc.csv', type=str)
parser.add_argument('delimiter',
                    type=str,
                    help='Delimiter character that will be used to delimit values. (Default = |) Ex: | or ,',
                    default='|')
parser.add_argument('wrapper_tag', help='This tag will used to wrap the whole xml output file.', type=str)
parser.add_argument('is_multiprocessing',
                    help='Parallel processing of the xml elements. Will result in unordered result. (Default=True)',
                    type=bool,
                    default=True)
parser.add_argument('is_multiprocessing',
                    help='Parallel processing of the xml elements. Will result in unordered result. (Default=True)',
                    type=bool,
                    default=True)
parser.add_argument('whole_element',
                    help='If True, the whole element tag will be outputted, instead of just the tag that is defined in the template file. ',
                    type=bool,
                    default=False)
args = parser.parse_args()


def validate_arguments():
    input_file = args.input_file
    template_file = args.template_file
    output_file = args.output_file
    delimiter = args.delimiter
    is_multiprocessing = args.is_multiprocessing
    wrapper_tag = args.wrapper_tag
    is_whole_element = args.whole_element

    if not os.path.isabs(args.input_file):
        input_file = os.path.realpath('.') + os.path.sep + args.input_file

    if not os.path.isfile(input_file):
        sys.stderr.write("ERROR: The input file '" +
                         input_file + "' does not exist!\n")
        sys.exit(1)

    if not os.path.isabs(args.template_file):
        template_file = os.path.realpath('.') + os.path.sep + args.template_file

    if not os.path.isfile(template_file):
        sys.stderr.write("ERROR: The template file '" +
                         template_file + "' does not exist!\n")
        sys.exit(1)

    if not os.path.isabs(args.output_file):
        output_file = os.path.realpath('.') + os.path.sep + args.output_file

    if not os.path.exists(os.path.dirname(output_file)):
        sys.stdout.write("WARNING: The output directory '" +
                         os.path.dirname(output_file) + "' does not exist!\nCreating directories...")
        os.makedirs(output_file)

    return (input_file, template_file, output_file, delimiter, is_multiprocessing, wrapper_tag, is_whole_element)


def write_line(line_no, element, values_to_write):
    values = list()
    for value in values_to_write:
        if not value:
            values.append("")
        else:
            child = element.xpath(value)
            for c in child:
                values.append(etree.tostring(c))
    text = ""
    for item in values:
        text += item

    return text


def process_element(elem, path, is_whole_element):
    output_line = ""
    if isinstance(elem, list):
        for sub_elem in elem:
            output_line += process_element(sub_elem, path)
    else:
        for key, val in path.items():
            if isinstance(val, list):
                output_line = output_line + write_line(key, elem, val)
            elif isinstance(val, dict):
                temp_elem = elem.xpath(key)
                output_line = output_line + process_element(temp_elem, val)
            if (is_whole_element and output_line):
                break;
    return output_file;


def start_processing_element(params):
    elem_string, d, is_whole_element = params
    elem_string = re.sub(r"xmlns=\".*?\"", "", elem_string)
    tree = etree.fromstring(elem_string)
    result = process_element(tree, d, is_whole_element)
    if (is_whole_element and result):
        return elem_string
    else:
        return result


def element_generator(input_file, template, root_tag, is_whole_element):
    root_tag_with_namespace = "{*}" + root_tag
    for event, elem in etree.iterparse(input_file, tag=root_tag_with_namespace):
        yield (etree.tostring(elem), template.get(root_tag), is_whole_element)
        elem.clear()


if __name__ == '__main__':
    start_time = time.time()
    input_file, template_file, output_file, delimiter, is_multiprocessing, wrapper_tag, is_whole_element = validate_arguments()
    counter = 0

    try:
        with open(args.template_file) as template_file:
            template = json.loads(template_file.read(), object_pairs_hook=OrderedDict)
    except Exception as e:
        sys.stderr.write("ERROR: Invalid JSON template file! " + e.message + "\n")
        sys.exit(1)

    root_tag = next(iter(template))

    f = open(output_file, 'w+')
    f.writelines(wrapper_tag)
    p = multiprocessing.Pool()
    if is_multiprocessing:
        try:
            for result in p.imap_unordered(start_processing_element,
                                           element_generator(input_file, template, root_tag, is_whole_element),
                                           chunksize=100):
                if (result and not is_whole_element):
                    f.write("<" + root_tag + ">\n")
                    f.write(result + str("\n"))
                    f.write("</" + root_tag + ">\n")
                else:
                    f.write(result)
                counter += 1
            f.writelines(wrapper_tag)
        finally:
            p.close()
            p.join()
    else:
        for elem in element_generator(input_file):
            f.write(start_processing_element(elem))
            counter += 1

    sys.stdout.writelines("Total Account Extracted: " + str(counter))
    print("\n--- %s seconds ---" % (time.time() - start_time))
