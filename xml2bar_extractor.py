import json
import os
import sys
import time
import multiprocessing
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from lxml import etree


def validate_arguments():
    if len(sys.argv) < 4:
        print_help()
    if not os.path.isfile(sys.argv[1]):
        sys.stderr.write("ERROR: The input file '" +
                         sys.argv[1] + "' does not exist!\n")
        sys.exit(1)
    if not os.path.isfile(sys.argv[2]):
        sys.stderr.write("ERROR: The template file '" +
                         sys.argv[1] + "' does not exist!\n")
        sys.exit(1)
    if not os.path.exists(os.path.dirname(sys.argv[3])):
        sys.stderr.write("ERROR: The output file path '" +
                         os.path.dirname(sys.argv[3]) + "' does not exist!\n")
        sys.exit(1)


def print_help():
    sys.stderr.write("ERROR: Missing Arguments!\n")
    sys.stdout.writelines("""This program takes 3 arguments.
Example: xml2bar_parser.py input_file template_file output_file
    input_file      = The XML file that needed to be parsed.
    template_file   = The JSON file that will be used to create the bar delimited file.
    output_file     = Name of the output bar delimited file.\n""")
    sys.exit(1)


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


def process_element(elem, path):
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
    return output_line


def start_processing_element(params):
    elem_string, d = params
    # elem_string = re.sub(r"xmlns=\".*?\"", "", elem_string)
    tree = etree.fromstring(elem_string)
    etree.cleanup_namespaces(tree)
    return process_element(tree, d)


def element_generator(input_file):
    try:
        with open(sys.argv[2]) as template_file:
            template = json.loads(template_file.read(), object_pairs_hook=OrderedDict)
    except Exception as e:
        sys.stderr.write("ERROR: Invalid JSON template file! " + e.message + "\n")
        sys.exit()

    root_tag = next(iter(template))
    root_tag_with_namespace = "{*}" + root_tag
    for event, elem in etree.iterparse(input_file, tag=root_tag_with_namespace):
        yield (etree.tostring(elem), template.get(root_tag))
        elem.clear()


def blocks(files, size=65536):
    while True:
        b = files.read(size)
        if not b:
            break
        yield b


if __name__ == '__main__':
    start_time = time.time()
    validate_arguments()
    f = open(sys.argv[3], 'w+')
    p = multiprocessing.Pool()
    counter = 0
    f.write("<statementProduction>\n")
    for result in p.imap_unordered(start_processing_element, element_generator(sys.argv[1]), chunksize=100):
        if (result):
            f.write("<envelop>\n")
            f.write(result + str("\n"))
            f.write("</envelop>\n")
            counter += 1
    f.writelines("</statementProduction>\n")

    sys.stdout.writelines("Total Account Extracted: " + str(counter))
    # f.write("--------------" + str(counter) + "------------\n")

    # counter = 0
    # for event, elem in etree.iterparse('data.xml', tag="{*}envelope"):
    #     f.write(start_processing_element((etree.tostring(elem), d)))
    #     counter +=1
    #     f.write("--------------"+str(counter)+"------------\n")
    #     elem.clear()
    #
    # print "Counted: ",counter

    # elem_counter = 0
    # file_counter = 0
    # f = open(sys.argv[3], 'w+')
    # for event, elem in etree.iterparse('data.xml', tag="{*}envelope"):
    #     f.write(re.sub(r"xmlns=\".*?\"", "", etree.tostring(elem)))
    #     if elem_counter > 500:
    #         elem_counter = 0
    #         file_counter += 1
    #         f.write("</statementProduction>")
    #         f = open("parts/data"+str(file_counter)+".xml", 'w+')
    #         f.write("<statementProduction>")
    #     elem_counter += 1
    #     elem.clear()

    print("\n--- %s seconds ---" % (time.time() - start_time))
