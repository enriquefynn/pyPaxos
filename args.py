from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('id',      help='node identifier', type=int)
parser.add_argument('config',  help='config file', default="config.txt")
parser.add_argument('-v',      help='verbosity', action='count', default=0)
parser.add_argument('-b',      help='Benchmark number of msgs', default=0, type=int)
args = parser.parse_args()
