import yaml
import os

here = os.path.dirname(os.path.realpath(__file__))

def load_conf():
    with open("%s/config.yml" % (here)) as f:
        doc = yaml.load(f)
    return doc
