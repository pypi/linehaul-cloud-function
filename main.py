import arrow
import cattr

import os
import json
import gzip
import sys

from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path

from linehaul.events.parser import parse, Download, Simple

_cattr = cattr.Converter()
_cattr.register_unstructure_hook(arrow.Arrow, lambda o: o.format('YYYY-MM-DD HH:mm:ss ZZ'))

input_file = sys.argv[1]
identifier = os.path.basename(input_file).split('-')[-1].split('.')[0]

class OutputFiles(defaultdict):

    def __init__(self, stack, *args, **kwargs):
        self.stack = stack
        super(OutputFiles, self).__init__(*args, **kwargs)

    def __missing__(self, key):
        Path(os.path.dirname(key)).mkdir(parents=True, exist_ok=True)
        ret = self[key] = self.stack.enter_context(open(key, 'wb'))
        return ret

prefix = {
    Simple.__name__: 'simple_requests',
    Download.__name__: 'downloads',
}

with ExitStack() as stack:
    f = stack.enter_context(gzip.open(input_file, 'rt'))
    output_files = OutputFiles(stack)
    for line in f:
        try:
            res = parse(line)
            if res is not None:
                partition = res.timestamp.format('YYYYMMDD')
                output_files[f'results/{prefix[res.__class__.__name__]}/{partition}/{identifier}.json'].write(json.dumps(_cattr.unstructure(res)).encode() + b'\n')
            else:
                output_files[f'results/unprocessed/{identifier}.txt'].write(line.encode() + b'\n')
        except Exception as e:
            output_files[f'results/unprocessed/{identifier}.txt'].write(line.encode() + b'\n')
    

#with open('downloads-result.json', 'wb') as wf:
#    with gzip.open('logs/downloads/2020/02/29/22/00/2020-02-29T22:00:00.000-RR11GaIPOBYjuohiUdWt.log.gz', 'rt') as f:
#        for line in f:
#            try:
#                res = parse(line)
#                if res is not None:
#                    wf.write(json.dumps(_cattr.unstructure(res)).encode() + b'\n')
#            except:
#                print(line)

#with open('simple-result.json', 'wb') as wf:
#    with gzip.open('logs/simple/2020/02/29/22/00/2020-02-29T22:00:00.000-J6jH0weiN3a7yBa6zZY-.log.gz', 'rt') as f:
#        for line in f:
#            try:
#                res = parse(line)
#                if res is not None:
#                    wf.write(json.dumps(_cattr.unstructure(res)).encode() + b'\n')
#            except Exception as e:
#                print(e)
#                print(line)
