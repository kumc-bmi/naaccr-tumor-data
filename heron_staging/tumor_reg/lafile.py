'''lafile -- least authority filesystem access
'''

from collections import namedtuple


class Rd(namedtuple('Rd',
                    ['path',
                     'inChannel',
                     'subRdFile', 'subRdFiles'])):
    def __repr__(self):
        return '<file:%s>' % self.path

    def __div__(self, n):
        return self.subRdFile(n)


def osRd(path_, openrd, osp, listdir):
    path = osp.abspath(osp.normpath(path_))

    def sub(n):
        there = osp.abspath(osp.normpath(osp.join(path, n)))
        if not there.startswith(path):
            raise IOError('%s not subordinate to %s' % (there, path))
        return osRd(there, openrd, osp, listdir)

    def subs():
        return [sub(n) for n in listdir(path)]

    return Rd(path, lambda: openrd(path), sub, subs)
