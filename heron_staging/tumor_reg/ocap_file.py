'''ocap_file -- least-privilege interaction with the filesystem, web

Inspired by:

  The Sash file object is quite similar to (though different from) the
  E file object, which has proven in practice to supply simple,
  intuitive, pola-disciplined interaction with the file system::

    type readable = {
         isDir : unit -> bool;
         exists : unit -> bool;
         subRdFiles : unit -> readable list;
         subRdFile : string -> readable;
         inChannel : unit -> in_channel;
         getBytes : unit -> string;
         fullPath : unit -> string;
    }


 * `How Emily Tamed the Caml`__
   Stiegler, Marc; Miller, Mark
   HPL-2006-116

__ http://www.hpl.hp.com/techreports/2006/HPL-2006-116.html

'''

from urlparse import urljoin


def Readable(path, os_path, os_listdir, openf):
    '''Wrap the python file API in the Emily/E least-authority API.

    os.path.join might not seem to need any authority,
    but its output depends on platform, so it's not a pure function.

    >>> import os
    >>> Readable('.', os.path, os.listdir, open).isDir()
    True
    '''
    def isDir():
        return os_path.isdir(path)

    def exists():
        return os_path.exists(path)

    def subRdFiles():
        return (Readable(os_path.join(path, n), os_path, os_listdir, openf)
                for n in os_listdir(path))

    def subRdFile(n):
        return Readable(os_path.join(path, n), os_path, os_listdir, openf)

    def inChannel():
        return openf(path)

    def getBytes():
        return openf(path).read()

    def fullPath():
        return os_path.abspath(path)

    return edef(isDir, exists, subRdFiles, subRdFile, inChannel,
                getBytes, fullPath)


def WebReadable(base, urlopener, RequestClass):
    '''Read-only wrapping of urllib2 in the Emily/E least-authority API.

    :param base: base URL
    :param urlopener: as from `urllib2.build_opener()`
    :param RequestClass: e.g. `urllib2.Request`

    >>> urlopener = _MockMostPagesOKButSome404('Z')
    >>> from urllib2 import Request
    >>> rdweb = WebReadable('http://example/stuff/', urlopener, Request)

    A refinement could fetch content, parse links,
    and enumerate those that point "downward", but
    this implementation doesn't supply directory functionality::

    >>> rdweb.isDir()
    False
    >>> len(rdweb.subRdFiles())
    0

    Check whether a HEAD request gives a 2xx response::
    >>> rdweb.exists()
    True
    >>> s = rdweb.subRdFile('Z')
    >>> s.fullPath()
    'http://example/stuff/Z'
    >>> s.exists()
    False

    Get a reader for the content or just the content::
    >>> rdweb.inChannel().read(4)
    'page'
    >>> rdweb.getBytes()[:4]
    'page'

    No authority is granted to URLs that don't start with `base`::
    >>> rdweb.subRdFile('x/../../y')
    Traceback (most recent call last):
       ...
    LookupError: Path does not lead to a subordinate.

    Hence traversing from `/stuff/Z` to `/stuff/x` is not allowed::
    >>> s.subRdFile('x')
    Traceback (most recent call last):
       ...
    LookupError: Path does not lead to a subordinate.

    .. todo:: consider taking a hint/name parameter for printing.
    '''
    def __repr__():
        return 'WebReadable(...)'

    def isDir():
        return False

    def exists():
        class HeadRequest(RequestClass):
            '''
            ack: doshea Jan 15 2010
            How do you send a HEAD HTTP request in Python?
            http://stackoverflow.com/questions/107405/
            '''
            def get_method(self):
                return "HEAD"

        try:
            urlopener.open(HeadRequest(base))
            return True
        except IOError:
            return False

    def subRdFiles():
        return ()

    def subRdFile(path):
        there = urljoin(base, path)
        if not there.startswith(base):
            raise LookupError('Path does not lead to a subordinate.')
        return WebReadable(there, urlopener, RequestClass)

    def inChannel():
        '''
        .. todo:: wrap result of open() for strict confinement.
        '''
        return urlopener.open(base)

    def getBytes():
        return inChannel().read()

    def fullPath():
        return base

    return edef(__repr__,
                isDir, exists, subRdFiles, subRdFile, inChannel,
                getBytes, fullPath)


def WebPostable(base, urlopener, RequestClass):
    '''Extend WebReadable with POST support.

    >>> urlopener = _MockMostPagesOKButSome404('Z')
    >>> from urllib2 import Request
    >>> doweb = WebPostable('http://example/stuff/', urlopener, Request)

    >>> doweb.post('stuff').read()
    'you posted: stuff'

    All the `ReadableWeb` methods work::

    >>> doweb.subRdFile('rd').fullPath()
    'http://example/stuff/rd'
    '''
    delegate = WebReadable(base, urlopener, RequestClass)

    def __repr__():
        return 'WebPostable(...)'

    def post(content):
        return urlopener.open(base, content)

    return edef(__repr__, post, delegate=delegate)


class _MockMostPagesOKButSome404(object):
    '''Raise 404 for pages containing given strings; otherwise succeed.
    '''
    def __init__(self, bad):
        self.bad = bad

    def open(self, request_or_address, content=None):
        from StringIO import StringIO

        try:
            address = request_or_address.get_full_url()
        except AttributeError:
            address = request_or_address

        if [txt for txt in self.bad if txt in address]:
            raise IOError('404...')

        if content:
            return StringIO('you posted: ' + content)

        return StringIO('page content...')


class Editable(object):
    #ro : readable;
    #subEdFiles : unit -> editable list;
    #subEdFile : string -> editable;
    #outChannel : unit -> out_channel;
    #setBytes : string -> unit;
    #mkDir : unit -> unit;
    #createNewFile : unit -> unit;
    #delete : unit -> unit;
    pass


def edef(*methods, **kwargs):
    '''Imitate E method suite definition.

    .. todo:: factor out overlap with `sealing.EDef`
    .. todo:: consider using a metaclass instead
    ref http://stackoverflow.com/questions/100003/what-is-a-metaclass-in-python
    '''
    lookup = dict([(f.__name__, f) for f in methods])
    delegate = kwargs.get('delegate', None)

    class EObj(object):
        def __getattr__(self, n):
            if n in lookup:
                return lookup[n]
            if delegate is not None:
                return getattr(delegate, n)
            raise AttributeError(n)

    return EObj()
