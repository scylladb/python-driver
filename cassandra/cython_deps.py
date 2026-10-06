import warnings

try:
    from cassandra.row_parser import make_recv_results_rows
    HAVE_CYTHON = True
except ImportError:
    HAVE_CYTHON = False

from cassandra.numpy_support import numpy_available


def __getattr__(name):
    """Keep the deprecated ``HAVE_NUMPY`` attribute lazy for old callers."""
    if name != 'HAVE_NUMPY':
        raise AttributeError("module %r has no attribute %r" % (__name__, name))

    warnings.warn(
        "cassandra.cython_deps.HAVE_NUMPY is deprecated; "
        "use cassandra.numpy_support.numpy_available()",
        DeprecationWarning,
        stacklevel=2,
    )
    return numpy_available()


def __dir__():
    return sorted(set(globals()) | {'HAVE_NUMPY'})
