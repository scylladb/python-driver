try:
    from cassandra.row_parser import make_recv_results_rows
    HAVE_CYTHON = True
except ImportError:
    HAVE_CYTHON = False


def __getattr__(name):
    """Probe for optional NumPy support only when a caller asks for it."""
    if name != 'HAVE_NUMPY':
        raise AttributeError("module %r has no attribute %r" % (__name__, name))

    try:
        import numpy  # noqa: F401
        have_numpy = True
    except ImportError:
        have_numpy = False

    return globals().setdefault(name, have_numpy)


def __dir__():
    return sorted(set(globals()) | {'HAVE_NUMPY'})
