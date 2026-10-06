"""Single runtime entry point for the optional NumPy dependency.

Import this module freely; call its functions only for explicit NumPy use.
"""

from threading import Lock


_NOT_CHECKED = object()
_NUMPY = _NOT_CHECKED
_NUMPY_IMPORT_ERROR = None
_NUMPY_LOCK = Lock()
_NUMPY_PROTOCOL_HANDLER = _NOT_CHECKED
_NUMPY_HANDLER_LOCK = Lock()


def get_numpy():
    """Import NumPy at most once, returning its module or ``None``."""
    global _NUMPY, _NUMPY_IMPORT_ERROR
    if _NUMPY is _NOT_CHECKED:
        with _NUMPY_LOCK:
            if _NUMPY is _NOT_CHECKED:
                try:
                    import numpy
                except ImportError as exc:
                    _NUMPY_IMPORT_ERROR = exc
                    _NUMPY = None
                else:
                    _NUMPY = numpy
    return _NUMPY


def numpy_available():
    """Return whether the optional NumPy module is available."""
    return get_numpy() is not None


def get_numpy_protocol_handler():
    """Return the optional NumPy handler class, or ``None`` if unavailable."""
    global _NUMPY_PROTOCOL_HANDLER
    if _NUMPY_PROTOCOL_HANDLER is _NOT_CHECKED:
        with _NUMPY_HANDLER_LOCK:
            if _NUMPY_PROTOCOL_HANDLER is _NOT_CHECKED:
                from cassandra.protocol import HAVE_CYTHON, cython_protocol_handler

                handler = None
                if HAVE_CYTHON and numpy_available():
                    from cassandra.numpy_parser import NumpyParser
                    handler = cython_protocol_handler(NumpyParser())
                _NUMPY_PROTOCOL_HANDLER = handler
    return _NUMPY_PROTOCOL_HANDLER


def require_numpy():
    """Return NumPy or raise when an explicitly requested path needs it."""
    numpy = get_numpy()
    if numpy is None:
        raise ImportError("NumPy is required for this operation") from _NUMPY_IMPORT_ERROR
    return numpy
