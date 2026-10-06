"""Reject direct runtime NumPy imports outside the optional-dependency gateway."""

import ast
import io
from pathlib import Path
import tokenize


ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / 'cassandra'
GATEWAY = DRIVER / 'numpy_support.py'
NUMPY_PARSER = DRIVER / 'numpy_parser.pyx'

def cython_numpy_imports(source, allow_cimport=False):
    """Yield lines with NumPy imports in Cython source."""
    statement = []
    tokens = tokenize.generate_tokens(io.StringIO(source).readline)
    for token in tokens:
        if token.type in (tokenize.NEWLINE, tokenize.ENDMARKER) or token.string == ';':
            for index, current in enumerate(statement):
                if current.string not in ('import', 'cimport'):
                    continue
                if current.string == 'cimport' and allow_cimport:
                    continue
                from_tokens = [item for item in statement[:index] if item.string == 'from']
                if from_tokens:
                    if statement[statement.index(from_tokens[-1]) + 1].string == 'numpy':
                        yield current.start[0]
                elif any(item.string == 'numpy' and
                         (position == index + 1 or statement[position - 1].string == ',')
                         for position, item in enumerate(statement[index + 1:], index + 1)):
                    yield current.start[0]
            statement = []
        elif token.type not in (tokenize.NL, tokenize.INDENT, tokenize.DEDENT,
                                tokenize.COMMENT):
            statement.append(token)


def direct_imports(path):
    """Yield line numbers of disallowed NumPy imports in driver source."""
    source = path.read_text(encoding='utf-8')
    if path.suffix == '.pyx':
        yield from cython_numpy_imports(source, allow_cimport=path == NUMPY_PARSER)
        return

    for node in ast.walk(ast.parse(source, filename=str(path))):
        if isinstance(node, ast.Import):
            if any(alias.name == 'numpy' or alias.name.startswith('numpy.')
                   for alias in node.names):
                yield node.lineno
        elif isinstance(node, ast.ImportFrom):
            if node.module and (node.module == 'numpy' or node.module.startswith('numpy.')):
                yield node.lineno


def main():
    """Check all driver sources, including files untouched by this PR."""
    violations = []
    for path in sorted(DRIVER.rglob('*')):
        if path.suffix not in ('.py', '.pyx') or path == GATEWAY:
            continue
        violations.extend((path.relative_to(ROOT), line) for line in direct_imports(path))

    for path, line in violations:
        print(f'{path}:{line}: import NumPy through cassandra.numpy_support')
    return bool(violations)


if __name__ == '__main__':
    raise SystemExit(main())
