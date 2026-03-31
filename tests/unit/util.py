# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from unittest.mock import NonCallableMagicMock


def check_sequence_consistency(ordered_sequence, equal=False):
    for i, el in enumerate(ordered_sequence):
        for previous in ordered_sequence[:i]:
            _check_order_consistency(previous, el, equal)
        for posterior in ordered_sequence[i + 1:]:
            _check_order_consistency(el, posterior, equal)


def _check_order_consistency(smaller, bigger, equal=False):
    assert smaller <= bigger
    assert bigger >= smaller
    if equal:
        assert smaller == bigger
    else:
        assert smaller != bigger
        assert smaller < bigger
        assert bigger > smaller


class HashableMock(NonCallableMagicMock):
    """A Mock subclass that is safely hashable and usable in sets/dicts.

    NonCallableMagicMock's __init__ (via MagicMixin) replaces __hash__
    on the *type* with a MagicMock object.  That MagicMock is not
    thread-safe, so concurrent hash() calls on the same instance —
    e.g. ``connection in self._trash`` in pool.py — can raise
    ``TypeError: __hash__ method should return an integer`` on Windows.

    We fix this by restoring a plain function as the class-level
    __hash__ after super().__init__ runs, so hash() always resolves to
    a real function (id-based) instead of a MagicMock callable.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Restore a real __hash__ after MagicMixin overwrites it.
        type(self).__hash__ = HashableMock._id_hash

    @staticmethod
    def _id_hash(self):
        return id(self)
