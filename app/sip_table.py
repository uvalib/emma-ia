# app/sip_table.py
#
# A table associating a SID (submission ID) with a Sip instance.


from app.sip import *


class SipTable:
    """
    A table of entries keyed by SID (submission ID) which are associated with
    the submissions found in the designated AWS bucket.
    """

    # =========================================================================
    # :section:
    # =========================================================================

    def __init__(self):
        self._table = {}

    def __len__(self) -> int:
        return len(self._table)

    def __getitem__(self, sid) -> Sip:
        if sid not in self._table:
            self._table[sid] = Sip()
        return self._table[sid]

    def __setitem__(self, sid, value) -> Sip:
        self._table[sid] = value if isinstance(value, Sip) else Sip(value)
        return self._table[sid]

    def __delitem__(self, sid):
        del self._table[sid]

    def __iter__(self):
        return iter(self._table)

    def __contains__(self, sid) -> bool:
        return sid in self._table

    def __repr__(self) -> str:
        return pp.pformat(self._table)

    def __bool__(self):
        return len(self._table) > 0

    # =========================================================================
    # :section:
    # =========================================================================

    def items(self) -> ItemsView[str, Sip]:
        return self._table.items()

    def keys(self) -> KeysView[str]:
        return self._table.keys()

    def values(self) -> ValuesView[Sip]:
        return self._table.values()

    # =========================================================================
    # :section:
    # =========================================================================

    def submission_ids(self) -> List[str]:
        return list(self.keys())

    def sips(self) -> List[Sip]:
        return list(self.values())
