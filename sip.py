# sip.py


from utility import *


class Sip:
    """
    A submission information package instance, which includes the AWS object
    keys associated with the submission, as well as dynamic information used to
    manage processing.
    """

    # =========================================================================
    # :section: Properties
    # =========================================================================

    @property
    def completed(self) -> bool:
        """
        Whether the associated submission has been transmitted to IA.
        """
        return self._completed

    @completed.setter
    def completed(self, value: bool):
        """
        Indicate if the associated submission has been transmitted to IA.
        """
        self._completed = value

    @property
    def metadata(self) -> Optional[dict]:
        """
        Extracted EMMA metadata.
        """
        return self._metadata

    @metadata.setter
    def metadata(self, value: Optional[dict]):
        """
        Assign extracted EMMA metadata.
        """
        self._metadata = value

    @property
    def entry(self) -> dict:
        """
        The elements of the submission information - package plus data file.
        """
        return self._entry

    _keys = ['package', 'data_file']

    @property
    def package(self) -> Optional[str]:
        """
        The name (AWS object key) of the package object.
        """
        return self._entry.get('package')

    @property
    def data_file(self) -> Optional[str]:
        """
        The name (AWS object key) of the data file.
        """
        return self._entry.get('data_file')

    # =========================================================================
    # :section:
    # =========================================================================

    def __init__(self, values=None, **kwargs):
        self._completed = False
        self._metadata  = None
        self._entry     = {}
        if isinstance(values, Sip):
            values = values.entry
        elif not isinstance(values, dict):
            values = kwargs
        for part in self._keys:
            self._entry[part] = values[part] if part in values else None

    def __len__(self) -> int:
        p = self.package
        d = self.data_file
        return 2 if (p and d) else 1 if (p or d) else 0

    def __getitem__(self, part) -> Optional[str]:
        if not isinstance(part, str):
            raise TypeError(f'part must be "str" not "{part.__class__}"')
        if part not in self._keys:
            raise KeyError(f'part must be in {self._keys}')
        return self._entry[part]

    def __setitem__(self, part, value) -> Optional[str]:
        self._entry[part] = value
        return self._entry[part]

    def __delitem__(self, part):
        raise RuntimeError(f'cannot delete "{part}"')

    def __iter__(self):
        return iter(self._entry)

    def __contains__(self, part) -> bool:
        return not not self[part]

    def __repr__(self) -> str:
        return pp.pformat(self._entry)
