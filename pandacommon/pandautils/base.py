##############################
# Base classes in PanDA/JEDI #
##############################

from collections.abc import Sequence
from typing import Any


class SpecBase(object):
    """
    Base class of specification
    """

    # attributes
    attributes: tuple[str, ...] = ()
    # attributes which have 0 by default
    _zeroAttrs: tuple[str, ...] = ()
    # attributes to force update
    _forceUpdateAttrs: tuple[str, ...] = ()
    # mapping between sequence and attr
    _seqAttrMap: dict[str, str] = {}
    # map of changed attributes. Declared without a value: __init__ installs it on the
    # instance, and a class attribute here would be shared by every spec ever made
    _changedAttrs: dict[str, Any]

    # constructor
    def __init__(self) -> None:
        # install attributes
        for attr in self.attributes:
            self._orig_setattr(attr, None)
        # map of changed attributes
        self._orig_setattr("_changedAttrs", {})

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name: str, value: Any) -> None:
        oldVal = getattr(self, name)
        self._orig_setattr(name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal or name in self._forceUpdateAttrs:
            self._changedAttrs[name] = value

    def _orig_setattr(self, name: str, value: Any) -> None:
        """
        original setattr method
        """
        super().__setattr__(name, value)

    def resetChangedList(self) -> None:
        """
        reset changed attribute list
        """
        self._orig_setattr("_changedAttrs", {})

    def forceUpdate(self, name: str) -> None:
        """
        force update the attribute
        """
        if name in self.attributes:
            self._changedAttrs[name] = getattr(self, name)

    def valuesMap(self, useSeq: bool = False, onlyChanged: bool = False) -> dict[str, Any]:
        """
        return map of values
        """
        ret: dict[str, Any] = {}
        for attr in self.attributes:
            # use sequence
            if useSeq and attr in self._seqAttrMap:
                continue
            # only changed attributes
            if onlyChanged:
                if attr not in self._changedAttrs:
                    continue
            val = getattr(self, attr)
            if val is None:
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            ret[f":{attr}"] = val
        return ret

    def pack(self, values: Sequence[Any]) -> None:
        """
        pack tuple into spec
        """
        for i in range(len(self.attributes)):
            attr = self.attributes[i]
            val = values[i]
            self._orig_setattr(attr, val)

    @classmethod
    def columnNames(cls, prefix: str | None = None) -> str:
        """
        return column names for INSERT
        """
        attr_list = []
        for attr in cls.attributes:
            if prefix is not None:
                attr_list.append(f"{prefix}.{attr}")
            else:
                attr_list.append(f"{attr}")
        ret = ",".join(attr_list)
        return ret

    @classmethod
    def bindValuesExpression(cls, useSeq: bool = True) -> str:
        """
        return expression of bind variables for INSERT
        """
        attr_list = []
        for attr in cls.attributes:
            if useSeq and attr in cls._seqAttrMap:
                attr_list.append(f"{cls._seqAttrMap[attr]}")
            else:
                attr_list.append(f":{attr}")
        attrs_str = ",".join(attr_list)
        ret = f"VALUES({attrs_str}) "
        return ret

    def bindUpdateChangesExpression(self) -> str:
        """
        return an expression of bind variables for UPDATE to update only changed attributes
        """
        attr_list = []
        for attr in self.attributes:
            if attr in self._changedAttrs:
                attr_list.append(f"{attr}=:{attr}")
        attrs_str = ",".join(attr_list)
        ret = f"{attrs_str} "
        return ret
