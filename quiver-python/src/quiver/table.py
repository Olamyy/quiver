"""FeatureTable wrapper around PyArrow Table with ML-friendly exports."""

from typing import List, Optional, Union, TYPE_CHECKING

try:
    import pyarrow as pa
except ImportError:
    pa = None

from .exceptions import ValidationError

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd


class FeatureTable:
    """Wrapper around PyArrow Table with ML-friendly export methods."""
    
    def __init__(self, table: "pa.Table") -> None:
        if pa is None:
            raise ImportError("PyArrow is required but not installed")
        if not isinstance(table, pa.Table):
            raise ValidationError(f"Expected PyArrow Table, got {type(table)}")
        self._table = table
        
    @property
    def column_names(self) -> List[str]:
        """List of column names."""
        return self._table.column_names
        
    @property
    def shape(self) -> tuple[int, int]:
        """Shape as (num_rows, num_columns)."""
        return (self._table.num_rows, self._table.num_columns)
        
    def __len__(self) -> int:
        """Number of rows in the table."""
        return self._table.num_rows
        
    def to_pandas(self) -> "pd.DataFrame":
        """Export to pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError("pandas is required for to_pandas()") from e
        return self._table.to_pandas()


__all__ = ["FeatureTable"]
