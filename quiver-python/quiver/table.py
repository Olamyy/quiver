from typing import List, Optional, Any, TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    import pandas # type: ignore
    import numpy # type: ignore

from .exceptions import QuiverValidationError


class FeatureTable:
    """Wrapper around PyArrow Table with ML-friendly export methods.

    Provides zero-copy exports to different ML frameworks while maintaining
    Arrow's memory efficiency and type safety.
    """

    def __init__(self, table: pa.Table) -> None: # noqa
        if not isinstance(table, pa.Table): # noqa
            raise QuiverValidationError(f"Expected PyArrow Table, got {type(table)}")
        self._table = table

    @property
    def schema(self) -> pa.Schema: # noqa
        """Arrow schema of the table."""
        return self._table.schema

    @property
    def column_names(self) -> List[str]:
        """List of column names."""
        return self._table.column_names

    @property
    def shape(self) -> tuple[int, int]:
        """Shape as (num_rows, num_columns)."""
        return self._table.num_rows, self._table.num_columns

    def __len__(self) -> int:
        """Number of rows in the table."""
        return self._table.num_rows

    def __repr__(self) -> str:
        return f"FeatureTable({self._table.num_rows} rows, {self._table.num_columns} columns)"

    def to_pandas(self) -> "pandas.DataFrame":
        """Export to pandas DataFrame."""
        try:
            import pandas
        except ImportError:
            raise ImportError("pandas is required for to_pandas()")
        return self._table.to_pandas()

    def to_numpy(self, columns: Optional[List[str]] = None) -> "numpy.ndarray":
        """Export to numpy array."""
        try:
            import numpy
        except ImportError:
            raise ImportError("numpy is required for to_numpy()")

        if columns:
            table = self._table.select(columns)
        else:
            table = self._table

        return table.to_pandas().values

    def to_torch(self, columns: Optional[List[str]] = None) -> Any:
        """Export to PyTorch tensor."""
        try:
            import torch
        except ImportError:
            raise ImportError("torch is required for to_torch()")

        arr = self.to_numpy(columns)
        return torch.from_numpy(arr)

    def to_tensorflow(self, columns: Optional[List[str]] = None) -> Any:
        """Export to TensorFlow tensor."""
        try:
            import tensorflow as tf
        except ImportError:
            raise ImportError("tensorflow is required for to_tensorflow()")

        arr = self.to_numpy(columns)
        return tf.constant(arr)

    def to_polars(self, columns: Optional[List[str]] = None) -> Any:
        """Export to Polars DataFrame."""
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars is required for to_polars()")

        if columns:
            table = self._table.select(columns)
        else:
            table = self._table

        return pl.from_arrow(table)
