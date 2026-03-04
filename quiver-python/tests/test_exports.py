"""Tests for FeatureTable export methods."""

import pytest
import sys
sys.path.insert(0, 'src')

import pyarrow as pa
import quiver


def create_test_table():
    """Create a simple test table for export testing."""
    schema = pa.schema([
        pa.field("entity_id", pa.string()),
        pa.field("age", pa.int64()),
        pa.field("score", pa.float64())
    ])
    
    data = [
        pa.array(["user_1", "user_2", "user_3"]),
        pa.array([25, 30, 35]),
        pa.array([0.85, 0.92, 0.78])
    ]
    
    return pa.Table.from_arrays(data, schema=schema)


class TestFeatureTableExports:
    """Test all FeatureTable export methods."""
    
    def test_to_pandas_success(self):
        """Test successful pandas export."""
        pytest.importorskip("pandas")
        
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        df = feature_table.to_pandas()
        
        assert len(df) == 3
        assert list(df.columns) == ["entity_id", "age", "score"]
        assert df.iloc[0]["entity_id"] == "user_1"
        assert df.iloc[0]["age"] == 25
        
    def test_to_pandas_with_column_selection(self):
        """Test pandas export with specific columns."""
        pytest.importorskip("pandas")
        
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        df = feature_table.to_pandas(columns=["entity_id", "age"])
        
        assert len(df) == 3
        assert list(df.columns) == ["entity_id", "age"]
        
    def test_to_pandas_import_error(self):
        """Test pandas export when pandas is not installed."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        # Mock pandas import to fail
        import sys
        original_modules = sys.modules.copy()
        if 'pandas' in sys.modules:
            del sys.modules['pandas']
        if 'pd' in locals():
            del pd
            
        try:
            with pytest.raises(ImportError, match="pandas is required for to_pandas"):
                feature_table.to_pandas()
        finally:
            # Restore modules
            sys.modules.update(original_modules)
            
    def test_to_numpy_success(self):
        """Test successful numpy export."""
        pytest.importorskip("numpy")
        pytest.importorskip("pandas")  # Required by to_numpy
        
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        # Only export numeric columns for numpy
        arr = feature_table.to_numpy(columns=["age", "score"])
        
        assert arr.shape == (3, 2)
        assert arr[0, 0] == 25  # age
        assert arr[0, 1] == 0.85  # score
        
    def test_to_numpy_with_dtype(self):
        """Test numpy export with specific dtype."""
        pytest.importorskip("numpy")
        pytest.importorskip("pandas")
        
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        arr = feature_table.to_numpy(columns=["age", "score"], dtype="float32")
        
        assert arr.dtype.name == "float32"
        
    def test_to_numpy_import_error(self):
        """Test numpy export when numpy is not installed."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        # Mock numpy import to fail
        import sys
        original_modules = sys.modules.copy()
        if 'numpy' in sys.modules:
            del sys.modules['numpy']
            
        try:
            with pytest.raises(ImportError, match="numpy is required for to_numpy"):
                feature_table.to_numpy()
        finally:
            sys.modules.update(original_modules)
            
    def test_to_torch_import_error(self):
        """Test torch export when torch is not installed."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        with pytest.raises(ImportError, match="torch is required for to_torch"):
            feature_table.to_torch()
            
    def test_to_tensorflow_import_error(self):
        """Test tensorflow export when tensorflow is not installed."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        with pytest.raises(ImportError, match="tensorflow is required for to_tensorflow"):
            feature_table.to_tensorflow()
            
    def test_to_polars_import_error(self):
        """Test polars export when polars is not installed."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        with pytest.raises(ImportError, match="polars is required for to_polars"):
            feature_table.to_polars()
            
    def test_invalid_columns_validation(self):
        """Test that invalid column names raise ValidationError."""
        pytest.importorskip("pandas")
        
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        with pytest.raises(quiver.ValidationError, match="Invalid columns"):
            feature_table.to_pandas(columns=["nonexistent_column"])
            
    def test_column_name_property(self):
        """Test column_names property."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        assert feature_table.column_names == ["entity_id", "age", "score"]
        
    def test_shape_property(self):
        """Test shape property."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        assert feature_table.shape == (3, 3)
        
    def test_len_function(self):
        """Test len() function."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        assert len(feature_table) == 3
        
    def test_repr(self):
        """Test string representation."""
        table = create_test_table()
        feature_table = quiver.FeatureTable(table)
        
        repr_str = repr(feature_table)
        assert "FeatureTable(3 rows, 3 columns)" in repr_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
