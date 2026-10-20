"""fabric-cdc-silver: CDC change logs -> clean silver tables, with schema contracts."""
from .core import CdcSpec, SchemaDrift, check_contract, to_silver

__all__ = ["CdcSpec", "SchemaDrift", "check_contract", "to_silver"]
__version__ = "0.1.0"
