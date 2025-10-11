"""
Drools syntax parsing utilities
"""
import re
from typing import Dict, Optional, Tuple

def parse_drools_syntax(syntax: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Parse drools syntax string to extract claim item and max quantity
    Returns (claim_item, max_quantity)
    """
    # Example: [or Claim item has 1 to 5 of "Vitamin B Complex Inj (Per Ml)"]
    if not syntax:
        return None, None
    
    # Extract quoted item
    item_match = re.search(r'"([^"]+)"', syntax)
    if not item_match:
        return None, None
    claim_item = item_match.group(1)
    
    # Extract max quantity
    quantity_match = re.search(r'has \d+ to (\d+) of', syntax)
    max_quantity = int(quantity_match.group(1)) if quantity_match else None
    
    return claim_item, max_quantity