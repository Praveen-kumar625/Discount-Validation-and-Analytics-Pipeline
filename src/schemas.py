from typing import Dict, Any

# =============================================================================
# DATA SCHEMAS & TYPE DEFINITIONS
# =============================================================================

COUPON_DATA_SCHEMA: Dict[str, str] = {
    'order_id': 'string',
    'product_id': 'string',
    'product_category': 'string',
    'original_price': 'float64',
    'coupon_code': 'string',
    'order_date': 'datetime64[ns]'
}

RULES_DATA_SCHEMA: Dict[str, Any] = {
    'coupon_code': 'string',
    'expiry_date': 'datetime64[ns]',
    'applicable_scope': 'string',
    'discount_type': 'string',
    'discount_value': 'float64',
    'max_cap': 'float64',
    'is_exclusive': 'int64'
}
