# backend/schemas/rule.py
from pydantic import BaseModel
from typing import Optional

class ResolveRequest(BaseModel):
    rule_topic_id: int
    query_date: str
    query_date_type: str
    anchor_type_id: Optional[int] = None
    anchor_value: Optional[str] = None