from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class GraderRecord:
    user_id: str
    oauth_consumer_key: str
    lis_result_sourcedid: str
    lis_outcome_service_url: str
    is_correct: bool | None
    attempt_type: str
    created_at: datetime
