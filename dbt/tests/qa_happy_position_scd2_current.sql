select position_id
from {{ ref('qa_happy_position_scd2') }}
group by position_id
having sum(case when is_current then 1 else 0 end) <> 1
