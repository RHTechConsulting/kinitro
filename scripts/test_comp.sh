curl -X POST "http://localhost:8080/competitions" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mt1 baby",
    "benchmarks": [
        {
            "provider": "metaworld",
            "benchmark_name": "MT1",
            "config": {
                "env_name": "reach-v3"
            }
        }
    ],
    "points": 50
  }'

#   class EvalJobMessage(BaseModel):
#     """Message for broadcasting evaluation jobs from backend to validators."""
# 
#     message_type: str = "eval_job"
#     job_id: str
#     competition_id: str
#     miner_hotkey: str
#     hf_repo_id: str
#     benchmarks: list[dict]
#     timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

