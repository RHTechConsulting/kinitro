curl -X POST "http://localhost:8080/competitions" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mt1",
    "benchmarks": [
        {
            "provider": "metaworld",
            "benchmark_name": "MT1",
            "config": {
                "env_name": "reach-v3",
		"episodes_per_task": 3,
		"max_episode_steps": 20
            }
        }
    ],
    "points": 50
  }'
