"""Lightweight helpers for generating MetaWorld benchmark tasks.

These utilities mirror the behaviour of the upstream benchmarks but allow
controlling how many goal variants are generated per environment. This reduces
setup time significantly compared to the default of 50 goals per task.
"""

from __future__ import annotations

import logging
import pickle
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import numpy as np
from metaworld import env_dict as _env_dict
from metaworld.types import Task

EnvDict = _env_dict.EnvDict
EnvArgsKwargsDict = _env_dict.EnvArgsKwargsDict

logger = logging.getLogger(__name__)

_ML_OVERRIDE = dict(partially_observable=True)
_MT_OVERRIDE = dict(partially_observable=False)


@dataclass(frozen=True)
class BenchmarkData:
    """Task and class configuration for a MetaWorld benchmark."""

    train_classes: EnvDict
    test_classes: EnvDict
    train_tasks: list[Task]
    test_tasks: list[Task]


def _encode_task(env_name: str, data: dict[str, Any]) -> Task:
    return Task(env_name=env_name, data=pickle.dumps(data))


def _make_tasks_limited(
    classes: EnvDict,
    args_kwargs: EnvArgsKwargsDict,
    kwargs_override: dict[str, Any],
    tasks_per_env: int,
    seed: int | None = None,
) -> list[Task]:
    if tasks_per_env < 1:
        raise ValueError("tasks_per_env must be >= 1")

    if seed is not None:
        prev_state = np.random.get_state()
        np.random.seed(seed)

    tasks: list[Task] = []
    try:
        for env_name, args in args_kwargs.items():
            kwargs = dict(args["kwargs"])
            env_cls = classes[env_name]
            env = env_cls()
            env._freeze_rand_vec = False  # noqa: SLF001 - rely on metaworld internals
            env._set_task_called = True  # noqa: SLF001

            if "task_id" in kwargs:
                del kwargs["task_id"]
            env._set_task_inner(**kwargs)  # noqa: SLF001

            rand_vecs = []
            try:
                for _ in range(tasks_per_env):
                    env.reset()
                    rand_vec = getattr(env, "_last_rand_vec", None)
                    if rand_vec is None:
                        raise RuntimeError(
                            f"Environment {env_name} did not expose _last_rand_vec"
                        )
                    rand_vecs.append(np.array(rand_vec, copy=True))
            finally:
                env.close()

            unique_goals = np.unique(np.array(rand_vecs), axis=0)
            if unique_goals.shape[0] < len(rand_vecs):
                logger.debug(
                    "Generated %d unique goals (requested %d) for %s",
                    unique_goals.shape[0],
                    len(rand_vecs),
                    env_name,
                )

            for rand_vec in rand_vecs:
                task_kwargs = dict(args["kwargs"])
                if "task_id" in task_kwargs:
                    del task_kwargs["task_id"]
                task_kwargs.update(dict(rand_vec=rand_vec, env_cls=env_cls))
                task_kwargs.update(kwargs_override)
                tasks.append(_encode_task(env_name, task_kwargs))
    finally:
        if seed is not None:
            np.random.set_state(prev_state)

    return tasks


def _create_mt_benchmark(
    classes: EnvDict,
    args_kwargs: EnvArgsKwargsDict,
    tasks_per_env: int,
    seed: int | None,
) -> BenchmarkData:
    train_tasks = _make_tasks_limited(
        classes, args_kwargs, _MT_OVERRIDE, tasks_per_env, seed
    )
    return BenchmarkData(
        train_classes=classes,
        test_classes=OrderedDict(),
        train_tasks=train_tasks,
        test_tasks=[],
    )


def _create_ml_benchmark(
    train_classes: EnvDict,
    test_classes: EnvDict,
    train_kwargs: EnvArgsKwargsDict,
    test_kwargs: EnvArgsKwargsDict,
    tasks_per_env: int,
    seed: int | None,
) -> BenchmarkData:
    train_tasks = _make_tasks_limited(
        train_classes, train_kwargs, _ML_OVERRIDE, tasks_per_env, seed
    )
    test_tasks = _make_tasks_limited(
        test_classes, test_kwargs, _ML_OVERRIDE, tasks_per_env, seed
    )
    return BenchmarkData(
        train_classes=train_classes,
        test_classes=test_classes,
        train_tasks=train_tasks,
        test_tasks=test_tasks,
    )


def load_benchmark_definition(
    name: str,
    *,
    tasks_per_env: int,
    env_name: str | None = None,
    seed: int | None = None,
) -> BenchmarkData:
    """Create a lightweight benchmark definition with limited tasks per env."""

    match name:
        case "MT1":
            if env_name is None:
                raise ValueError("MT1 requires env_name")
            if env_name not in _env_dict.ALL_V3_ENVIRONMENTS:
                raise ValueError(f"Unknown MT1 environment: {env_name}")
            cls = _env_dict.ALL_V3_ENVIRONMENTS[env_name]
            train_classes: EnvDict = OrderedDict([(env_name, cls)])
            args_kwargs = {env_name: _env_dict.ML1_args_kwargs[env_name]}
            train_tasks = _make_tasks_limited(
                train_classes, args_kwargs, _MT_OVERRIDE, tasks_per_env, seed
            )
            return BenchmarkData(
                train_classes=train_classes,
                test_classes=train_classes,
                train_tasks=train_tasks,
                test_tasks=[],
            )
        case "MT10":
            return _create_mt_benchmark(
                _env_dict.MT10_V3, _env_dict.MT10_V3_ARGS_KWARGS, tasks_per_env, seed
            )
        case "MT25":
            return _create_mt_benchmark(
                _env_dict.MT25_V3, _env_dict.MT25_V3_ARGS_KWARGS, tasks_per_env, seed
            )
        case "MT50":
            return _create_mt_benchmark(
                _env_dict.MT50_V3, _env_dict.MT50_V3_ARGS_KWARGS, tasks_per_env, seed
            )
        case "ML10":
            return _create_ml_benchmark(
                _env_dict.ML10_V3["train"],
                _env_dict.ML10_V3["test"],
                _env_dict.ML10_ARGS_KWARGS["train"],
                _env_dict.ML10_ARGS_KWARGS["test"],
                tasks_per_env,
                seed,
            )
        case "ML25":
            return _create_ml_benchmark(
                _env_dict.ML25_V3["train"],
                _env_dict.ML25_V3["test"],
                _env_dict.ML25_ARGS_KWARGS["train"],
                _env_dict.ML25_ARGS_KWARGS["test"],
                tasks_per_env,
                seed,
            )
        case "ML45":
            return _create_ml_benchmark(
                _env_dict.ML45_V3["train"],
                _env_dict.ML45_V3["test"],
                _env_dict.ML45_ARGS_KWARGS["train"],
                _env_dict.ML45_ARGS_KWARGS["test"],
                tasks_per_env,
                seed,
            )
        case _:
            raise ValueError(f"Unsupported MetaWorld benchmark: {name}")
