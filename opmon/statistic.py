"""Implementations of custom statistics that can be referenced in metric configs."""

import re
from abc import ABC
from typing import Any, Dict, List

import attr

from opmon import Metric
from opmon.errors import StatisticNotImplementedForTypeException


@attr.s(auto_attribs=True)
class Statistic(ABC):
    """
    Abstract representation of a statistic.

    A statistic is a transformation that accepts a table of per-client aggregates and
    returns a table representing a summary of the aggregates with respect to the branches
    of the experiment.
    """

    @classmethod
    def name(cls):
        """Return snake-cased name of the statistic."""
        # https://stackoverflow.com/a/1176023
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", cls.__name__)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()

    def compute(self, metric: Metric) -> str:
        """
        Return the statistic computation as SQL.

        The SQL needs to return ARRAY<STRUCT<
            metric STRING,
            statistic STRING,
            point FLOAT64,
            lower FLOAT64,
            upper FLOAT64,
            parameter STRING
        >>
        """
        if metric.type == "scalar":
            return self._scalar_compute(metric)
        elif metric.type == "histogram":
            return self._histogram_compute(metric)
        else:
            raise StatisticNotImplementedForTypeException(
                f"Statistic {self.name()} not implemented for type {metric.type} ({metric.name})"
            )

    def _scalar_compute(self, metric: Metric) -> str:
        raise StatisticNotImplementedForTypeException(
            f"Statistic {self.name()} not implemented for type {metric.type} ({metric.name})"
        )

    def _histogram_compute(self, metric: Metric) -> str:
        raise StatisticNotImplementedForTypeException(
            f"Statistic {self.name()} not implemented for type {metric.type} ({metric.name})"
        )

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        """Create a class instance with the specified config parameters."""
        return cls(**config_dict)  # type: ignore


class Count(Statistic):
    """Count statistic."""

    def _scalar_compute(self, metric: Metric):
        return f"""ARRAY<STRUCT<
                metric STRING,
                statistic STRING,
                point FLOAT64,
                lower FLOAT64,
                upper FLOAT64,
                parameter STRING
            >>[
            STRUCT(
                "{metric.name}" AS metric,
                "{self.name()}" AS statistic,
                COUNT({metric.name}) AS point,
                NULL AS lower,
                NULL AS upper,
                NULL AS parameter
            )
        ]"""


class Sum(Statistic):
    """Sum statistic."""

    def _scalar_compute(self, metric: Metric):
        return f"""ARRAY<STRUCT<
                metric STRING,
                statistic STRING,
                point FLOAT64,
                lower FLOAT64,
                upper FLOAT64,
                parameter STRING
            >>[
            STRUCT(
                "{metric.name}" AS metric,
                "{self.name()}" AS statistic,
                SUM({metric.name}) AS point,
                NULL AS lower,
                NULL AS upper,
                NULL AS parameter
            )
        ]"""


class Mean(Statistic):
    """Mean statistic."""

    def _scalar_compute(self, metric: Metric):
        return f"""ARRAY<STRUCT<
                metric STRING,
                statistic STRING,
                point FLOAT64,
                lower FLOAT64,
                upper FLOAT64,
                parameter STRING
            >>[
            STRUCT(
                "{metric.name}" AS metric,
                "{self.name()}" AS statistic,
                AVG({metric.name}) AS point,
                NULL AS lower,
                NULL AS upper,
                NULL AS parameter
            )
        ]"""


class Quantile(Statistic):
    """Quantile statistic."""

    number_of_quantiles: int = 100
    quantile: int = 50

    def _scalar_compute(self, metric: Metric):
        return f"""ARRAY<STRUCT<
                metric STRING,
                statistic STRING,
                point FLOAT64,
                lower FLOAT64,
                upper FLOAT64,
                parameter STRING
            >>[
            STRUCT(
                "{metric.name}" AS metric,
                "{self.name()}" AS statistic,
                APPROX_QUANTILES(
                    {metric.name},
                    {self.number_of_quantiles}
                )[OFFSET({self.quantile})] AS point,
                NULL AS lower,
                NULL AS upper,
                {self.quantile} AS parameter
            )
        ]"""


@attr.s(auto_attribs=True)
class Percentile(Statistic):
    """Percentile with confidence interval statistic."""

    percentiles: List[int] = [50, 90, 99]

    def _scalar_compute(self, metric: Metric):
        return f"""
            jackknife_percentile_ci(
                {self.percentiles},
                STRUCT(
                    histogram_normalized_sum(
                        [STRUCT<values ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(
                            ARRAY_AGG(
                                STRUCT<key FLOAT64, value FLOAT64>(
                                    COALESCE(
                                        mozfun.glam.histogram_bucket_from_value(
                                            {metric.name}_buckets,
                                            SAFE_CAST({metric.name} AS FLOAT64)
                                        ), 0.0
                                    ), 1.0
                                ) IGNORE NULLS
                            )
                        )], 1.0
                    )
                ),
                "{metric.name}"
            )
        """

    def _histogram_compute(self, metric: Metric):
        return f"""
            jackknife_percentile_ci(
                {self.percentiles},
                STRUCT(
                    histogram_normalized_sum(
                        ARRAY_CONCAT_AGG({metric.name}), 1.0
                    )
                ),
                "{metric.name}"
            )
        """
