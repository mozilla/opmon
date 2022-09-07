import re
from abc import ABC
from typing import Any, Dict, List, Optional

import attr

from opmon import Probe
from opmon.errors import StatisticNotImplementedForTypeException


@attr.s(auto_attribs=True)
class StatisticComputation:
    point: str
    name: str
    lower: Optional[str] = None
    upper: Optional[str] = None
    parameter: Optional[str] = None


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

    def computation(self, metric: Probe) -> List[StatisticComputation]:
        if metric.type == "scalar":
            return self._scalar_computation(metric)
        elif metric.type == "histogram":
            return self._histogram_computation(metric)
        else:
            raise StatisticNotImplementedForTypeException(
                f"Statistic {self.name()} not implemented for type {metric.type} ({metric.name})"
            )

    def _scalar_computation(self, metric: Probe) -> List[StatisticComputation]:
        raise StatisticNotImplementedForTypeException(
            f"Statistic {self.name()} not implemented for type {metric.type} ({metric.name})"
        )

    def _histogram_computation(self, metric: Probe) -> List[StatisticComputation]:
        raise StatisticNotImplementedForTypeException(
            f"Statistic {self.name()} not implemented for type {metric.type} ({metric.name})"
        )

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        """Create a class instance with the specified config parameters."""
        return cls(**config_dict)  # type: ignore


class Count(Statistic):
    def _scalar_computation(self, metric: Probe):
        return [
            StatisticComputation(
                point=f"COUNT({metric.name})",
                name=self.name(),
            )
        ]


class Sum(Statistic):
    def _scalar_computation(self, metric: Probe):
        return [
            StatisticComputation(
                point=f"SUM({metric.name})",
                name=self.name(),
            )
        ]


class Mean(Statistic):
    def _scalar_computation(self, metric: Probe):
        return [
            StatisticComputation(
                point=f"AVG({metric.name})",
                name=self.name(),
            )
        ]


class Quantile(Statistic):
    number_of_quantiles: int = 100
    quantile: int = 50

    def _scalar_computation(self, metric: Probe):
        return [
            StatisticComputation(
                point=f"""
                    APPROX_QUANTILES(
                        {metric.name},
                        {self.number_of_quantiles}
                    )[OFFSET({self.quantile})]
                """,
                name=self.name(),
            )
        ]


@attr.s(auto_attribs=True)
class Percentile(Statistic):
    percentiles: List[int] = [50, 90, 99]

    def _scalar_computation(self, metric: Probe):
        return [
            StatisticComputation(
                point=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT<
                            bucket_count INT64,
                            sum INT64,
                            histogram_type INT64,
                            `range` ARRAY<INT64>,
                            VALUES
                            ARRAY<STRUCT<key FLOAT64, value FLOAT64>
                        >>(1,
                            COALESCE(
                                SAFE_CAST(
                                    SAFE_CAST(
                                        FORMAT(
                                            "%.*f",
                                            2,
                                            COALESCE(
                                                mozfun.glam.histogram_bucket_from_value(
                                                    {metric.name}_buckets,
                                                    SAFE_CAST({metric.name} AS FLOAT64)
                                            ), 0) + 0.0001
                                        )
                                    AS FLOAT64)
                                AS INT64),
                            0),
                            1,
                            [
                                0,
                                COALESCE(
                                    SAFE_CAST(
                                        SAFE_CAST(
                                            FORMAT(
                                                "%.*f",
                                                2,
                                                COALESCE(
                                                    mozfun.glam.histogram_bucket_from_value(
                                                        {metric.name}_buckets,
                                                        SAFE_CAST({metric.name} AS FLOAT64)
                                                    ), 0
                                                ) + 0.0001
                                            )
                                        AS FLOAT64)
                                    AS INT64),
                                0)
                            ],
                            [
                                STRUCT<key FLOAT64, value FLOAT64>(
                                    COALESCE(
                                        SAFE_CAST(
                                            FORMAT(
                                                "%.*f",
                                                2,
                                                COALESCE(
                                                    mozfun.glam.histogram_bucket_from_value(
                                                        {metric.name}_buckets,
                                                        SAFE_CAST({metric.name} AS FLOAT64)
                                                    ),
                                                0) + 0.0001
                                            ) AS FLOAT64
                                        ), 0.0
                                    ), 1
                                )
                            ]
                        )
                    ).percentile
                """,
                lower=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT<
                            bucket_count INT64,
                            sum INT64,
                            histogram_type INT64,
                            `range` ARRAY<INT64>,
                            VALUES
                            ARRAY<STRUCT<key FLOAT64, value FLOAT64>
                        >>(1,
                            COALESCE(
                                SAFE_CAST(
                                    SAFE_CAST(
                                        FORMAT(
                                            "%.*f",
                                            2,
                                            COALESCE(
                                                mozfun.glam.histogram_bucket_from_value(
                                                    {metric.name}_buckets,
                                                    SAFE_CAST({metric.name} AS FLOAT64)
                                            ), 0) + 0.0001
                                        )
                                    AS FLOAT64)
                                AS INT64),
                            0),
                            1,
                            [
                                0,
                                COALESCE(
                                    SAFE_CAST(
                                        SAFE_CAST(
                                            FORMAT(
                                                "%.*f",
                                                2,
                                                COALESCE(
                                                    mozfun.glam.histogram_bucket_from_value(
                                                        {metric.name}_buckets,
                                                        SAFE_CAST({metric.name} AS FLOAT64)
                                                    ), 0
                                                ) + 0.0001
                                            )
                                        AS FLOAT64)
                                    AS INT64),
                                0)
                            ],
                            [
                                STRUCT<key FLOAT64, value FLOAT64>(
                                    COALESCE(
                                        SAFE_CAST(
                                            FORMAT(
                                                "%.*f",
                                                2,
                                                COALESCE(
                                                    mozfun.glam.histogram_bucket_from_value(
                                                        {metric.name}_buckets,
                                                        SAFE_CAST({metric.name} AS FLOAT64)
                                                    ),
                                                0) + 0.0001
                                            ) AS FLOAT64
                                        ), 0.0
                                    ), 1
                                )
                            ]
                        )
                    ).low
                """,
                upper=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT<
                            bucket_count INT64,
                            sum INT64,
                            histogram_type INT64,
                            `range` ARRAY<INT64>,
                            VALUES
                            ARRAY<STRUCT<key FLOAT64, value FLOAT64>
                        >>(1,
                            COALESCE(
                                SAFE_CAST(
                                    SAFE_CAST(
                                        FORMAT(
                                            "%.*f",
                                            2,
                                            COALESCE(
                                                mozfun.glam.histogram_bucket_from_value(
                                                    {metric.name}_buckets,
                                                    SAFE_CAST({metric.name} AS FLOAT64)
                                            ), 0) + 0.0001
                                        )
                                    AS FLOAT64)
                                AS INT64),
                            0),
                            1,
                            [
                                0,
                                COALESCE(
                                    SAFE_CAST(
                                        SAFE_CAST(
                                            FORMAT(
                                                "%.*f",
                                                2,
                                                COALESCE(
                                                    mozfun.glam.histogram_bucket_from_value(
                                                        {metric.name}_buckets,
                                                        SAFE_CAST({metric.name} AS FLOAT64)
                                                    ), 0
                                                ) + 0.0001
                                            )
                                        AS FLOAT64)
                                    AS INT64),
                                0)
                            ],
                            [
                                STRUCT<key FLOAT64, value FLOAT64>(
                                    COALESCE(
                                        SAFE_CAST(
                                            FORMAT(
                                                "%.*f",
                                                2,
                                                COALESCE(
                                                    mozfun.glam.histogram_bucket_from_value(
                                                        {metric.name}_buckets,
                                                        SAFE_CAST({metric.name} AS FLOAT64)
                                                    ),
                                                0) + 0.0001
                                            ) AS FLOAT64
                                        ), 0.0
                                    ), 1
                                )
                            ]
                        )
                    ).high
                """,
                name=self.name(),
                parameter=str(percentile),
            )
            for percentile in self.percentiles
        ]

    def _histogram_computation(self, metric: Probe) -> List[StatisticComputation]:
        return [
            StatisticComputation(
                point=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT(
                            histogram_normalized_sum(
                                mozfun.hist.merge(
                                    ARRAY_AGG({metric.name} IGNORE NULLS)
                                ).values, 1.0
                            )
                        )
                    ).percentile
                """,
                lower=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT(
                            histogram_normalized_sum(
                                mozfun.hist.merge(
                                    ARRAY_AGG({metric.name} IGNORE NULLS)
                                ).values, 1.0
                            )
                        )
                    ).low
                """,
                upper=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT(
                            histogram_normalized_sum(
                                mozfun.hist.merge(
                                    ARRAY_AGG({metric.name} IGNORE NULLS)
                                ).values, 1.0
                            )
                        )
                    ).high
                """,
                name=self.name(),
                parameter=str(percentile),
            )
            for percentile in self.percentiles
        ]
