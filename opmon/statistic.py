import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import attr


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

    @abstractmethod
    def computation(self, value: str = "values") -> List[StatisticComputation]:
        return NotImplemented

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        """Create a class instance with the specified config parameters."""
        return cls(**config_dict)  # type: ignore


class Count(Statistic):
    def computation(self, value: str = "values"):
        return [
            StatisticComputation(
                point=f"COUNT({value})",
                name=self.name(),
            )
        ]


class Sum(Statistic):
    def computation(self, value: str = "values"):
        return [
            StatisticComputation(
                point=f"SUM({value})",
                name=self.name(),
            )
        ]


class Mean(Statistic):
    def computation(self, value: str = "values"):
        return [
            StatisticComputation(
                point=f"AVG({value})",
                name=self.name(),
            )
        ]


class Quantile(Statistic):
    number_of_quantiles: int = 100
    quantile: int = 50

    def computation(self, value: str = "values"):
        return [
            StatisticComputation(
                point=f"""
                    APPROX_QUANTILES({value}, {self.number_of_quantiles})[OFFSET({self.quantile})]
                """,
                name=self.name(),
            )
        ]


@attr.s(auto_attribs=True)
class Percentile(Statistic):
    percentiles: List[int] = [50, 90, 99]

    def computation(self, value: str = "values"):
        return [
            StatisticComputation(
                point=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT(
                            {value}
                        )
                    ).percentile
                """,
                lower=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT(
                            {value}
                        )
                    ).low
                """,
                upper=f"""
                    `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                        {percentile},
                        STRUCT(
                            {value}
                        )
                    ).high
                """,
                name=self.name(),
                parameter=str(percentile),
            )
            for percentile in self.percentiles
        ]
