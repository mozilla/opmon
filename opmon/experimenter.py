"""Interface to Experimenter."""

import datetime as dt
import logging
from typing import List, Optional

import attr
import cattr
import pytz
import requests
from metric_config_parser.experiment import Channel

from .utils import retry_get

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class Variant:
    """Experiment variant."""

    is_control: bool
    slug: str
    ratio: int


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class Branch:
    """Experiment branch."""

    slug: str
    ratio: int


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class Experiment:
    """Common Experimenter experiment and rollout representation."""

    experimenter_slug: Optional[str]
    normandy_slug: Optional[str]
    name: Optional[str]
    type: str
    status: Optional[str]
    branches: List[Branch]
    start_date: Optional[dt.datetime]
    end_date: Optional[dt.datetime]
    reference_branch: Optional[str]
    app_name: str
    app_id: str
    boolean_pref: Optional[str]
    channel: Optional[Channel]
    is_rollout: bool


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class ExperimentV1:
    """Experimenter v1 experiment."""

    slug: str  # experimenter slug
    type: str
    status: str
    name: Optional[str]
    start_date: Optional[dt.datetime]
    end_date: Optional[dt.datetime]
    variants: List[Variant]
    normandy_slug: Optional[str] = None
    pref_name: Optional[str]
    firefox_channel: Optional[str]

    @staticmethod
    def _unix_millis_to_datetime(num: Optional[float]) -> Optional[dt.datetime]:
        if num is None:
            return None
        return dt.datetime.fromtimestamp(num / 1e3, pytz.utc)

    @classmethod
    def from_dict(cls, d) -> "ExperimentV1":
        """Create an experiment from a dictionary."""
        converter = cattr.Converter()
        converter.register_structure_hook(
            dt.datetime,
            lambda num, _: cls._unix_millis_to_datetime(num),
        )
        return converter.structure(d, cls)

    def to_experiment(self) -> "Experiment":
        """Convert to Experiment."""
        branches = [Branch(slug=variant.slug, ratio=variant.ratio) for variant in self.variants]
        control_slug = None

        control_slugs = [variant.slug for variant in self.variants if variant.is_control]
        if len(control_slugs) == 1:
            control_slug = control_slugs[0]

        return Experiment(
            normandy_slug=self.normandy_slug,
            experimenter_slug=self.slug,
            name=self.name,
            type=self.type,
            status=self.status,
            start_date=self.start_date,
            end_date=self.end_date,
            branches=branches,
            reference_branch=control_slug,
            app_name="firefox_desktop",
            app_id="firefox-desktop",
            boolean_pref=self.pref_name,
            channel=(
                Channel(self.firefox_channel.lower())
                if self.firefox_channel and Channel.has_value(self.firefox_channel.lower())
                else None
            ),
            is_rollout=(self.type == "rollout"),
        )


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class ExperimentV6:
    """Represents a v6 experiment from Experimenter."""

    slug: str  # Normandy slug
    branches: List[Branch]
    startDate: Optional[dt.datetime]
    endDate: Optional[dt.datetime]
    referenceBranch: Optional[str]
    userFacingName: Optional[str]
    _appName: Optional[str] = None
    _appId: Optional[str] = None
    channel: Optional[str] = None
    isRollout: Optional[bool] = None

    @property
    def appName(self) -> str:
        """Return app name where experiment was launched on."""
        return self._appName or "firefox_desktop"

    @property
    def appId(self) -> str:
        """Return app ID where experiment was launched on."""
        return self._appId or "firefox-desktop"

    @classmethod
    def from_dict(cls, d) -> "ExperimentV6":
        """Create an experiment from a dictionary."""
        converter = cattr.GenConverter()
        converter.register_structure_hook(
            dt.datetime,
            lambda num, _: dt.datetime.strptime(num, "%Y-%m-%d"),
        )
        converter.register_structure_hook(
            cls,
            cattr.gen.make_dict_structure_fn(
                cls,
                converter,
                _appName=cattr.override(rename="appName"),
                _appId=cattr.override(rename="appId"),
            ),  # type: ignore
            # Ignore type check for now as it appears to be a bug in cattrs library
            # for more info see issue: https://github.com/mozilla/jetstream/issues/995
        )
        return converter.structure(d, cls)

    def to_experiment(self) -> "Experiment":
        """Convert to Experiment."""
        return Experiment(
            normandy_slug=self.slug,
            experimenter_slug=None,
            name=self.userFacingName,
            type="v6",
            status=(
                "Live"
                if (
                    self.endDate
                    and pytz.utc.localize(self.endDate) >= pytz.utc.localize(dt.datetime.now())
                )
                or self.endDate is None
                else "Complete"
            ),
            start_date=pytz.utc.localize(self.startDate) if self.startDate else None,
            end_date=pytz.utc.localize(self.endDate) if self.endDate else None,
            branches=self.branches,
            reference_branch=self.referenceBranch,
            app_name=self.appName,
            app_id=self.appId,
            boolean_pref=None,
            channel=(
                Channel(self.channel) if self.channel and Channel.has_value(self.channel) else None
            ),
            is_rollout=self.isRollout if self.isRollout else (len(self.branches) == 1),
        )


@attr.s(auto_attribs=True)
class ExperimentCollection:
    """Collection of all the experiments from experimenter."""

    experiments: List[Experiment] = attr.Factory(list)

    MAX_RETRIES = 3
    EXPERIMENTER_API_URL_V1 = "https://experimenter.services.mozilla.com/api/v1/experiments/"

    # for nimbus experiments
    EXPERIMENTER_API_URL_V6 = "https://experimenter.services.mozilla.com/api/v6/experiments/"

    # user agent sent to the Experimenter API
    USER_AGENT = "opmon"

    @classmethod
    def from_experimenter(
        cls, session: Optional[requests.Session] = None
    ) -> "ExperimentCollection":
        """Fetch all experiments from Experimenter."""
        session = session or requests.Session()
        legacy_experiments_json = retry_get(
            session, cls.EXPERIMENTER_API_URL_V1, cls.MAX_RETRIES, cls.USER_AGENT
        )
        legacy_experiments = []

        for experiment in legacy_experiments_json:
            if experiment["type"] != "rapid":
                try:
                    legacy_experiments.append(ExperimentV1.from_dict(experiment).to_experiment())
                except Exception as e:
                    logger.exception(str(e), exc_info=e, extra={"experiment": experiment["slug"]})

        nimbus_experiments_json = retry_get(
            session, cls.EXPERIMENTER_API_URL_V6, cls.MAX_RETRIES, cls.USER_AGENT
        )
        nimbus_experiments = []

        for experiment in nimbus_experiments_json:
            try:
                nimbus_experiments.append(ExperimentV6.from_dict(experiment).to_experiment())
            except Exception as e:
                logger.exception(str(e), exc_info=e, extra={"experiment": experiment["slug"]})

        return cls(nimbus_experiments + legacy_experiments)

    def ever_launched(self) -> "ExperimentCollection":
        """Return all experiments that have ever been live."""
        cls = type(self)
        return cls(
            [
                ex
                for ex in self.experiments
                if ex.status in ("Complete", "Live") or ex.status is None
            ]
        )

    def with_slug(self, slug: str) -> Optional[Experiment]:
        """Return all experiments with a specific slug."""
        for ex in self.experiments:
            if ex.experimenter_slug == slug or ex.normandy_slug == slug:
                return ex

        return None

    def rollouts(self) -> "ExperimentCollection":
        """Return all rollouts."""
        cls = type(self)
        return cls([ex for ex in self.experiments if ex.is_rollout])
