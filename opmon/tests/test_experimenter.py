import datetime as dt
import json
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
import pytz

from opmon.experimenter import (
    Branch,
    Experiment,
    ExperimentCollection,
    ExperimentV8,
)

EXPERIMENTER_FIXTURE_V8 = r"""
[
{
  "schemaVersion": "1",
  "application": "firefox-desktop",
  "id":"bug-1629000-rapid-testing-rapido-intake-1-release-79",
  "slug":"bug-1629098-rapid-please-reject-me-beta-86",
  "userFacingName":"",
  "userFacingDescription":" This is an empty CFR A/A experiment. The A/A experiment is being run to test the automation, effectiveness, and accuracy of the rapid experiments platform.\n    The experiment is an internal test, and Firefox users will not see any noticeable change and there will be no user impact.",
  "isEnrollmentPaused":false,
  "metricSets":[],
  "proposedEnrollment":7,
  "bucketConfig": {
    "randomizationUnit":"userId",
    "namespace":"bug-1629098-rapid-please-reject-me-beta-86",
    "start":0,
    "count":100,
    "total":10000 
  },
  "startDate":"2020-07-29",
  "endDate":null,
  "branches":[{
      "slug":"treatment",
      "ratio":1,
      "feature": {"featureId": "foo", "enabled": false, "value": null}    
    },
    {
      "slug":"control",
      "ratio":1,
      "feature": {"featureId": "foo", "enabled": false, "value": null}    
    }
  ],
  "referenceBranch":"control",
  "filter_expression":"env.version|versionCompare('86.0') >= 0",
  "targeting":"[userId, \"bug-1629098-rapid-please-reject-me-beta-86\"]|bucketSample(0, 100, 10000) && localeLanguageCode == 'en' && region == 'US' && browserSettings.update.channel == 'beta'"
},
{
  "schemaVersion": "1",
  "application": "firefox-desktop",   
  "id":"bug-1629000-rapid-testing-rapido-intake-1-release-79",
    "slug":"bug-1629000-rapid-testing-rapido-intake-1-release-79",
    "userFacingName":"testing rapido intake 1",
    "userFacingDescription":" This is an empty CFR A/A experiment. The A/A experiment is being run to test the automation, effectiveness, and accuracy of the rapid experiments platform.\n    The experiment is an internal test, and Firefox users will not see any noticeable change and there will be no user impact.",
    "isEnrollmentPaused":false,
    "metricSets":[
      "fake_feature"
    ],
    "proposedEnrollment":14,
    "proposedDuration":30,
    "bucketConfig":{
      "randomizationUnit":"normandy_id",
      "namespace":"",
      "start":0,
      "count":0,
      "total":10000
    },
    "startDate":"2020-07-28",
    "endDate":null,
    "branches":[{
      "slug":"treatment",
      "ratio":1,
      "feature": {"featureId": "foo", "enabled": false, "value": null}     
      },
      {
        "slug":"control",
        "ratio":1,
        "feature": {"featureId": "foo", "enabled": false, "value": null}   
    }],
  "referenceBranch":"control",
  "filter_expression":"env.version|versionCompare('79.0') >= 0",
  "targeting":""
},
{   
  "id":null,
    "slug":null,
    "userFacingName":"some invalid experiment",
    "userFacingDescription":" This is an empty CFR A/A experiment. The A/A experiment is being run to test the automation, effectiveness, and accuracy of the rapid experiments platform.\n    The experiment is an internal test, and Firefox users will not see any noticeable change and there will be no user impact.",
    "isEnrollmentPaused":false,
    "proposedEnrollment":14,
    "bucketConfig":{
      "randomizationUnit":"normandy_id",
      "namespace":"",
      "start":0,
      "count":0,
      "total":10000
    },
    "startDate":null,
    "endDate":null,
    "branches":[],
  "referenceBranch":"control",
  "enabled":true,
  "targeting":null
}
]
"""

FENIX_EXPERIMENT_FIXTURE = """
{
  "schemaVersion": "1.4.0",
  "slug": "fenix-bookmark-list-icon",
  "id": "fenix-bookmark-list-icon",
  "arguments": {},
  "application": "org.mozilla.fenix",
  "appName": "fenix",
  "appId": "org.mozilla.fenix",
  "channel": "nightly",
  "userFacingName": "Fenix Bookmark List Icon",
  "userFacingDescription": "If we make the save-bookmark and access-bookmarks icons more visually distinct,  users are more likely to know what icon to click to save their bookmarks. By changing the access-bookmarks icon, we believe that we will and can see an increase in engagement with the save to bookmarks icon.",
  "isEnrollmentPaused": true,
  "bucketConfig": {
    "randomizationUnit": "nimbus_id",
    "namespace": "fenix-bookmark-list-icon-1",
    "start": 0,
    "count": 10000,
    "total": 10000
  },
  "metricSets": [],
  "outcomes": [{
    "slug": "default-browser",
    "priority": "primary"
  }],
  "branches": [
    {
      "slug": "control",
      "ratio": 1
    },
    {
      "slug": "treatment",
      "ratio": 1
    }
  ],
  "targeting": "true",
  "startDate": "2021-02-09",
  "endDate": "2021-03-11",
  "proposedDuration": 28,
  "proposedEnrollment": 7,
  "referenceBranch": "control",
  "featureIds": []
}
"""


@pytest.fixture
def mock_session():
    def experimenter_fixtures(url):
        mocked_value = MagicMock()
        if url == ExperimentCollection.EXPERIMENTER_API_URL_V8:
            mocked_value.json.return_value = json.loads(EXPERIMENTER_FIXTURE_V8)
        else:
            raise Exception("Invalid Experimenter API call.")

        return mocked_value

    session = MagicMock()
    session.get = MagicMock(side_effect=experimenter_fixtures)
    return session


@pytest.fixture
def experiment_collection(mock_session):
    return ExperimentCollection.from_experimenter(mock_session)


def test_from_experimenter(mock_session):
    collection = ExperimentCollection.from_experimenter(mock_session)
    mock_session.get.assert_any_call(ExperimentCollection.EXPERIMENTER_API_URL_V8)
    assert len(collection.experiments) == 3
    assert isinstance(collection.experiments[0], Experiment)
    assert isinstance(collection.experiments[0].branches[0], Branch)
    assert len(collection.experiments[0].branches) == 2
    assert collection.experiments[0].start_date > dt.datetime(2019, 1, 1, tzinfo=pytz.utc)
    assert len(collection.experiments[1].branches) == 2


def test_normandy_experiment_slug(experiment_collection):
    normandy_slugs = list(map(lambda e: e.normandy_slug, experiment_collection.experiments))
    assert "bug-1629098-rapid-please-reject-me-beta-86" in normandy_slugs
    assert "None" in normandy_slugs
    assert "bug-1629000-rapid-testing-rapido-intake-1-release-79" in normandy_slugs


def test_with_slug(experiment_collection):
    experiment = experiment_collection.with_slug(
        "bug-1629000-rapid-testing-rapido-intake-1-release-79"
    )
    assert experiment.experimenter_slug is None
    assert experiment.normandy_slug == "bug-1629000-rapid-testing-rapido-intake-1-release-79"

    experiment = experiment_collection.with_slug("bug-1629098-rapid-please-reject-me-beta-86")
    assert experiment.experimenter_slug is None
    assert experiment.normandy_slug == "bug-1629098-rapid-please-reject-me-beta-86"

    experiment = experiment_collection.with_slug("non-existing-slug")
    assert experiment is None


def test_convert_experiment_v8_to_experiment():
    experiment_v8 = ExperimentV8(
        slug="test_slug",
        userFacingName="Test",
        startDate=dt.datetime(2019, 1, 1),
        endDate=dt.datetime(2019, 1, 10),
        branches=[Branch(slug="control", ratio=2), Branch(slug="treatment", ratio=1)],
        referenceBranch="control",
    )

    experiment = experiment_v8.to_experiment()

    assert experiment.experimenter_slug is None
    assert experiment.normandy_slug == "test_slug"
    assert experiment.status == "Complete"
    assert experiment.type == "v6"
    assert len(experiment.branches) == 2
    assert experiment.reference_branch == "control"
    assert experiment.boolean_pref is None
    assert experiment.channel is None


def test_experiment_v8_status():
    experiment_live = ExperimentV8(
        slug="test_slug",
        startDate=dt.datetime(2019, 1, 1),
        userFacingName="Test",
        endDate=dt.datetime.now() + timedelta(days=1),
        branches=[Branch(slug="control", ratio=2), Branch(slug="treatment", ratio=1)],
        referenceBranch="control",
        channel=None,
    )

    assert experiment_live.to_experiment().status == "Live"

    experiment_complete = ExperimentV8(
        slug="test_slug",
        startDate=dt.datetime(2019, 1, 1),
        userFacingName="Test",
        endDate=dt.datetime.now() - timedelta(minutes=1),
        branches=[Branch(slug="control", ratio=2), Branch(slug="treatment", ratio=1)],
        referenceBranch="control",
        channel="release",
    )

    assert experiment_complete.to_experiment().status == "Complete"


def test_app_name():
    x = ExperimentV8.from_dict(json.loads(FENIX_EXPERIMENT_FIXTURE))
    assert x.appName == "fenix"
    assert x.appId == "org.mozilla.fenix"
