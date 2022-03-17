from setuptools import setup


def text_from_file(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


test_dependencies = [
    "coverage",
    "isort",
    "jsonschema",
    "pytest",
    "pytest-black",
    "pytest-cov",
    "pytest-flake8",
    "mypy",
    "types-futures",
    "types-pkg-resources",
    "types-protobuf",
    "types-pytz",
    "types-PyYAML",
    "types-requests",
    "types-six",
    "types-toml",
]

extras = {
    "testing": test_dependencies,
}


setup(
    name="mozilla-opmon",
    author="Mozilla Corporation",
    author_email="fx-data-dev@mozilla.org",
    description="Continuous monitoring of experiments and rollouts",
    url="https://github.com/mozilla/opmon",
    packages=[
        "opmon",
        "opmon.logging",
        "opmon.templates",
        "opmon.tests",
        "opmon.tests.integration",
    ],
    package_data={
        "opmon.templates": ["*.sql"],
        "opmon.tests": ["data/*"],
        "opmon": ["../*.toml"],
    },
    install_requires=[
        "attrs",
        "cattrs",
        "Click",
        "GitPython",
        "google-cloud-bigquery",
        "grpcio",  # https://github.com/googleapis/google-cloud-python/issues/6259
        "jinja2",
        "pytz",
        "requests",
        "toml",
    ],
    include_package_data=True,
    tests_require=test_dependencies,
    extras_require=extras,
    long_description=text_from_file("README.md"),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    entry_points="""
        [console_scripts]
        opmon=opmon.cli:cli
    """,
    # This project does not issue releases, so this number is not meaningful
    # and should not need to change.
    version="2022.3.0",
)
