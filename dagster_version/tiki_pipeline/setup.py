from setuptools import find_packages, setup

setup(
    name="tiki_pipeline",
    packages=find_packages(exclude=["tiki_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
