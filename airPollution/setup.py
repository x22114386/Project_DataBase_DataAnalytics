from setuptools import find_packages, setup

setup(
    name="airPollution",
    packages=find_packages(exclude=["airPollution_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
