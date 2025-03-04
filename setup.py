from setuptools import setup, find_packages


def parse_requirements(filename: str) -> list[str]:
    with open(filename, "r") as file:
        return [line.strip() for line in file if line.strip() and not line.startswith("#")]


setup(
    name="fhir_query",
    version="0.1.1",
    packages=find_packages(),
    install_requires=parse_requirements("requirements.txt"),
    extras_require={
        'dtale': ['dtale'],
    },
    entry_points={
        "console_scripts": [
            "fq=fhir_query:cli.cli",
        ],
    },
)
