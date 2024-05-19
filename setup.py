from setuptools import find_packages, setup
from os import path

with open("requirements.txt", "r") as f:
    install_requires = f.read().splitlines()

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="abnkommati",
    version="0.0.1",
    description="The program joins two provided CSV files using PySpark and applies some minor transformations and renaming of columns.",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/saurabh18cs/abnkommati",
    author="saurabh18cs",
    license="",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8.5",
        "Operating System :: OS Independent",
    ],
    install_requires=install_requires,
    extras_require={},
    python_requires=">=3.8.5",
)