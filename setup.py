from setuptools import find_packages, setup

setup(
    name="ucrolehelper",
    description="",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/databricks/uc-rolehelper",
    author="Douglas Moore",
    author_email="douglas.moore@databricks.com",
    license="Apache 2",
    packages=find_packages(include=["databricks-sdk", "dbx.ucrolehelper", "dbx.ucrolehelper.*"]),
    package_data={"ucrolehelper": ["py.typed"]},
    use_scm_version={
        "write_to": "dbx/ucrolehelper/version.py",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    setup_requires=["setuptools_scm"],
    install_requires=["databricks-sdk>=0.11.0"],
    extras_require={
        "dev": [
            "autoflake",
            "black",
            "isort",
            "mypy>=0.990",
            "pdoc",
            "pre-commit",
        ],
    },
    classifiers=[
        "Development Status :: 1 - x/y",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
