from setuptools import setup, find_packages

# Include requirements.txt from lambdas
with open("src/lambda_processing/requirements.txt", "r") as f:
    requirements_txt = f.read().splitlines()

setup(
    name="data-asset-uploader",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    extras_require={
        "dev": [
            "pytest",
            "ruff",
            "pytest-timeout>=2.3.1",
            "pytest-freezegun>=0.4.2",
            "faker>=30.1.0",
        ],
    },
    install_requires=[
        "awscli>=1.34.29",
        "localstack>=3.7.2",
        "awscli-local>=0.22.0",
        "aws-sam-cli>=1.125.0",
        *requirements_txt,
    ],
)
