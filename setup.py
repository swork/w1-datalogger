from setuptools import setup, find_packages
setup(
    name="w1-datalogger",
    version="0.6.0",
    packages=find_packages(),
    author="Steve Work",
    author_email="steve@work.renlabs.com",
    description="Log data from local w1 busses to a cloud endpoint",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: MIT License"
    ],
    zip_safe=True,
    install_requires = [
        "requests>=2,<3"
    ],
    entry_points = {
        "console_scripts": [
            "w1logger = w1datalogger.logger:main",
        ]
    }
)
