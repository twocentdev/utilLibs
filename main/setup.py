from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'Util submodules and functions.'
LONG_DESCRIPTION = 'This package contains some submodules and functions that may be usefull for different projects.'


setup(
        name="utilLib", 
        version=VERSION,
        author="@twocentdev",
        author_email="<contact@twocentdev.com>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[],
        keywords=['python', 'util'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)