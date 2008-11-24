try:
    from setuptools import setup
except ImportError:
    from distutils import setup

setup(
    name="Dynomite",
    version="0.1",
    packages=['dynomite'],
    tests_require=['nose>=0.11.0.dev', 'boto']
    )
