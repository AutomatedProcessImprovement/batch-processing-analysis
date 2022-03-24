from setuptools import setup

setup(
    name='batch_processing_analysis',
    version='1.0.3',
    package_dir={"": "src/batch_processing_analysis"},
    include_package_data=True,
    install_requires=[
        'numpy',
        'pandas',
        'wittgenstein'
    ],
    entry_points={
        'console_scripts': [
            'batch_processing_analysis = main:main',
        ]
    }
)
