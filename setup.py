from setuptools import setup

setup(name='awscostusageprocessor',
      version='0.1',
      description='AWS Cost and Usage analysis tools',
      url='https://github.com/concurrencylabs/aws-cost-analysis',
      author='Concurrency Labs',
      author_email='github@concurrencylabs.com',
      license='GNU',
      packages=['awscostusageprocessor','awscostusageprocessor.sql'],
      include_package_data=True,
      zip_safe=False)

