from setuptools import setup, find_packages

setup(name='MIBEL_Downloader',
      version='0.65',
      description='Downloader for MIBEL Market Prices and Load/Wind Forecasts',
      url='http://github.com/jmfilipe/Price_Forecasts___Downloader',
      author='Jorge Filipe',
      author_email='jmfilipe@inesctec.pt',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'pandas >=0.18',
          'beautifulsoup4 >= 4.4.1, < 5.0',
          'html5lib'
      ],)
