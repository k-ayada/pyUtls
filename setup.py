from distutils.core import setup
import setuptools

setup(
    name            ='PyUtils',
    version         ='0.1',
    description     ='Collection of python utilities for AWS, pySpark etc..',
    url             ='http://github.com/k-ayada/PyUtils',
    author          ='Kiran Ayada',
    packages        =['pyHelper',
                      'pyHelper.awsUtils',
                      'pyHelper.pySparkUtils',
                     ],
   #py_modules      =['mod1', 'pkg.mod2'],
   #data_files      =[('bitmaps', ['bm/b1.gif', 'bm/b2.gif']),
   #                  ('config', ['cfg/data.cfg']),
   #                 ],
    author_email    ='kiran14n@gmail.com' ,
    license         ='No restrictions :) ',
    install_requires=['boto3',
                      'pandas',
                      's3fs',
                      'pyarrow',
                     ],
   #dependency_links=['http://github.com/user/repo/tarball/master#egg=package-1.0']
    long_description=open('README.txt').read(),
    zip_safe        = False,   #Create the folder with Code instaed of the .egg file
    include_package_data = True,
    keywords = 'python utilitis',
        classifiers = [
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: System Administrators',
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
