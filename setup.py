from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.md') as change_file:
    changelog = change_file.read()

with open('requirements.txt') as req_file:
    requirements = req_file.read()

long_description = '\n\n'.join((readme, changelog))

test_requirements = ['pytest>=3', ]

setup(
    author="Jan Ulffers, Leon Thurner, Jannis Kupka, Mike Vogt, Joschka Thurner, Alexander Scheidler",
    author_email='info@pandapower.de',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    description="Data hub for pandapower and pandapipes networks based on MongoDB",
    install_requires=requirements,
    long_description=readme,
    entry_points = {
    'console_scripts': ['pandahub-login=pandahub.client.user_management:login'],
    },
    keywords='pandahub',
    name='pandahub',
    packages=find_packages(),
    url='https://github.com/e2nIEE/pandahub',
    version='0.2.10',
    include_package_data=True,
    long_description_content_type='text/markdown',
    zip_safe=False,
)
