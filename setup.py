if __name__ == '__main__':
    from setuptools import setup

    setup(
        name='btrfslime',
        version='0.0.1',
        author='Aaron',
        description='A few useful BTRFS utilities',
        packages=['btrfslime'],
        classifiers=[
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.7',
            'License :: OSI Approved :: MIT License',
            'Operating System :: POSIX :: Linux',
            'Topic :: System :: Filesystems',
            'Development Status :: 4 - Beta',
        ],
        install_requires=[
            'cffi~=1.14.4',
            'sqlalchemy~=1.3.22',
            'tqdm~=4.56.0',
        ],
        setup_requires=['cffi~=1.14.4'],
        cffi_modules=[
            'btrfslime_cffi/build_fs.py:ffibuilder',
            'btrfslime_cffi/build_xxhash.py:ffibuilder',
        ],
        entry_points={
            'console_scripts': [
                'btrfslime.defrag = btrfslime.defrag.__main__:main',
                'btrfslime.dedupe = btrfslime.dedupe.__main__:main',
            ],
        },
    )
