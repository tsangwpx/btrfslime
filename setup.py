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
            'cffi~=1.14.3',
            'sqlalchemy~=1.3.20',
            'tqdm ==4.50.2',
        ],
        setup_requires=['cffi~=1.14.3'],
        cffi_modules=['btrfslime_cffi/build_fs.py:ffibuilder'],
    )
