# cf. https://nbconvert.readthedocs.io/en/latest/execute_api.html
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


def main(argv, cwd):
    [notebook_filename, workspace, out_path] = argv[1:4]
    with (cwd / notebook_filename).open() as f:
        nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600)  # , kernel_name='python3'
        ep.preprocess(nb, {'metadata': {'path': str((cwd / workspace).resolve())}})
    with (cwd / out_path).open('w', encoding='utf-8') as f:
        nbformat.write(nb, f)


if __name__ == '__main__':
    def _script_io():
        from pathlib import Path
        from sys import argv

        main(argv[:], Path('.'))

    _script_io()
