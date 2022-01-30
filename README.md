# qsub.py: submits a PBS array job using Python array data


## Usage

Suppose one has a Python program called `run.py` which takes positional arguments.
To call `run.py` multiple times in parallel with different input arguments, e.g.
```python
python run.py 1 2
python run.py 3 4 5
python run.py ...
```
this may be achieved by running the following in Python:
```python
from qsub import qsub

qsub('python run.py', [(1, 2), (3, 4, 5), ...])

```
