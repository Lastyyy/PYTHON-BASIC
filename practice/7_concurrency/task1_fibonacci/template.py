import os
import time
from random import randint
import threading
import concurrent.futures
import multiprocessing

OUTPUT_DIR = './output'
RESULT_FILE = './output/result.csv'


def fib(n: int):
    """Calculate a value in the Fibonacci sequence by ordinal number"""

    f0, f1 = 0, 1
    for _ in range(n-1):
        f0, f1 = f1, f0 + f1
    return f1


def create_file(n: int, n_fib: int):
    file = open(OUTPUT_DIR + "/" + str(n) + ".txt", 'w')
    file.write(str(n_fib))


def func1(array: list):
    st = time.time()
    with multiprocessing.Pool() as pool:
        array_fib = pool.map(fib, array)
    end = time.time()
    print(end - st)

    for file in os.listdir(OUTPUT_DIR):
        os.remove(os.path.join(OUTPUT_DIR, file))

    st = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(create_file, array, array_fib)
    end = time.time()
    print(end-st)


def func2(result_file: str):
    pass


if __name__ == '__main__':
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    func1(array=[randint(1000, 100000) for _ in range(1000)])
    func2(result_file=RESULT_FILE)
