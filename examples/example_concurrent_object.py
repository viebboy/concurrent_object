"""
example_concurrent_object.py: minimum example to demonstrate the usage of ConcurrentObject
------------------------------------------------------------------------------------------


* Copyright: 2022 Dat Tran
* Authors: Dat Tran (viebboy@gmail.com)
* Date: 2022-03-17
* Version: 0.0.1


This is part of the python_concurrency project
This example demonstrates how to use ConcurrentObject
The setup is the following: we have 2 objects: MyObject1 and MyObject2
We want to get a random matrix using MyObject1.get_array(shape) and then use
MyObject2.sum(array) to compute the sum of all elements in the random matrix

MyObject1.get_array() does not have to wait for the result of MyObject2.sum() to start
generating new array. That is, they can run in an async manner.



License
-------
Apache 2.0

"""

from concurrent_object import ConcurrentObject
import numpy as np
import time
from loguru import logger
import string
import random


class MyObject1:
    """
    an example object
    """
    def __init__(self, sleep_time):
        self.sleep_time = sleep_time

    def get_array(self, shape):
        """
        this method creates a random numpy array of given shape
        """
        # sleep to simulate a long computation
        time.sleep(self.sleep_time)
        # then create random numpy array and return
        return np.random.rand(*shape)

class MyObject2:
    """
    an example object
    """
    def __init__(self, sleep_time):
        self.sleep_time = sleep_time
        self.counter = 0

    def sum(self, array):
        """
        this method computes the sum of all elements in the given numpy array
        """
        # sleep to simulate a long computation
        time.sleep(self.sleep_time)
        # create random string of given length and return
        value = np.sum(array.flatten())
        logger.info(f'child process: sum at iteration: {self.counter} is {value}')
        self.counter += 1
        return value


if __name__ == '__main__':
    # create object 1
    obj1 = ConcurrentObject(
        name='object1',
        max_input_size='1 MB',
        max_output_size='1 MB',
        object_constructor=MyObject1,
        sleep_time=1,
    )
    # call start to initialize the process while we continue to create object 2
    obj1.start()

    # create object 2
    obj2 = ConcurrentObject(
        name='object2',
        max_input_size='1 MB',
        max_output_size='1 MB',
        object_constructor=MyObject2,
        sleep_time=1.2,
    )
    # call start to initialize the process
    obj2.start()

    # now wait until both objects are ready for execution
    obj1.wait_until_ready()
    obj2.wait_until_ready()

    count = 0
    array = None
    try:
        start_time = time.time()
        while True:

            # check if obj1 is ready to push and the shape is not None
            if obj1.status()[0]:
                # create random shape
                shape = (random.randint(1, 100), random.randint(1, 100))
                # execute obj1.get_array(), this returns almost immediately
                obj1.push('get_array', shape)

            # check if obj1 has any result to pull
            if obj1.status()[1]:
                # pull the result
                array = obj1.pull()

            # if array is not None and obj2 can push, compute sum
            if array is not None and obj2.status()[0]:
                obj2.push('sum', array)
                array = None

            # check if obj2 has any result to pull
            if obj2.status()[1]:
                # pull the result
                result = obj2.pull()
                # print the result
                logger.info(f'main process: sum at iteration {count} is: {result}')
                count += 1
                if count == 10:
                    break
        stop_time = time.time()
        print(f'took: {stop_time - start_time} seconds running concurrently')


    except Exception as error:
        # need to call .terminate() to clean up if error happens
        obj1.terminate()
        obj2.terminate()
        raise error
    finally:
        # need to call .terminate() to clean up when we are done
        obj1.terminate()
        obj2.terminate()

    # run in sequential mode
    obj1 = MyObject1(1)
    obj2 = MyObject2(1.2)

    start_time = time.time()
    for iteration in range(10):
        shape = (random.randint(1, 100), random.randint(1, 100))
        array = obj1.get_array(shape)
        total = obj2.sum(array)
        print(f'sum at iteration {iteration} is: {total}')
    stop_time = time.time()
    print(f'took: {stop_time - start_time} seconds running sequentially')
