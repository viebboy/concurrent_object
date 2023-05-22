"""
concurrent_object.py: worker implementation in a separate process
-----------------------------------------------------------------


* Copyright: 2022 Dat Tran
* Authors: Dat Tran (viebboy@gmail.com)
* Date: 2022-03-17
* Version: 0.0.1


This is part of the concurrent_object project (https://github.com/viebboy/concurrent_object)

License
-------
Apache 2.0

"""

from typing import Callable, List, Dict, Union, Any
import dill
import time
import os
import multiprocessing as MP
from multiprocessing import shared_memory as SM
from enum import Enum
from loguru import logger


CTX = MP.get_context('spawn')


class PARENT_MESSAGE(Enum):
    """
    List of messages sent from parent
    """
    TERMINATE = 1
    EXECUTE = 2

    def all():
        variables = []
        class_ = globals()[__class__.__name__]
        for x in dir(class_):
            if not x.startswith('__') and not callable(getattr(class_, x)):
                variables.append(getattr(class_, x))
        return variables

    def get(value):
        class_ = globals()[__class__.__name__]
        all_func = getattr(class_, 'all')
        for e in all_func():
            if e.value == value:
                return e


class CHILD_MESSAGE(Enum):
    """
    List of messages sent from child
    """
    TERMINATE = 1
    INPUT_READY = 2
    OUTPUT_READY = 3

    def all():
        variables = []
        class_ = globals()[__class__.__name__]
        for x in dir(class_):
            if not x.startswith('__') and not callable(getattr(class_, x)):
                variables.append(getattr(class_, x))
        return variables

    def get(value):
        class_ = globals()[__class__.__name__]
        all_func = getattr(class_, 'all')
        for e in all_func():
            if e.value == value:
                return e


def catch_exception(func):
    def func_with_exception_handler(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            # cleanup
            self._close_with_notice()
            logger.debug(f'exception in {func.__name__}: {e}')
            raise e

    return func_with_exception_handler


class ConcurrentObject(CTX.Process):
    """
    ConcurrentObject allows executing an object's methods in a separate process
    ConcurrentObject exposes the following interfaces:

    status()
        Returns 2 flags, can_push and can_pull.
        can_push: Indicates whether the worker is ready to accept execution requests.
        can_pull: Indicates whether the execution result is ready to be pulled.

    push(method_name, *method_arg, **method_kwargs)
        Requests execution of object.method_name(*method_args, **method_kwargs) in a child process.
        Returns True if execution request is sent successfully to the child, otherwise False.

    pull()
        Pulls the execution result from the child process.
        Even if the execution doesn't return anything, pull() should always be called after a push.

    __call__(method_name, *method_args, **method_kwargs)
        Requests execution and waits for the results, returning the results.
        This method is synchronous and blocks the main loop until the execution is done.

    status(), push(), and pull() methods allow asynchronous execution without blocking the main loop.
    The __call__() method is synchronous and blocks the main loop until the execution is done.

    Args:
        name (str): The name of the worker.
        max_input_size (Union[int, str]): Maximum allowed size for input data, can be an integer or a string.
            If string is provided, it must ends with ' KB' or ' MB'
        max_output_size (Union[int, str]): Maximum allowed size for output data, can be an integer or a string.
            If string is provided, it must ends with ' KB' or ' MB'
        object_constructor (Callable): A callable object representing the constructor of the class to be executed.
        *object_args (List): A list of positional arguments to be passed to the object's constructor.
        **object_kwargs (Dict): A dictionary of keyword arguments to be passed to the object's constructor.
    """

    def __init__(
        self,
        name: str,
        max_input_size: Union[int, str],
        max_output_size: Union[int, str],
        object_constructor: Callable,
        *object_args: List,
        **object_kwargs: Dict,
    ):
        super().__init__()

        self._name = name
        max_input_size, max_output_size = self._set_buffer_size(max_input_size, max_output_size)

        self._front_read_pipe, self._back_write_pipe = CTX.Pipe()
        self._back_read_pipe, self._front_write_pipe = CTX.Pipe()

        # shared memory
        self._input_shared_memory = SM.SharedMemory(
            create=True,
            size=max_input_size
        )
        self._input_memory_name = self._input_shared_memory.name

        self._output_shared_memory = SM.SharedMemory(
            create=True,
            size=max_input_size
        )
        self._output_memory_name = self._output_shared_memory.name

        # object arguments
        if object_args is None:
            object_args = ()
        elif not isinstance(object_args, (tuple, list)):
            raise ValueError('object_args must be a tuple or a list or None')

        if object_kwargs is None:
            object_kwargs = {}
        elif not isinstance(object_kwargs, dict):
            raise ValueError('object_kwargs must be a dictionary or None')

        self._object_constructor = object_constructor
        self._object_args = object_args
        self._object_kwargs = object_kwargs

        # states
        self._is_closed = False
        self._child_ready = False
        self._can_push = False
        self._can_pull = False
        self._nb_request = 0
        self._output_len = None
        self._has_started = False

    def _set_buffer_size(self, max_input_size, max_output_size):
        if isinstance(max_input_size, int):
            self._max_input_size = max_input_size
        elif isinstance(max_input_size, str):
            if max_input_size.endswith(' KB'):
                try:
                    self._max_input_size = 1000 * int(max_input_size.split(' ')[0])
                except Exception as error:
                    logger.error(f'cannot parse max_input_size with value: {max_input_size}')
                    raise error
            elif max_input_size.endswith(' MB'):
                try:
                    self._max_input_size = 1_000_000 * int(max_input_size.split(' ')[0])
                except Exception as error:
                    logger.error(f'cannot parse max_input_size with value: {max_input_size}')
                    raise error
            else:
                logger.error(f'cannot parse max_input_size with value: {max_input_size}')
                logger.error('when max_input_size is a string, it should ends with " KB" or " MB"')
                raise ValueError(f'cannot parse max_input_size with value: {max_input_size}')

        if isinstance(max_output_size, int):
            self._max_output_size = max_output_size

        elif isinstance(max_output_size, str):
            if max_output_size.endswith(' KB'):
                try:
                    self._max_output_size = 1000 * int(max_output_size.split(' ')[0])
                except Exception as error:
                    logger.error(f'cannot parse max_output_size with value: {max_output_size}')
                    raise error
            elif max_output_size.endswith(' MB'):
                try:
                    self._max_output_size = 1_000_000 * int(max_output_size.split(' ')[0])
                except Exception as error:
                    logger.error(f'cannot parse max_output_size with value: {max_output_size}')
                    raise error
            else:
                logger.error(f'cannot parse max_output_size with value: {max_output_size}')
                logger.error('when max_output_size is a string, it should ends with " KB" or " MB"')
                raise ValueError(f'cannot parse max_output_size with value: {max_output_size}')

        return self._max_input_size, self._max_output_size

    def _read_pipe(self):
        return self._front_read_pipe

    def _write_pipe(self):
        return self._front_write_pipe

    def run(self):
        """
        this runs in the child, a.k.a, the child process
        """
        logger.debug(f'{self._name}-child: start running')

        # attach to shared memory
        input_shared_memory = SM.SharedMemory(name=self._input_memory_name)
        output_shared_memory = SM.SharedMemory(name=self._output_memory_name)

        # construct object
        obj = self._object_constructor(*self._object_args, **self._object_kwargs)

        try:
            # send message to parent that the child is ready
            self._back_write_pipe.send(CHILD_MESSAGE.INPUT_READY.value)
            logger.debug(f'{self._name}-child: ready')

            while True:
                # check the pipe for message from parent
                if self._back_read_pipe.poll():
                    msg_code = self._back_read_pipe.recv()
                    message = PARENT_MESSAGE.get(msg_code)

                    # unknown message
                    if message is None:
                        raise RuntimeError(f'unknown message code: {msg_code}')

                    # terminate message
                    elif message == PARENT_MESSAGE.TERMINATE:
                        logger.debug(f'{self._name}: receive signal to terminate')
                        break

                    # request for inference
                    elif message == PARENT_MESSAGE.EXECUTE:
                        # when message is execute, we expect the parent to send
                        # name of the method and the size of data to be read from shared memory
                        method_name, data_len = self._back_read_pipe.recv()
                        method = getattr(obj, method_name)

                        if data_len > 0:
                            # data_len > 0 indicating that the requested method
                            # requires input data
                            # we get the data from buffer
                            args, kwargs = dill.loads(input_shared_memory.buf[:data_len])

                        # notify parent that shared memory is ready to be written
                        self._back_write_pipe.send(CHILD_MESSAGE.INPUT_READY.value)

                        # execute
                        if data_len > 0:
                            outputs = method(*args, **kwargs)
                        else:
                            outputs = method()

                        # serialize to bytes and write to buffer
                        if outputs is not None:
                            outputs_in_bytes = dill.dumps(outputs)
                            output_len = len(outputs_in_bytes)
                            output_shared_memory.buf[:output_len] = outputs_in_bytes
                        else:
                            output_len = 0

                        # notify parent that shared memory is ready to be read
                        self._back_write_pipe.send(CHILD_MESSAGE.OUTPUT_READY.value)
                        # send the size of output
                        self._back_write_pipe.send(output_len)
                    else:
                        raise NotImplemented
                else:
                    time.sleep(0.001)

            # clean up
            self._back_write_pipe.close()

            # close the shared memory access
            input_shared_memory.close()
            output_shared_memory.close()

        # catch all exception and notify parent
        except Exception as error:
            # notify parent that error happened
            self._back_write_pipe.send(CHILD_MESSAGE.TERMINATE.value)

            # send error message
            self._back_write_pipe.send(str(error))
            self._back_write_pipe.close()

            # close the shared memory access
            input_shared_memory.close()
            output_shared_memory.close()

            # then raise again
            raise error

    @catch_exception
    def __call__(self, method_name, *method_args, **method_kwargs):
        """
        request execution and wait for result
        """
        logger.debug(f'{self._name}: request execution and wait for result')

        while True:
            can_push, can_pull = self.status()
            if can_push:
                break
            else:
                time.sleep(0.001)

        self.push(method_name, *method_args, **method_kwargs)

        # wait for result
        while True:
            _, can_pull = self.status()
            if can_pull:
                break
            else:
                time.sleep(0.001)

        return self.pull()

    @catch_exception
    def status(self):
        """
        check the status and return whether it is ready to push a new sample for inference
        or the result is ready for previous submission
        """

        if self._read_pipe().poll():
            # has message from child
            msg_code = self._read_pipe().recv()
            # decode the message
            message = CHILD_MESSAGE.get(msg_code)

            # cannot decode
            if message is None:
                raise RuntimeError(
                    f'{self._name}: unknown message code from child: {msg_code}'
                )

            # child is ready to receive new input
            elif message == CHILD_MESSAGE.INPUT_READY:
                logger.debug(f'{self._name}: receive input ready message')
                self._can_push = True
                if not self._child_ready:
                    self._child_ready = True

            # output is ready
            elif message == CHILD_MESSAGE.OUTPUT_READY:
                logger.debug(f'{self._name}: receive output ready message')
                self._can_pull = True
                # get the number of bytes to read
                self._output_len = self._read_pipe().recv()
                logger.debug(f'{self._name}: output size: {self._output_len}')

            # child terminated
            elif message == CHILD_MESSAGE.TERMINATE:
                error = self._read_pipe().recv()
                logger.warning(
                    f'{self._name}: child terminated with error: {error}'
                )
                # clean up without sending signal to close the child
                # because child is already closed
                self._close_without_notice()
                # then raise the error
                raise RuntimeError(error)

            # known message but handling not implemented
            else:
                raise NotImplemented

        return self._can_push, self._can_pull

    @catch_exception
    def pull(self):
        """
        pull the result of the last push
        return None if nothing to pull
        """

        if self._can_pull and self._output_len is not None:
            # reconstruct
            if self._output_len > 0:
                outputs = dill.loads(self._output_shared_memory.buf[:self._output_len])
            else:
                outputs = None

            # reduce the number of request
            self._nb_request -= 1
            self._output_len = None
            self._can_pull = False
            return outputs
        else:
            logger.warning(
                '{self._name}: pull() blocking. Returning None, which is not the result of the last push()'
            )

    @catch_exception
    def push(self, method_name: str, *method_args, **method_kwargs):
        """
        """

        if not self._child_ready:
            logger.debug(f'{self._name}: child is NOT ready')
            self._wait_for_child()
            logger.debug(f'{self._name}: child is NOW ready')

        if self._can_push:
            if method_args is None:
                method_args = ()
            if method_kwargs is None:
                method_kwargs = {}

            if method_args != () or method_kwargs != {}:
                inputs_in_bytes = dill.dumps((method_args, method_kwargs))
                input_len = len(inputs_in_bytes)

                # put the data into shared memory
                if len(inputs_in_bytes) > self._input_shared_memory.size:
                    logger.error(f'{self._name}: input size exceeds max_input_size')
                    logger.error(f'{self._name}: input size: {input_len}')
                    logger.error(f'{self._name}: max input size: {self._input_shared_memory.size}')
                    raise RuntimeError(f'{self._name}: input size exceeds max_input_size')
                else:
                    self._input_shared_memory.buf[:input_len] = inputs_in_bytes[:]
            else:
                input_len = 0

            # send the message to child
            logger.debug(f'{self._name}: sending execution request to child')
            self._write_pipe().send(PARENT_MESSAGE.EXECUTE.value)

            # send the method name and size of data to child
            logger.debug(f'{self._name}: sending method name and input size to child')
            self._write_pipe().send((method_name, input_len))

            self._can_push = False
            self._nb_request += 1
            return True
        else:
            return False

    @catch_exception
    def _wait_for_child(self):
        """
        check and wait until the child is ready
        """
        if self._child_ready:
            return

        while True:
            if self._read_pipe().poll():
                msg_code = self._read_pipe().recv()
                message = CHILD_MESSAGE.get(msg_code)

                # unknown message
                if message is None:
                    raise RuntimeError(
                        f'{self._name}: unknown message code from child: {msg_code}'
                    )

                # ready message
                elif message == CHILD_MESSAGE.INPUT_READY:
                    self._child_ready = True
                    self._can_push = True
                    break
                elif message == CHILD_MESSAGE.TERMINATE:
                    error_msg = self._read_pipe().recv()
                    print(f'error msg: {error_msg}')
                    logger.warning(f'{self._name} child terminated with error: {error_msg}')
                    raise RuntimeError(
                        f'{self._name}: child terminated with error: {error_msg}'
                    )
                else:
                    raise RuntimeError(
                        f'{self._name}: first message from child should be INPUT_READY or TERMINATE'
                    )
        logger.debug(f'{self._name}: child is ready')

    def terminate(self):
        self._close_with_notice()
        time.sleep(0.01)
        return super().terminate()

    def close(self):
        self._close_with_notice()

    def _close_with_notice(self):
        if not self._is_closed:
            logger.debug(f'{self._name}: close with notice')
            # need to send noti to the subprocess
            self._write_pipe().send(PARENT_MESSAGE.TERMINATE.value)
            self._write_pipe().close()
            self._is_closed = True

            time.sleep(0.1)

            # perform clean up here
            self._input_shared_memory.close()
            self._input_shared_memory.unlink()
            self._output_shared_memory.close()
            self._output_shared_memory.unlink()

            # need try catch here because process might not be started
            try:
                self.join()
            except Exception:
                pass

    def _close_without_notice(self):
        """
        close without telling the child to terminate
        """

        if not self._is_closed:
            logger.debug(f'{self._name}: close with notice')
            # perform clean up here
            self._input_shared_memory.close()
            self._input_shared_memory.unlink()
            self._output_shared_memory.close()
            self._output_shared_memory.unlink()

            # note that we only close the write end of the pipe
            # child will handle the other end
            self._write_pipe().close()
            self._is_closed = True

            # need try & catch here because process might not be started
            try:
                self.join()
            except Exception:
                pass

    @catch_exception
    def wait_until_ready(self):
        """
        wait until the child is ready
        """
        self.start()
        self._wait_for_child()

    def start(self):
        """
        start the child
        """
        if not self._has_started:
            super().start()
            self._has_started = True
