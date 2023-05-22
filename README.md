# ConcurrentObject

This is a wrapper that allows you to wrap an object and execute its method in a child process without blocking, i.e., concurrently.  

Using this wrapper, you don't need to spend any effort in writing multiprocessing or async code. 

Arguments and results are shared via SharedMemory so efficiency is guaranteed.


## Installation

Install the dependencies in `requirements.txt` and then run `pip3 install -e .` to install the package in development mode

## Usage

In a single thread/process application, the logic is executed sequentially like this

```python

# create my object
my_obj = MyObject(*args, **kwargs)

# execute a method and wait for the result
result1 = my_obj.method1(*method1_args, **method1_kwargs)

# execute the next logic...
```

If there are other logics that are not dependent on the result of `MyObject.method1()`, using `ConcurrentObject` we could do the following:

```python


from concurrent_object import ConcurrentObject

# create my object as a ConcurrentObject
my_obj = ConcurrentObject(
    name='name_for_this_object',
    max_input_size='1 MB',
    max_output_size='1 MB',
    object_constructor=MyObject,
    *args,
    **kwargs,
)

# start the child process behind
my_obj.start()

# perform other initialization ...

# wait until my_obj is ready
my_obj.wait_until_ready()

# request the execution of method1(), which will be done in a child process without blocking the main process
# push() should return quickly
my_obj.push('method1', *method1_args, **method1_kwargs)

# execute other logics while waiting for method1() to complete ....
...
...


# we can check the status of execution with status(), which returns immediately without blocking
can_push, can_pull = my_obj.status()

# if can pull, then pull the result of method1()
if can_pull:
    result1 = my_obj.pull()

# when the object is no longer needed, we need to call terminate() to clean up properly
my_obj.terminate()

```

For a complete example, please checkout [minimal_working_example](./examples/example_concurrent_object.py)


## Detailed Interface


### `ConcurrentObject` constructor 

```python
ConcurrentObject(
    self,
    name: str,
    max_input_size: Union[int, str],
    max_output_size: Union[int, str],
    object_constructor: Callable,
    *object_args: List,
    **object_kwargs: Dict,
)
```


**Parameters**:

- **name**: *str*  
  The name of this object.  
  This is especially useful when parsing the logs coming from many ConcurrentObject instances  
- **max_input_size**: *[int, str]*   
  The maximum size of input arguments given to a method of the object.  
  If given as integer, the unit is byte  
  You can also specify in KB or MB. For example '1 MB'  
  If inputs to the object's methods are not large arrays, putting a sufficiently high number like '10 MB' is usually enough  
- **max_output_size**: *[int, str]*   
  The maximum size of the output returned by a method of the object.   
  If given as integer, the unit is byte  
  You can also specify in KB or MB. For example '1 MB'  
  If object's methods don't return large arrays, putting a sufficiently high number like '10 MB' is usually enough  
- **object_constructor**: *Callable*   
  The object class  
  keyword arguments used to construct the object  
- __\*object_args__: (list)   
  Variable-length arguments used to construct the object  
- __\**object_kwargs__: (dict)   
  Keyword based arguments used to construct the object  


### `ConcurrentObject.start()`   

```python
ConcurrentObject.start(self)
```

Start the child process behind this ConcurrentObject.  
After construction, this method needs to be called before any other methods  


### `ConcurrentObject.wait_until_ready()`   

```python
ConcurrentObject.wait_until_ready(self)
```

Wait until this object is ready.  

### `ConcurrentObject.terminate()`   

```python
ConcurrentObject.terminate(self)
```

Clean up and terminate the child process.  
This should be called when the object is no longer needed.  


### `ConcurrentObject.status()`   

```python
ConcurrentObject.status(self)
```


**Returns**:

- **can_push**: (bool) if true, the object is ready to execute a method  
- **can_pull**: (bool) if true, the result from the current method execution is ready.  


This method is used to check the state of the ConcurrentObject, whether it can start executing a method or the result from the previous execution is ready.  


### `ConcurrentObject.push()`   

```python
ConcurrentObject.push(self, method_name: str, *method_args: list, **method_kwargs: dict)
```

**Parameters**:

- **method_name**: (str)  
  The name of the method to be executed  
- __\*method_args__: (list)   
  Variable length arguments for this method  
- __\**method_kwargs__: (dict)   
  Keyword based arguments for this method  



**Returns**:

Return True if push() is successful


### `ConcurrentObject.pull()`   

```python
ConcurrentObject.pull(self)
```

**Returns**:

Return the result of the method requested previously.  
If `can_pull` is False, calling `pull()` will return None  

**Note**: Even when a method doesn't return anything, calling `pull()` is still needed to flush the result queue


## Notes on Efficiency

To avoid unnecesary overheads, one should minimize the amount of input data passed to a method.  
This is because `method1_args` and `method1_kwargs` are serialized and shared to the child process.   



## Authors
Dat Tran (viebboy@gmail.com)
