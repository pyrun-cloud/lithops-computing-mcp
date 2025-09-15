from fastmcp import FastMCP
import lithops
from typing import Optional


mcp = FastMCP("lithops-compute")
fexec = None
future = []


@mcp.tool()
def lithops_function_executor(args: dict):
    """
    Name --> lithops_function_executor

    Description --> Executor abstract class that contains the common logic for the Localhost, Serverless and Standalone executors

    Parameters -->

        mode (str | None) - Execution mode. One of: localhost, serverless or standalone
        config (Dict[str, Any] | None) - Settings passed in here will override those in lithops_config
        config_file (str | None) - Path to the lithops config file
        backend (str | None) - Compute backend to run the functions
        storage (str | None) - Storage backend to store Lithops data
        monitoring (str | None) - Monitoring system implementation. One of: storage, rabbitmq
        log_level (str | None) - Log level printing (INFO, DEBUG, …). Set it to None to hide all logs. If this is param is set, all logging params in config are disabled
        kwargs (Dict[str, Any] | None) - Any parameter that can be set in the compute backend section of the config file, can be set here

    Return :
        NOTHING
    """
    global fexec
    fexec = lithops.FunctionExecutor(**args)


@mcp.tool()
def lithops_localhost_executor(args: dict):
    """
    Name --> lithops_localhost_executor
    Description --> Initialize a LocalhostExecutor class. / Bases : FunctionExecutor
    Parameters -->
        config (Dict[str, Any] | None) - Settings passed in here will override those in config file.
        config_file (str | None) - Path to the lithops config file
        storage (str | None) - Name of the storage backend to use.
        monitoring (str | None) - monitoring system.
        log_level (str | None) - log level to use during the execution.
        kwargs (Dict[str, Any] | None) - Any parameter that can be set in the compute backend section of the config file, can be set here
    """
    global fexec
    fexec = lithops.LocalhostExecutor(**args)


@mcp.tool()
def lithops_severless_executor(args: dict):
    """
    Name --> lithops_severless_executor
    Description --> Initialize a ServerlessExecutor class. / Bases : FunctionExecutor
    Parameters -->
        config (Dict[str, Any] | None) - Settings passed in here will override those in config file
        config_file (str | None) - Path to the lithops config file
        backend (str | None) - Name of the serverless compute backend to use
        storage (str | None) - Name of the storage backend to use
        monitoring (str | None) - monitoring system
        log_level (str | None) - log level to use during the execution
        kwargs (Dict[str, Any] | None) - Any parameter that can be set in the compute backend section of the config file, can be set here
    """
    global fexec
    fexec = lithops.ServerlessExecutor(**args)


@mcp.tool()
def lithops_standalone_executor(args: dict):
    """
    Name --> lithops_standalone_executor
    Description --> Initialize a StandaloneExecutor class. / Bases : FunctionExecutor
    Parameters -->
        config (Dict[str, Any] | None) - Settings passed in here will override those in config file

        config_file (str | None) - Path to the lithops config file

        backend (str | None) - Name of the standalone compute backend to use

        storage (str | None) - Name of the storage backend to use

        monitoring (str | None) - monitoring system

        log_level (str | None) - log level to use during the execution

        kwargs (Dict[str, Any] | None) -
    """
    global fexec
    fexec = lithops.StandaloneExecutor(**args)


@mcp.tool()
def lithops_map(
    func: str,
    name_func: str,
    map_iterdata: list,
    tuple_list: Optional[bool] = False,
    args: Optional[dict] = {},
):
    """
    Name:   lithops_map

    Description: Spawn multiple function activations based on the items of an input list.

    Arguments:
        'map_function': Callable,
        'map_iterdata': List[List[Any] | Tuple[Any, ...] | Dict[str, Any]]) - An iterable of input data (e.g python list)
        'tuple_list'

        args : {
            'extra_args' (List[Any] | Tuple[Any, ...] | Dict[str, Any] | None) - Additional arguments to pass to each map_function activation
            'timeout' (int | None) - Max time per function activation (seconds)
            'extra_env' (Dict[str, str] | None) - Additional environment variables for function environment

            'include_modules' (List[str] | None) - Explicitly pickle these dependencies. All required dependencies are pickled if default empty list. No one dependency is pickled if it is explicitly set to None
            'exclude_modules' (List[str] | None) - Explicitly keep these modules from pickled dependencies. It is not taken into account if you set include_modules.
            'chunksize'= (int | None) - Split map_iteradata in chunks of this size. Lithops spawns 1 worker per resulting chunk
            'runtime_memory' (int | None) - Memory (in MB) to use to run the functions
            'obj_chunk_size' (int | None) - Used for data processing. Chunk size to split each object in bytes. Must be >= 1MiB. 'None' for processing the whole file in one function activation
            'obj_chunk_number' (int | None) - Used for data processing. Number of chunks to split each object. 'None' for processing the whole file in one function activation. chunk_n has prevalence over chunk_size if both parameters are set
            'obj_newline' (str | None) - new line character for keeping line integrity of partitions. 'None' for disabling line integrity logic and get partitions of the exact same size in the functions
        }

    If the value is None you don't have to pass it, by default is set to None

    Return:
        A list with size len(map_iterdata) of futures for each job (Futures are also internally stored by Lithops).

    Return type:
        FutureList

    Example -->

        func = "def func(x,y): return x+y"
        name_func = "func"
        map_iterdata = [[2],[3],[4]]
        tuple_list = True
        args = {
            "extra_args":[10],
            "timeout":5
        }

    """
    exec(func, globals())
    if tuple_list:
        aux = [tuple(item) for item in map_iterdata]
        map_iterdata = aux
        try:
            if args["extra_args"] is not None:
                args["extra_args"] = tuple(args["extra_args"])
        except KeyError:
            print("Not extra_args")

    arguments = {
        "map_function": globals().get(name_func),
        "map_iterdata": map_iterdata,
    }

    arguments.update(args)
    global future
    future.append(fexec.map(**arguments))
    return future


@mcp.tool()
def lithops_call_async(
    func: str, name_func: str, data: list | dict, tuple_list: bool, args: dict
):
    """

    Name:   lithops_call_async

    Description: For running one function execution asynchronously.

    Arguments:
        func (Callable) - The function to map over the data
        name_func (str) - The name of the function
        data (List[Any] | Tuple[Any, ...] | Dict[str, Any]) - Input data. Arguments can be passed as a list or tuple, or as a dictionary for keyword arguments.
        tuple_list: (Bool) If the list passed is a tuple or not

        args:{
            extra_env (Dict | None) - Additional env variables for function environment.
            timeout (int | None) - Time that the function has to complete its execution before raising a timeout.
            runtime_memory (int | None) - Memory to use to run the function.
            include_modules (List | None) - Explicitly pickle these dependencies.
            exclude_modules (List | None) - Explicitly keep these modules from pickled dependencies.
        }

    Return :
        ResponseFuture

    Example -->
        First Example -->

            data = [[1, 2, 3, 4, 5]]
            func = "def sum_list(list_of_numbers):
                total = 0
                for num in list_of_numbers:
                    total = total+num
                return total"
            tuple_list=True
            args={
                "timeout":10
            }

        Second Example -->

            data = {"list_of_numbers": [1, 2, 3, 4, 5], "x": 3}
            func = "def sum_list(list_of_numbers,x):
                total = 0
                for num in list_of_numbers:
                    total = total+num
                return total*x"
            tuple_list=False
            args={
                "timeout":10
            }

    """
    exec(func, globals())
    if tuple_list:
        data = tuple(data)

    arguments = {
        "func": globals().get(name_func),
        "data": data,
    }

    arguments.update(args)
    global future
    future.append(fexec.call_async(**arguments))
    return future


@mcp.tool()
def lithops_map_reduce(
    map_function: str,
    map_func_name: str,
    map_iterdata: list,
    reduce_function: str,
    reduce_func_name: str,
    args: dict,
    tuple_list: Optional[bool] = False,
):
    """
    Name --> lithops_map_reduce

    Description --> Map the map_function over the data and apply the reduce_function across all futures.

    Parameters:

        map_function (Callable) - The function to map over the data
        map_func_nname (str) --> the name of the map function
        map_iterdata (List[List[Any] | Tuple[Any, ...] | Dict[str, Any]]) - An iterable of input data
        reduce_function (Callable) - The function to reduce over the futures
        reduce_func_name (str) --> the name of the function reduce
        tuple_list (bool) --> If we are talking about a tuple list

        args -->
            timeout (int | None) - Time that the functions have to complete their execution before raising a timeout
            extra_args (List[Any] | Tuple[Any, ...] | Dict[str, Any] | None) - Additional arguments to pass to function activation. Default None
            extra_args_reduce ( Tuple[Any, ...] | None) - Additional arguments to pass to the reduce function activation. Default None

            chunksize (int | None) - Split map_iteradata in chunks of this size. Lithops spawns 1 worker per resulting chunk. Default 1
            extra_env (Dict[str, str] | None) - Additional environment variables for action environment. Default None
            map_runtime_memory (int | None) - Memory to use to run the map function. Default None (loaded from config)
            reduce_runtime_memory (int | None) - Memory to use to run the reduce function. Default None (loaded from config)
            obj_chunk_size (int | None) - the size of the data chunks to split each object. 'None' for processing the whole file in one function activation
            obj_chunk_number (int | None) - Number of chunks to split each object. 'None' for processing the whole file in one function activation
            obj_newline (str | None) - New line character for keeping line integrity of partitions. 'None' for disabling line integrity logic and get partitions of the exact same size in the functions
            obj_reduce_by_key (bool | None) - Set one reducer per object after running the partitioner. By default there is one reducer for all the objects
            spawn_reducer (int | None) - Percentage of done map functions before spawning the reduce function
            include_modules (List[str] | None) - Explicitly pickle these dependencies.
            exclude_modules (List[str] | None) - Explicitly keep these modules from pickled dependencies.

    Returns --> A list with size len(map_iterdata) of futures.

    Return Type --> FuturesList

    Examples -->

        First Example -->
            lithops_map_reduce(
                map_function="
                    def my_map_function(x):
                        import time
                        time.sleep(x * 2)
                        return x + 7
                ",
                map_func_name="my_map_function",
                map_iterdata=[1, 2, 3, 4, 5],
                reduce_function="
                    def my_reduce_function(results):
                        total = 0
                        for map_result in results:
                            total = total + map_result
                        return total
                ",
                reduce_func_name="my_reduce_function",
            )

        Second Example -->
            lithops_map_reduce(
                map_function="
                    def my_map_function(x):
                        import time
                        time.sleep(x * 2)
                        return x + 7
                ",
                map_func_name="my_map_function",
                map_iterdata=[[1], [2], [3], [4], [5]],
                reduce_function="
                    def my_reduce_function(results):
                        total = 0
                        for map_result in results:
                            total = total + map_result
                        return total
                ",
                reduce_func_name="my_reduce_function",
                tuple_list=True
            )


    """

    exec(map_function, globals())
    exec(reduce_function, globals())

    if tuple_list:
        aux = [tuple(item) for item in map_iterdata]
        map_iterdata = aux

    try:
        if args["extra_args"] is not None and not isinstance(map_iterdata[0], dict):
            args["extra_args"] = tuple(args["extra_args"])
        if args["extra_args_reduce"] is not None:
            args["extra_args_reduce"] = tuple(args["extra_args_reduce"])
    except KeyError:
        print("Not extra_args_reduce")

    arguments = {
        "map_function": globals().get(map_func_name),
        "map_iterdata": map_iterdata,
        "reduce_function": globals().get(reduce_func_name),
    }

    arguments.update(args)
    global future
    future.append(fexec.map_reduce(**arguments))
    return future


@mcp.tool()
def lithops_get_result(
    args: dict, response: bool, start: Optional[int] = None, end: Optional[int] = None
):
    """
    Name --> lithops_get_result
    Description --> For getting the results from all function activations
    Parameters -->
        fs (ResponseFuture | FuturesList | List[ResponseFuture] | None) - Futures list. Default None (Indexes of the list future[start:end])
        args:{
                throw_except (bool | None) - Reraise exception if call raised. Default True.
                timeout (int | None) - Timeout for waiting for results.
                threadpool_size (int | None) - Number of threads to use. Default 128
                wait_dur_sec (int | None) - Time interval between each check. Default 1 second
                show_progressbar (bool | None) - whether or not to show the progress bar.
            }
    """
    global future
    if response:
        args["fs"] = future[start:end]
    else:
        args["fs"] = [item for sublist in future[start:end] for item in sublist]
    return fexec.get_result(**args)


@mcp.tool()
def lithops_clean(
    args: dict, response: bool, start: Optional[int] = None, end: Optional[int] = None
):
    """
    Name --> lithops_clean
    Description --> Deletes all the temp files from storage. These files include the function, the data serialization and the function invocation results. It can also clean cloudobjects.
    Parameters -->
        fs (ResponseFuture | FuturesList | List[ResponseFuture] | None) - Futures list. Default None (Indexes of the list future[start:end])
        # cs (List[CloudObject] | None) - List of cloudobjects to clean (Not Implemented)
        args:{
                clean_cloudobjects (bool | None) - Delete all cloudobjects created with this executor
                clean_fn (bool | None) - Delete cached functions in this executor
                force (bool | None) - Clean all future objects even if they have not benn completed
                on_exit (bool | None) -
            }

    Param on_exit -->
        do not print logs on exit
    """
    global future
    if response:
        args["fs"] = future[start:end]
    else:
        args["fs"] = [item for sublist in future[start:end] for item in sublist]
    return fexec.clean(**args)


@mcp.tool()
def lithops_job_summary(cloud_objects_n: Optional[int] = None):
    """
    Description --> Logs information of a job executed by the calling function executor. currently supports: code_engine, ibm_vpc and ibm_cf.
    Parameters -->
        cloud_objects_n (int | None) - number of cloud object used in COS, declared by user.
    """
    fexec.job_summary(cloud_objects_n=cloud_objects_n)


@mcp.tool()
def lithops_plot(
    args: dict, response: bool, start: Optional[int] = None, end: Optional[int] = None
):
    """
    Name --> lithops_plot
    Description --> Creates timeline and histogram of the current execution in dst_dir.

    Parameters -->
        fs (ResponseFuture | FuturesList | List[ResponseFuture] | None) - Futures list. Default None (Indexes of the list future[start:end])
        args:{
                dst (str | None) - destination path to save .png plots.
                figsize (tuple | None)
            }

    """
    global future
    if response:
        args["fs"] = future[start:end]
    else:
        args["fs"] = [item for sublist in future[start:end] for item in sublist]
    fexec.plot(**args)
    stat = []
    for fut in args["fs"]:
        stat.append(fut.stats)
    return stat


@mcp.tool()
def lithops_wait(
    args: dict, response: bool, start: Optional[int] = None, end: Optional[int] = None
):
    """
    Name --> lithops_wait

    Description --> Wait for the Future instances (possibly created by different Executor instances) given by fs to complete.
    Returns a named 2-tuple of sets. The first set, named done, contains the futures that completed (finished or cancelled futures) before the wait completed.
    The second set, named not_done, contains the futures that did not complete (pending or running futures). timeout can be used to control the maximum number of seconds to wait before returning.

    Parameters -->

        fs (ResponseFuture | FuturesList | List[ResponseFuture] | None) - Futures list. Default None (Indexes of the list future[start:end])
        args:{
            throw_except (bool | None) - Re-raise exception if call raised. Default True
            return_when (Any | None) - Percentage of done futures
            download_results (bool | None) - Download results. Default false (Only get statuses)
            timeout (int | None) - Timeout of waiting for results
            threadpool_size (int | None) - Number of threads to use. Default 64
            wait_dur_sec (int | None) - Time interval between each check. Default 1 second
            show_progressbar (bool | None) - whether or not to show the progress bar.
        }

    Returns --> (fs_done, fs_notdone) where fs_done is a list of futures that have completed and fs_notdone is a list of futures that have not completed.

    Return Type --> Tuple[FuturesList, FuturesList]
    """
    global future
    if response:
        args["fs"] = future[start:end]
    else:
        args["fs"] = [item for sublist in future[start:end] for item in sublist]
    return fexec.wait(**args)


@mcp.tool()
def delete_from_future(start: int, end: int):
    """
    Description --> Delete Future from the future list
    Params -->
        Indexes of the list future[start:end]
    Return --> Future List NOW
    """
    global future
    del future[start:end]
    return future


if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8080)
