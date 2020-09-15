from cudf.testing import fuzzer


class PythonFuzz(object):
    def __init__(self, func, data_handle=None, **kwargs):
        self.function = func
        self.data_handler_class = data_handle
        f = fuzzer.Fuzzer(
            target=self.function,
            data_handler_class=self.data_handler_class,
            dirs=kwargs.get("dir", None),
            crash_reports_dir=kwargs.get("crash_reports_dir", None),
            regression=kwargs.get("regression", False),
            max_rows_size=kwargs.get("max_rows_size", 4096),
            max_cols_size=kwargs.get("max_cols_size", 1000),
            runs=kwargs.get("runs", -1),
        )
        f.start()


# wrap PythonFuzz to allow for deferred calling
def pythonfuzz(function=None, data_handle=None, **kwargs):
    if function:
        return PythonFuzz(function, **kwargs)
    else:

        def wrapper(function):
            return PythonFuzz(function, data_handle, **kwargs)

        return wrapper


if __name__ == "__main__":
    PythonFuzz(None)
