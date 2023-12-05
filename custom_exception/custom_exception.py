import inspect
import traceback
from functools import wraps
from typing import Optional, Iterable, Callable, Union, Any

from rplus_utils.logger import Logging


def custom_exception(
    message: str = None,
    fallback_func: Optional[Callable] = None,
    fallback_args: Optional[Iterable] = None,
    fallback_args_same_as_original: bool = True,
    custom_exception_class: Optional[Any] = None,
) -> Union[Callable, Any]:
    """This decorator will handle exception with out declare it explicitly, it will make code more clean
    :param message: string message when exception is reach
    :param fallback_func: function that need to execute when exception is reach
    :param fallback_args: parameters that need to add in our method
    :param fallback_args_same_as_original: bool is to tell this function that fall back arguments same as original function
    :param custom_exception_class: is custom wrapper for exception
    :return: return value can be another function or result calculation
    """

    def handle_exception(func):
        @wraps(func)
        def catch_error(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except custom_exception_class if custom_exception_class is not None else Exception as e:
                # set default message
                # this message format will look like this
                # [ValueError - calculate_fallback] -> invalid literal for int() with base 10: 'a'
                msg = "[{} - {}] -> {} -> {}".format(
                    e.__class__.__name__,
                    func.__name__,
                    ", ".join(e.args),
                    str(e.__traceback__),
                )
                # if message is exists then print the message
                if message is not None:
                    msg = "{} -> {}".format(message, msg)

                # print logging message to console
                Logging.error(msg)
                # print stack trace
                traceback.print_tb(e.__traceback__)
                # if any fallback method then execute it
                if fallback_func is not None:

                    if fallback_args_same_as_original is False:
                        return fallback_func(*fallback_args)

                    return fallback_func(*args, **kwargs)

                # otherwise return none value
                return None

        return catch_error

    return handle_exception


def class_custom_exception(
    message: str = None,
    fallback_func: Optional[Callable] = None,
    fallback_args: Optional[Iterable] = None,
    fallback_args_same_as_original: bool = True,
) -> Union[Callable, Any]:
    """This decorator will handle exception with out declare it explicitly, it will make code more clean
    :param message: string message when exception is reach
    :param fallback_func: function that need to execute when exception is reach
    :param fallback_args: parameters that need to add in our method
    :param fallback_args_same_as_original: bool is to tell this function that fall back arguments same as original function
    :return: return value can be another function or result calculation
    """

    def handle_exception(func):
        @wraps(func)
        def catch_error(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                # set default message
                # this message format will look like this
                # [ValueError - calculate_fallback] -> invalid literal for int() with base 10: 'a'
                msg = "[{} - {}] -> {} -> {}".format(
                    e.__class__.__name__,
                    func.__name__,
                    ", ".join(e.args),
                    str(e.__traceback__),
                )
                # if message is exists then print the message
                if message is not None:
                    msg = "{} -> {}".format(message, msg)

                # print logging message to console
                Logging.error(msg)
                # print stack trace
                traceback.print_tb(e.__traceback__)
                # if any fallback method then execute it
                if fallback_func is not None:

                    if fallback_args_same_as_original is False:
                        return getattr(self, fallback_func.__name__)(*fallback_args)

                    if (
                        len(
                            inspect.getfullargspec(
                                getattr(self, fallback_func.__name__)
                            ).args
                        )
                        > 2
                    ):
                        return getattr(self, fallback_func.__name__)(*args, **kwargs)

                    return getattr(self, fallback_func.__name__)()

                # otherwise return none value
                return None

        return catch_error

    return handle_exception
