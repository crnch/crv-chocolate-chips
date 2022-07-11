import shelve
import os
from functools import wraps
import itertools
import hashlib

from loguru import logger

def cache(fnc):
    """
    Saves the result to disk with the sorted tuple of the args as key
    """
    dir = os.path.dirname(__file__)
    shelve_path = os.path.join(dir, "cache", "cache.shelve")

    @wraps(fnc)
    def wrapper(*args, **kwargs):
        sorted_kwargs_keys = sorted(kwargs.keys())
        if args and isinstance(obj := args[0], object):
            cache_args = [obj.__class__.__name__] + list(args[1:])
        else:
            cache_args = args
        hash = hashlib.sha256()
        for hashable_arg in itertools.chain([fnc.__module__, fnc.__name__], cache_args, [kwargs[key] for key in sorted_kwargs_keys]):
            args_str = str(hashable_arg)
            hash.update(args_str.encode("utf-8"))
        cache_key = hash.hexdigest()
        with shelve.open(shelve_path) as db:
            if (result := db.get(cache_key)):
                logger.success(f"Result for function {fnc.__name__} cached from disk")
            else:
                result = fnc(*args, **kwargs)
                db[cache_key] = result
            return result

    return wrapper


