import threading
from collections import defaultdict
from functools import lru_cache, _make_key

def run_once_cache(func):
    func = lru_cache()(func)
    lock_dict = defaultdict(threading.Lock)

    def _thread_lru(*args, **kwargs):
        key = _make_key(args, kwargs, typed=False)  
        with lock_dict[key]:
            return func(*args, **kwargs)

    return _thread_lru

