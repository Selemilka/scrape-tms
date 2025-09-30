# кто увидит этот код - предупреждаю, это нахуй джипити-потоко-санхиронно-очередно-декораторное искусство от челика который пишет на питоне 0 лет
import time
import threading
import functools
import schedule
import queue
import asyncio
from datetime import datetime, timezone 

import scrape_postgres as pg
import parser_sbercity as sbercity
import parser_a101 as a101
import parser_forma as forma
import parser_pik as pik
import parser_mrgroup as mrgroup
import parser_donstroy as donstroy
import parser_samolet as samolet
import parser_ingrad as ingrad
import parser_level as level


jobqueue = queue.Queue()
jobs_in_queue = [] # TODO create set
job_results = []


def get_func_name(target):
    return target.func.__qualname__ if isinstance(target, functools.partial) else target.__qualname__


def as_thread(target=None, **tkwds):
    # A decorator to create a one-off thread from a function.
    if target is None:
        # Used as a decorator factory
        return lambda target: as_thread(target, **tkwds)

    def _target(*args, **kwargs):
        func_name = get_func_name(target)
        try:
            jobs_in_queue.append(func_name)
            start_time = datetime.now(timezone.utc)
            try:
                res = target(*args, **kwargs)
                t.result = res
            except Exception as exc:
                t.failure = exc
            end_time = datetime.now(timezone.utc)

            if hasattr(target, 'failure'):
                result = (func_name, start_time, end_time, "FAIL", target.failure)
            elif hasattr(target, 'result'):
                result = (func_name, start_time, end_time, "OK", target.result)
            else:
                result = (func_name, start_time, end_time, "OK", None)

            job_results.append(result)
            pg.safe_insert_job_result(result)
            jobqueue.task_done()
        except Exception as ex:
            print(f'something VERY bad happened: {ex}')
        finally:
            jobs_in_queue.remove(func_name)

    t = threading.Thread(target=_target, **tkwds)
    return t


def threaded(target, **tkwds):
    # A decorator to produce a started thread when the "function" is called.
    if target is None:
        # Used as a decorator factory
        return lambda target: as_thread(target, **tkwds)

    @functools.wraps(target)
    def wrapper(*targs, **tkwargs):
        def _target(*args, **kwargs):
            try:
                t.result = target(*args, **kwargs)
            except Exception as exc:
                t.failure = exc
        t = threading.Thread(target=_target, args=targs, kwargs=tkwargs, **tkwds)
        t.start()
        return t
    return wrapper


def worker():
    while 1:
        job_func = jobqueue.get()
        if (jobs_in_queue.count(get_func_name(job_func))):
            jobqueue.put(job_func)
        else:
            job_thread = as_thread(job_func)
            job_thread.start()
        time.sleep(4) # кто жизнь познал тот не спешит 👆🏻


def show_jobs():
    print('-----')
    for x in job_results:
        print(f"{x[0]} {x[1].timestamp()} {x[2].timestamp()} {x[3]} {x[4]}")
    print('+++++')


async def main():
    # schedule.every(20).seconds.do(jobqueue.put, functools.partial(job, 4))
    # schedule.every().day.at("00:05:00").do(jobqueue.put, sbercity.scrape_full)
    # schedule.every().day.at("00:10:00").do(jobqueue.put, a101.scrape_full)
    # schedule.every().second.do(jobqueue.put, show_jobs)

    try:
        result = sbercity.scrape_full()
        print(result)
    except Exception as ex:
        print("SBER NOT WORKING")
        print(ex)

    try:
        result = a101.scrape_full()
        print(result)
    except Exception as ex:
        print("A101 NOT WORKING")
        print(ex)

    try:
        result = forma.scrape_full()
        print(result)
    except Exception as ex:
        print("FORMA NOT WORKING")
        print(ex)

    try:
        result = pik.scrape_full()
        print(result)
    except Exception as ex:
        print("PIK NOT WORKING")
        print(ex)

    try:
        result = mrgroup.scrape_full()
        print(result)
    except Exception as ex:
        print("MRGROUP NOT WORKING")
        print(ex)

    try:
        result = donstroy.scrape_full()
        print(result)
    except Exception as ex:
        print("DONSTROY NOT WORKING")
        print(ex)

    try:
        result = samolet.scrape_full()
        print(result)
    except Exception as ex:
        print("SAMOLET NOT WORKING") ## !!!!!!!!!!
        print(ex)

    try:
        result = ingrad.scrape_full()
        print(result)
    except Exception as ex:
        print("INGRAD NOT WORKING")
        print(ex)
    
    try:
        result = level.scrape_full()
        print(result)
    except Exception as ex:
        print("LEVEL NOT WORKING")
        print(ex)

    show_jobs()

    worker_thread = threading.Thread(target=worker)
    worker_thread.start()
    while 1:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    # logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
