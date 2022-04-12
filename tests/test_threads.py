#!/usr/bin/env python

"""
此测试主要用来测试，kafka producer 发送消息类，是否是线程安全的。

她启动了4个线程同时发送，看这4个线程能否都发送成功。

发送成功之后会抛出 IntendedException 异常，如果捕获到了这个异常则说明发送成功。
"""

from confluent_kafka import Producer
import threading
import time
try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty


class IntendedException (Exception):
    pass


def thread_run(myid, p, q):
    def do_crash(err, msg):
        raise IntendedException()

    for i in range(1, 3):
        cb = None
        # 每个线程只发送一条消息，从第二条开始发送线程就退出了。
        if i == 2:
            cb = do_crash
        p.produce('mytopic', value='hi', callback=cb)
        t = time.time()
        try:
            p.flush()
            print(myid, 'Flush %s took %.3f' % (i, time.time() - t))
        except IntendedException:
            print(myid, "Intentional callback crash: ok")
            continue

    print(myid, 'Done')
    q.put(myid)


def test_thread_safety():
    """ Basic thread safety tests. """

    q = Queue()
    p = Producer({
        'bootstrap.servers': 'localhost:9092',
        'socket.timeout.ms': 10,
        'message.timeout.ms': 10
    })

    threads = list()
    for i in range(1, 5):
        thr = threading.Thread(target=thread_run, name=str(i), args=[i, p, q])
        thr.start()
        threads.append(thr)

    for thr in threads:
        thr.join()

    # Count the number of threads that exited cleanly
    cnt = 0
    try:
        for x in iter(q.get_nowait, None):
            cnt += 1
    except Empty:
        pass

    if cnt != len(threads):
        raise Exception('Only %d/%d threads succeeded' % (cnt, len(threads)))

    print('All Done')


if __name__ == '__main__':
    test_thread_safety()
