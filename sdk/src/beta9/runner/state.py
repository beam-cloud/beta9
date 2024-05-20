import threading


class ThreadLocal(threading.local):
    task_id: str = ""


thread_local = ThreadLocal()
