import os
import sys
import threading
from client_setup import load_env, normalize_beam_env

from beam import Image, Sandbox, GpuType

NUM_WORKERS = 4


def worker(index: int):
    sandbox = Sandbox(
        image=Image(),
        gpu=GpuType.A6000,
        gpu_count=1,
        gpu_virtualized=True,
    ).create()
    proc = None
    try:
        proc = sandbox.process.exec('nvidia-smi')
    except Exception as e:
        print(f'sandbox worker: {index}; received error: {e}')
    finally:
        if proc is not None:
            proc.wait()
            print(f'sandbox worker: {index}; result: {proc.stdout.read()}')
    if sandbox.terminate():
        print(f'sandbox worker {index} terminated')

if __name__ == "__main__":
    exit_code = 0
    load_env()
    normalize_beam_env()
    try:
        threaded = False
        if len(sys.argv) > 1:
            for index, arg in enumerate(sys.argv):
                if arg == "--mode" and index < len(sys.argv) - 1 and sys.argv[index + 1] == "threaded":
                    threaded = True

        if threaded:
            threads = []
            for index in range(NUM_WORKERS):
                thread = threading.Thread(target=worker, args=(index,))
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()
        else:
            for index in range(NUM_WORKERS):
                worker(index)
    except BaseException:
        exit_code = 1
        import traceback

        traceback.print_exc()
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(exit_code)
