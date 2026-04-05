"""
Micro-benchmark: RLock vs Lock acquire/release overhead.

Measures the performance difference between threading.RLock and
threading.Lock for non-recursive lock acquisition patterns.

Run:
    python benchmarks/bench_rlock_vs_lock.py
"""
import timeit
from threading import Lock, RLock


def bench_lock_types():
    """Compare Lock vs RLock acquire/release cycles."""
    lock = Lock()
    rlock = RLock()

    n = 2_000_000

    def use_lock():
        lock.acquire()
        lock.release()

    def use_rlock():
        rlock.acquire()
        rlock.release()

    def use_lock_with():
        with lock:
            pass

    def use_rlock_with():
        with rlock:
            pass

    t_lock = timeit.timeit(use_lock, number=n)
    t_rlock = timeit.timeit(use_rlock, number=n)

    print(f"Lock   acquire/release ({n} iters): {t_lock:.3f}s  ({t_lock / n * 1e9:.1f} ns/cycle)")
    print(f"RLock  acquire/release ({n} iters): {t_rlock:.3f}s  ({t_rlock / n * 1e9:.1f} ns/cycle)")
    print(f"RLock overhead: {(t_rlock / t_lock - 1) * 100:.0f}%  ({t_rlock / t_lock:.2f}x)")

    t_lock_with = timeit.timeit(use_lock_with, number=n)
    t_rlock_with = timeit.timeit(use_rlock_with, number=n)

    print(f"\nLock   'with' stmt     ({n} iters): {t_lock_with:.3f}s  ({t_lock_with / n * 1e9:.1f} ns/cycle)")
    print(f"RLock  'with' stmt     ({n} iters): {t_rlock_with:.3f}s  ({t_rlock_with / n * 1e9:.1f} ns/cycle)")
    print(f"RLock overhead: {(t_rlock_with / t_lock_with - 1) * 100:.0f}%  ({t_rlock_with / t_lock_with:.2f}x)")


if __name__ == '__main__':
    bench_lock_types()
