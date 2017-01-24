/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_ATOMIC_H_
#define SRC_ATOMIC_H_ 1

#include "config.h"

#include <atomic>

#include "callbacks.h"
#include "locks.h"
#include "utility.h"

template <typename T>
void atomic_setIfBigger(std::atomic<T> &obj, const T &newValue) {
    T oldValue = obj.load();
    while (newValue > oldValue) {
        if (obj.compare_exchange_strong(oldValue, newValue)) {
            break;
        }
        oldValue = obj.load();
    }
}

template <typename T>
void atomic_setIfLess(std::atomic<T> &obj, const T &newValue) {
    T oldValue = obj.load();
    while (newValue < oldValue) {
        if (obj.compare_exchange_strong(oldValue, newValue)) {
            break;
        }
        oldValue = obj.load();
    }
}

template <typename T>
T atomic_swapIfNot(std::atomic<T> &obj, const T &badValue, const T &newValue) {
    T oldValue;
    while (true) {
        oldValue = obj.load();
        if (oldValue != badValue) {
            if (obj.compare_exchange_strong(oldValue, newValue)) {
                break;
            }
        } else {
            break;
        }
    }
    return oldValue;
}

/**
 * Atomic pointer.
 *
 * This does *not* make the item that's pointed to atomic.
 */
template <typename T>
class AtomicPtr : public std::atomic<T*> {
public:
    AtomicPtr(T *initial = NULL) : std::atomic<T*>(initial) {}

    ~AtomicPtr() {}

    T *operator ->() {
        return std::atomic<T*>::load();
    }

    T &operator *() {
        return *std::atomic<T*>::load();
    }

    operator bool() const {
        return std::atomic<T*>::load() != NULL;
    }

    bool operator !() const {
        return std::atomic<T*>::load() == NULL;
    }
};

/**
 * A lighter-weight, smaller lock than a mutex.
 *
 * This is primarily useful when contention is rare.
 */
class SpinLock {
public:
    // It seems like inlining the code caused the dtrace probe to
    // be optimized away ;)
    SpinLock();
    ~SpinLock();

    void lock(void);
    void unlock(void);

private:
    bool tryAcquire(void);

    std::atomic_flag lck;
    DISALLOW_COPY_AND_ASSIGN(SpinLock);
};

template <class T> class RCPtr;
template <class S> class SingleThreadedRCPtr;

/**
 * A reference counted value (used by RCPtr and SingleThreadedRCPtr).
 */
class RCValue {
public:
    RCValue() : _rc_refcount(0) {}
    RCValue(const RCValue &) : _rc_refcount(0) {}
    ~RCValue() {}
private:
    template <class MyTT> friend class RCPtr;
    template <class MySS> friend class SingleThreadedRCPtr;
    int _rc_incref() const {
        return ++_rc_refcount;
    }

    int _rc_decref() const {
        return --_rc_refcount;
    }

    mutable std::atomic<int> _rc_refcount;
};

/**
 * Concurrent reference counted pointer.
 */
template <class C>
class RCPtr {
public:
    RCPtr(C *init = NULL) : value(init) {
        if (init != NULL) {
            static_cast<RCValue*>(value)->_rc_incref();
        }
    }

    RCPtr(const RCPtr<C> &other) : value(other.gimme()) {}

    ~RCPtr() {
        if (value && static_cast<RCValue *>(value)->_rc_decref() == 0) {
            delete get();
        }
    }

    void reset(C *newValue = NULL) {
        if (newValue != NULL) {
            static_cast<RCValue *>(newValue)->_rc_incref();
        }
        swap(newValue);
    }

    void reset(const RCPtr<C> &other) {
        swap(other.gimme());
    }

    // safe for the lifetime of this instance
    C *get() const {
        return value;
    }

    RCPtr<C> & operator =(const RCPtr<C> &other) {
        reset(other);
        return *this;
    }

    C &operator *() const {
        return *value;
    }

    C *operator ->() const {
        return value;
    }

    bool operator! () const {
        return !value;
    }

    operator bool () const {
        return (bool)value;
    }

private:
    C *gimme() const {
        std::lock_guard<SpinLock> lh(lock);
        if (value) {
            static_cast<RCValue *>(value)->_rc_incref();
        }
        return value;
    }

    void swap(C *newValue) {
        C* tmp;
        {
            std::lock_guard<SpinLock> lh(lock);
            tmp = value.exchange(newValue);
        }
        if (tmp != NULL && static_cast<RCValue *>(tmp)->_rc_decref() == 0) {
            delete tmp;
        }
    }

    AtomicPtr<C> value;
    mutable SpinLock lock; // exists solely for the purpose of implementing reset() safely
};

/**
 * Single-threaded reference counted pointer.
 * "Single-threaded" means that the reference counted pointer should be accessed
 * by only one thread at any time or accesses to the reference counted pointer
 * by multiple threads should be synchronized by the external lock.
 */
template <class T>
class SingleThreadedRCPtr {
public:
    SingleThreadedRCPtr(T *init = NULL) : value(init) {
        if (init != NULL) {
            static_cast<RCValue*>(value)->_rc_incref();
        }
    }

    SingleThreadedRCPtr(const SingleThreadedRCPtr<T> &other) : value(other.gimme()) {}

    template <typename Y>
    SingleThreadedRCPtr(const SingleThreadedRCPtr<Y>& other)
        : value(other.gimme()) {
    }

    ~SingleThreadedRCPtr() {
        if (value && static_cast<RCValue *>(value)->_rc_decref() == 0) {
            delete value;
        }
    }

    void reset(T *newValue = NULL) {
        if (newValue != NULL) {
            static_cast<RCValue *>(newValue)->_rc_incref();
        }
        swap(newValue);
    }

    void reset(const SingleThreadedRCPtr<T> &other) {
        swap(other.gimme());
    }

    // safe for the lifetime of this instance
    T *get() const {
        return value;
    }

    SingleThreadedRCPtr<T> & operator =(const SingleThreadedRCPtr<T> &other) {
        reset(other);
        return *this;
    }

    T &operator *() const {
        return *value;
    }

    T *operator ->() const {
        return value;
    }

    bool operator! () const {
        return !value;
    }

    operator bool () const {
        return (bool)value;
    }

private:
    template <typename Y>
    friend class SingleThreadedRCPtr;

    T *gimme() const {
        if (value) {
            static_cast<RCValue *>(value)->_rc_incref();
        }
        return value;
    }

    void swap(T *newValue) {
        T *old = value;
        value = newValue;
        if (old != NULL && static_cast<RCValue *>(old)->_rc_decref() == 0) {
            delete old;
        }
    }

    T *value;
};

template <typename T, class... Args>
SingleThreadedRCPtr<T> make_STRCPtr(Args&&... args) {
    return SingleThreadedRCPtr<T>(new T(std::forward<Args>(args)...));
}

/**
 * Debugging wrapper around std::atomic which print all accesses to the atomic
 * value to stderr.
 */
template <typename T>
class LoggedAtomic {
public:
    LoggedAtomic(T initial)
        : value(initial) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        std::cerr << "LoggedAtomic[" << this << "]::LoggedAtomic: "
                  << value.load() << std::endl;

    }

    T operator=(T desired) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        value.store(desired);
        std::cerr << "LoggedAtomic[" << this << "]::operator=: "
                  << value.load() << std::endl;
        return value.load();
    }

    T load() const {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        auto result = value.load();
        std::cerr << "LoggedAtomic[" << this << "]::load: " << result
                  << std::endl;
        return result;
    }

    void store(T desired) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        value.store(desired);
        std::cerr << "LoggedAtomic[" << this << "]::store: " << value.load()
                  << std::endl;
    }

    operator T() const {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        auto result = value.load();
        std::cerr << "LoggedAtomic[" << this << "]::operator T: " << result
                  << std::endl;
        return result;
    }

    bool compare_exchange_strong(T& expected, T desired,
                                 std::memory_order order =
                                      std::memory_order_seq_cst ) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        std::cerr << "LoggedAtomic[" << this << "]::compare_exchange_strong("
                  << "expected:" << expected << ", desired:) = " << desired;
        auto result = value.compare_exchange_strong(expected, desired, order);
        std::cerr << result << std::endl;
        return result;
    }

    T fetch_add(T arg,
                std::memory_order order = std::memory_order_seq_cst ) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        T result = value.fetch_add(arg, order);
        std::cerr << "LoggedAtomic[" << this << "]::fetch_add(" << arg
                  << "): " << result << std::endl;
        return value.load();
    }

    T fetch_sub(T arg,
                std::memory_order order = std::memory_order_seq_cst ) {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        T result = value.fetch_sub(arg, order);
        std::cerr << "LoggedAtomic[" << this << "]::fetch_sub(" << arg
                  << "): " << result << std::endl;
        return value.load();
    }

    T& operator++() {
        std::lock_guard<std::mutex> lock(stderr_mutex);
        ++value;
        std::cerr << "LoggedAtomic[" << this << "]::pre-increment: "
                  << value << std::endl;
        return value;
    }

protected:
    mutable std::mutex stderr_mutex;
    std::atomic<T> value;
};

#endif  // SRC_ATOMIC_H_
