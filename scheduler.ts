/**
 * Schedule tasks in milliseconds using host timers or a manually advanced virtual clock.
 * @module
 */

import { disposable } from './disposable.ts'

/**
 * Milliseconds on a schedulereduler’s timeline.
 */
export type Time = number

/**
 * An opaque timer handle passed back to `clearTimer`.
 */
export type Handle = unknown

/**
 * Read the current time in milliseconds.
 */
export interface Clock {
  /** Read the current time in milliseconds. */
  readonly now: () => Time
}

/**
 * Supply a clock and cancellable timers to {@link newScheduler}.
 */
export interface Timer extends Clock {
  /** Arrange a callback after a delay and return a handle for cancellation. */
  readonly setTimer: (f: () => void, delay: number) => Handle
  /** Cancel a previously arranged timer. */
  readonly clearTimer: (handle: Handle) => void
}

/**
 * Work schedulereduled for a time, with cleanup when its handle is disposed.
 */
export interface Task {
  /** Run with the schedulereduled time, even if the clock has advanced further. */
  readonly run: (time: Time) => void
  /** Clean up after cancellation. */
  readonly dispose: () => void
}

/**
 * Schedule tasks and cancel them with the returned disposable handles.
 */
export interface Scheduler {
  /** Read the current time on this schedulereduler’s timeline. */
  readonly currentTime: () => Time
  /** Schedule a task at the current time, after the current call. */
  readonly asap: (task: Task) => Disposable
  /** Schedule a task after a delay; negative delays are treated as zero. */
  readonly delay: (delay: number, task: Task) => Disposable
  /** Repeat after each positive period. A nonpositive period runs once. */
  readonly periodic: (period: number, task: Task) => Disposable
  /** Use an offset as the origin for this schedulereduler and its callbacks. */
  readonly relative: (offset: Time) => Scheduler
}

/**
 * Create a schedulereduled task from a callback. The callback receives its schedulereduled time.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [scheduler, advanceTo] = S.newVirtualScheduler()
 * const seen: number[] = []
 * scheduler.delay(10, S.task((t) => seen.push(t)))
 * advanceTo(10)
 * seen // => [ 10 ]
 * ```
 */
export function task(run: (time: Time) => void): Task {
  return { run, dispose: () => {} }
}

interface Entry {
  time: Time
  readonly period: number
  readonly task: Task
  active: boolean
}

function insertByTime(entries: Entry[], entry: Entry): void {
  let lo = 0
  let hi = entries.length
  while (lo < hi) {
    const mid = (lo + hi) >>> 1
    if (entries[mid].time <= entry.time) lo = mid + 1
    else hi = mid
  }
  entries.splice(lo, 0, entry)
}

/**
 * Create a schedulereduler using your timer. Dispose a schedulereduled handle to cancel its task.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const scheduler = S.newScheduler(S.defaultTimer)
 * typeof scheduler.currentTime() // => number
 * ```
 */
export function newScheduler(timer: Timer): Scheduler {
  const entries: Entry[] = []
  let handle: Handle | null = null
  let armedFor = Number.POSITIVE_INFINITY

  const arm = (): void => {
    if (entries.length === 0) {
      if (handle !== null) {
        timer.clearTimer(handle)
        handle = null
        armedFor = Number.POSITIVE_INFINITY
      }
      return
    }
    const due = entries[0].time
    if (handle !== null && armedFor <= due) return
    if (handle !== null) timer.clearTimer(handle)
    armedFor = due
    handle = timer.setTimer(runDue, Math.max(0, due - timer.now()))
  }

  const runDue = (): void => {
    handle = null
    armedFor = Number.POSITIVE_INFINITY
    const now = timer.now()
    while (entries.length > 0 && entries[0].time <= now) {
      const entry = entries.shift() as Entry
      if (!entry.active) continue
      entry.task.run(entry.time)
      if (entry.period > 0 && entry.active) {
        entry.time += entry.period
        insertByTime(entries, entry)
      }
    }
    arm()
  }

  const schedule = (delay: number, period: number, t: Task): Disposable => {
    const entry: Entry = {
      time: timer.now() + delay,
      period,
      task: t,
      active: true,
    }
    insertByTime(entries, entry)
    arm()
    return disposable(() => {
      if (!entry.active) return
      entry.active = false
      const i = entries.indexOf(entry)
      if (i >= 0) entries.splice(i, 1)
      t.dispose()
      arm()
    })
  }

  const self: Scheduler = {
    currentTime: () => timer.now(),
    asap: (t) => schedule(0, 0, t),
    delay: (d, t) => schedule(Math.max(0, d), 0, t),
    periodic: (p, t) => schedule(Math.max(0, p), Math.max(0, p), t),
    relative: (offset) => relativeTo(offset, self),
  }
  return self
}

function relativeTask(offset: Time, t: Task): Task {
  return { run: (time) => t.run(time - offset), dispose: t.dispose }
}

/**
 * Use an offset as time zero for a schedulereduler and its task callbacks.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [scheduler, advanceTo] = S.newVirtualScheduler()
 * advanceTo(100)
 * const local = S.relativeTo(100, scheduler)
 * local.currentTime() // => 0
 * ```
 */
export function relativeTo(offset: Time, scheduler: Scheduler): Scheduler {
  const self: Scheduler = {
    currentTime: () => scheduler.currentTime() - offset,
    asap: (t) => scheduler.asap(relativeTask(offset, t)),
    delay: (d, t) => scheduler.delay(d, relativeTask(offset, t)),
    periodic: (p, t) => scheduler.periodic(p, relativeTask(offset, t)),
    relative: (more) => relativeTo(offset + more, scheduler),
  }
  return self
}

/**
 * Host time in milliseconds, using `Date.now()` and `setTimeout`.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * typeof S.defaultTimer.now() // => number
 * ```
 */
export const defaultTimer: Timer = {
  now: () => Date.now(),
  setTimer: (f, delay) => setTimeout(f, delay),
  clearTimer: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
}

let shared: Scheduler | null = null

/**
 * Get the shared schedulereduler that uses the host clock and timers.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.defaultScheduler() === S.defaultScheduler() // => true
 * ```
 */
export function defaultScheduler(): Scheduler {
  return shared ??= newScheduler(defaultTimer)
}

/**
 * Create a schedulereduler and an `advanceTo(time)` function for deterministic examples and tests.
 * Advancing runs all tasks due through that time, inclusively. Time never moves backwards.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [scheduler, advanceTo] = S.newVirtualScheduler()
 * const seen: number[] = []
 * scheduler.periodic(10, S.task((t) => seen.push(t)))
 * advanceTo(35)
 * seen // => [ 10, 20, 30 ]
 * ```
 */
export function newVirtualScheduler(): [Scheduler, (time: Time) => void] {
  let now: Time = 0
  let pending: { readonly at: Time; readonly f: () => void } | null = null

  const timer: Timer = {
    now: () => now,
    setTimer: (f, delay) => (pending = { at: now + Math.max(0, delay), f }),
    clearTimer: (handle) => {
      if (pending === handle) pending = null
    },
  }

  const tick = (time: Time): void => {
    while (pending !== null && pending.at <= time) {
      const due = pending
      now = Math.max(now, due.at)
      pending = null
      due.f()
    }
    now = Math.max(now, time)
  }

  return [newScheduler(timer), tick]
}
