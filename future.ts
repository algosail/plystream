/**
 * Convert between streams and plyfuture tasks. Import this module from
 * `@algosail/plystream/future`. Task failures become Either values in streams.
 * @module
 */

import type { Either } from '@algosail/ply/either'
import type { Future } from '@algosail/plyfuture'
import type { Behavior } from './behavior.ts'
import type { Scheduler, Time } from './scheduler.ts'
import type { Stream } from './stream.ts'
import { left, right } from '@algosail/ply/either'
import { future } from '@algosail/plyfuture'
import { disposable, dispose, disposeNone } from './disposable.ts'
import { sink, stream } from './stream.ts'
import { switcher } from './behavior.ts'
import { defaultScheduler, task } from './scheduler.ts'

/**
 * The stream ended at `time` before producing a value for {@link next}.
 */
export interface Ended {
  /** Identifies this value as an Ended result. */
  readonly '@@type': 'Ended'
  /** The time at which the stream ended. */
  readonly time: Time
}

/**
 * Create the rejection value used when {@link next} sees completion before any event.
 *
 * @example
 * ```ts
 * import { ended } from '@algosail/plystream/future'
 *
 * ended(20).time // => 20
 * ```
 */
export function ended(time: Time): Ended {
  return { '@@type': 'Ended', time }
}

/**
 * Check whether a value has the `Ended` tag.
 *
 * @example
 * ```ts
 * import { ended, isEnded } from '@algosail/plystream/future'
 *
 * isEnded(ended(0)) // => true
 * isEnded('nope') // => false
 * ```
 */
export function isEnded(value: unknown): value is Ended {
  return typeof value === 'object' && value !== null &&
    (value as { '@@type'?: unknown })['@@type'] === 'Ended'
}

/**
 * Run a Future on each subscription and emit its result as `Right(value)` or `Left(error)`.
 * Both outcomes end the stream normally. Disposal stops delivery, not the underlying task.
 *
 * @example
 * ```ts
 * import * as P from '@algosail/ply'
 * import * as F from '@algosail/plyfuture'
 * import * as S from '@algosail/plystream'
 * import { fromFuture } from '@algosail/plystream/future'
 *
 * const failed = await S.collect(fromFuture(F.rejected<string, number>('nope')))
 * failed.map(P.show) // => [ 'Left ("nope")' ]
 * ```
 */
export function fromFuture<E, A>(f: Future<E, A>): Stream<Either<E, A>> {
  return stream<Either<E, A>>((snk, sch) => {
    let active = true
    let settled = false
    let scheduled = disposeNone()

    const settle = (e: Either<E, A>) => {
      if (settled || !active) return
      settled = true
      scheduled = sch.asap(task((t) => {
        if (!active) return
        active = false
        snk.event(t, e)
        snk.end(t)
      }))
    }

    f.fork((reason) => settle(left(reason)), (value) => settle(right(value)))

    return disposable(() => {
      active = false
      dispose(scheduled)
    })
  })
}

/**
 * Create a lazy Future for the first stream event, then stop its subscription.
 * Reject with {@link Ended} if the stream ends first. A stream that never emits or ends leaves it pending.
 *
 * @example
 * ```ts
 * import * as F from '@algosail/plyfuture'
 * import * as S from '@algosail/plystream'
 * import { next } from '@algosail/plystream/future'
 *
 * const answer = next(S.defaultScheduler())(S.at(5)('hello'))
 * await F.promise(answer) // => 'hello'
 * ```
 */
export function next(
  scheduler: Scheduler = defaultScheduler(),
): <A>(strm: Stream<A>) => Future<Ended, A> {
  return <A>(strm: Stream<A>): Future<Ended, A> =>
    future<Ended, A>((onRejected, onResolved) => {
      let settled = false
      let d: Disposable | null = null

      const done = (f: () => void) => {
        if (settled) return
        settled = true
        if (d !== null) dispose(d)
        f()
      }

      d = strm.run(
        sink<A>(
          (_t, value) => done(() => onResolved(value)),
          (t) => done(() => onRejected(ended(t))),
        ),
        scheduler,
      )

      if (settled && d !== null) dispose(d)
    })
}

/**
 * Follow the initial behavior until a Future resolves with its replacement.
 * Start the task immediately; rejection leaves the initial behavior in place.
 *
 * @example
 * ```ts
 * import * as F from '@algosail/plyfuture'
 * import * as S from '@algosail/plystream'
 * import { switchTo } from '@algosail/plystream/future'
 *
 * const [sch, advanceTo] = S.newVirtualScheduler()
 * const loaded = F.resolved<string, S.Behavior<string>>(S.constant('ready'))
 * const status = switchTo(S.constant('loading'), sch)(loaded)
 *
 * S.sample(status) // => 'loading'
 * advanceTo(0)
 * S.sample(status) // => 'ready'
 * ```
 */
export function switchTo<A>(
  init: Behavior<A>,
  scheduler: Scheduler = defaultScheduler(),
): <E>(f: Future<E, Behavior<A>>) => Behavior<A> {
  return <E>(f: Future<E, Behavior<A>>) => {
    const arrivals = stream<Behavior<A>>((snk, sch) =>
      fromFuture(f).run(
        sink<Either<E, Behavior<A>>>(
          (t, e) => e.match(() => {}, (b: Behavior<A>) => snk.event(t, b)),
          snk.end,
        ),
        sch,
      )
    )
    return switcher(init, scheduler)(arrivals)
  }
}

function forkOnce<E, A>(
  f: Future<E, A>,
  onSettle: (e: Either<E, A>) => void,
): Disposable {
  let wanted = true
  const settle = (e: Either<E, A>) => {
    if (!wanted) return
    wanted = false
    onSettle(e)
  }
  f.fork((reason) => settle(left(reason)), (value) => settle(right(value)))
  return disposable(() => (wanted = false))
}

/**
 * Run arriving Futures concurrently and emit their Either results in completion order.
 * Wait for all tasks before ending. Disposal stops delivery, not the tasks.
 *
 * @example
 * ```ts
 * import * as P from '@algosail/ply'
 * import * as F from '@algosail/plyfuture'
 * import * as S from '@algosail/plystream'
 *
 * import { flatFutures } from '@algosail/plystream/future'
 *
 * const tasks = S.fromIterable([
 *   F.after(20)<string, number>(1),
 *   F.resolved<string, number>(2),
 * ])
 * const results = await S.collect(flatFutures(tasks))
 * results.map(P.show) // => ['Right (2)', 'Right (1)']
 * ```
 */
export function flatFutures<E, A>(
  strm: Stream<Future<E, A>>,
): Stream<Either<E, A>> {
  return stream<Either<E, A>>((snk, sch) => {
    let outerDone = false
    let inFlight = 0
    let ended = false
    const forks = new Set<Disposable>()

    const finish = (t: Time) => {
      if (ended || !outerDone || inFlight > 0) return
      ended = true
      snk.end(t)
    }

    const deliver = (e: Either<E, A>) =>
      sch.asap(task((t) => {
        inFlight -= 1
        if (ended) return
        snk.event(t, e)
        finish(t)
      }))

    const outer = strm.run(
      sink<Future<E, A>>((_t, f) => {
        if (ended) return
        inFlight += 1
        forks.add(forkOnce(f, deliver))
      }, () => {
        outerDone = true
        sch.asap(task(finish))
      }),
      sch,
    )

    return disposable(() => {
      if (ended) return
      ended = true
      dispose(outer)
      forks.forEach((d) => dispose(d))
      forks.clear()
    })
  })
}

/**
 * Run arriving Futures concurrently but emit their Either results in input order.
 * Later results wait for earlier tasks. A rejection is a Left value and does not stop the others.
 *
 * @example
 * ```ts
 * import * as P from '@algosail/ply'
 * import * as F from '@algosail/plyfuture'
 * import * as S from '@algosail/plystream'
 *
 * import { flatFuturesOrdered } from '@algosail/plystream/future'
 *
 * const tasks = S.fromIterable([
 *   F.after(20)<string, number>(1),
 *   F.resolved<string, number>(2),
 * ])
 * const results = await S.collect(flatFuturesOrdered(tasks))
 * results.map(P.show) // => ['Right (1)', 'Right (2)']
 * ```
 */
export function flatFuturesOrdered<E, A>(
  strm: Stream<Future<E, A>>,
): Stream<Either<E, A>> {
  return stream<Either<E, A>>((snk, sch) => {
    type Slot = { settled: boolean; value: Either<E, A> | null }
    const queue: Slot[] = []
    const forks = new Set<Disposable>()
    let outerDone = false
    let ended = false

    const finish = (t: Time) => {
      if (ended || !outerDone || queue.length > 0) return
      ended = true
      snk.end(t)
    }

    const drain = (t: Time) => {
      if (ended) return
      while (queue.length > 0 && queue[0].settled) {
        const slot = queue.shift() as Slot
        snk.event(t, slot.value as Either<E, A>)
      }
      finish(t)
    }

    const outer = strm.run(
      sink<Future<E, A>>((_t, f) => {
        if (ended) return
        const slot: Slot = { settled: false, value: null }
        queue.push(slot)
        forks.add(forkOnce(f, (e) => {
          slot.settled = true
          slot.value = e
          sch.asap(task(drain))
        }))
      }, () => {
        outerDone = true
        sch.asap(task(finish))
      }),
      sch,
    )

    return disposable(() => {
      if (ended) return
      ended = true
      dispose(outer)
      forks.forEach((d) => dispose(d))
      forks.clear()
      queue.length = 0
    })
  })
}

/**
 * Emit results only from the newest arriving Future. Older tasks continue but their results are ignored.
 * After the source ends, wait for the current task before ending.
 *
 * @example
 * ```ts
 * import * as P from '@algosail/ply'
 * import * as F from '@algosail/plyfuture'
 * import * as S from '@algosail/plystream'
 *
 * import { flatFuturesLatest } from '@algosail/plystream/future'
 *
 * const tasks = S.fromIterable([
 *   F.after(20)<string, number>(1),
 *   F.resolved<string, number>(2),
 * ])
 * const results = await S.collect(flatFuturesLatest(tasks))
 * results.map(P.show) // => ['Right (2)']
 * ```
 */
export function flatFuturesLatest<E, A>(
  strm: Stream<Future<E, A>>,
): Stream<Either<E, A>> {
  return stream<Either<E, A>>((snk, sch) => {
    let current = disposeNone()
    let pendingEmit = disposeNone()
    let waiting = false
    let outerDone = false
    let ended = false

    const finish = (t: Time) => {
      if (ended || !outerDone || waiting) return
      ended = true
      snk.end(t)
    }

    const outer = strm.run(
      sink<Future<E, A>>((_t, f) => {
        if (ended) return
        dispose(current)
        dispose(pendingEmit)
        waiting = true
        current = forkOnce(f, (e) => {
          pendingEmit = sch.asap(task((t) => {
            waiting = false
            if (ended) return
            snk.event(t, e)
            finish(t)
          }))
        })
      }, () => {
        outerDone = true
        sch.asap(task(finish))
      }),
      sch,
    )

    return disposable(() => {
      if (ended) return
      ended = true
      dispose(outer)
      dispose(current)
      dispose(pendingEmit)
    })
  })
}
