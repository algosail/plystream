/**
 * Create, transform, and subscribe to timed event streams.
 * Use {@link simulate} for virtual time or {@link collect} to wait for completion.
 * @module
 */

import type {
  AltMethods,
  FilterableMethods,
  FunctorMethods,
  MonoidTypeRep,
  PlusTypeRep,
  Satisfies,
  SemigroupMethods,
  Shape,
  Shaped,
  ShowMethods,
} from '@algosail/ply/define'
import type { Either } from '@algosail/ply/either'
import type { Maybe } from '@algosail/ply/maybe'
import type { Scheduler, Time } from './scheduler.ts'
import { finiteFrom, integer } from '@algosail/ply/define'
import { left, right } from '@algosail/ply/either'
import { just, nothing } from '@algosail/ply/maybe'
import { disposable, dispose, disposeNone } from './disposable.ts'
import { defaultScheduler, newVirtualScheduler, task } from './scheduler.ts'
import { batch } from './transaction.ts'

/**
 * Handlers for timed events and normal completion.
 */
export interface Sink<A> {
  /** Receive a value with its event time. */
  readonly event: (time: Time, value: A) => void
  /** Receive normal completion with its time. */
  readonly end: (time: Time) => void
}

/**
 * A subscribable sequence of timed values. `map` and `filter` preserve event times;
 * `concat` and `alt` merge concurrent sources.
 */
export interface Stream<A> extends StreamMethods<A> {
  /** Start listening with a sink and optional scheduler. Dispose the handle to stop. */
  readonly run: (sink: Sink<A>, scheduler?: Scheduler) => Disposable
}

/**
 * A subscribable sequence of timed values. `map` and `filter` preserve event times;
 * `concat` and `alt` merge concurrent sources.
 */
/**
 * Type mapping for using Stream with generic ply operations.
 */
export interface StreamShape extends Shape<'Stream'> {
  /** The Stream type for the supplied value slot. */
  readonly out: Stream<this['slotA']>
}

/**
 * The Stream representative accepted by generic ply operations.
 */
export type StreamTypeRep = Satisfies<
  StreamStatics & Shaped<StreamShape>,
  & PlusTypeRep<StreamShape>
  & MonoidTypeRep<StreamShape>
>

interface StreamStatics {
  empty<A>(): Stream<A>
  zero<A>(): Stream<A>
}

interface StreamMethods<A>
  extends
    ShowMethods<Stream<A>, A>,
    FunctorMethods<Stream<A>, A>,
    FilterableMethods<Stream<A>, A>,
    SemigroupMethods<Stream<A>, A>,
    AltMethods<Stream<A>, A> {
  readonly '@@type': 'Stream'
  readonly _shape: StreamShape
  readonly _A?: (_: never) => A
  readonly constructor: StreamTypeRep
}

type StreamProto = Omit<
  StreamMethods<unknown>,
  '_shape' | '_A' | 'constructor'
>

const proto: StreamProto = {
  '@@type': 'Stream' as const,

  map<A, B>(this: Stream<A>, f: (a: A) => B): Stream<B> {
    return stream<B>((snk, sch) =>
      this.run({ event: (t, a) => snk.event(t, f(a)), end: snk.end }, sch)
    )
  },

  filter<A>(this: Stream<A>, p: (a: A) => boolean): Stream<A> {
    return stream<A>((snk, sch) =>
      this.run({
        event: (t, a) => {
          if (p(a)) snk.event(t, a)
        },
        end: snk.end,
      }, sch)
    )
  },

  concat<A>(this: Stream<A>, that: Stream<A>): Stream<A> {
    return mergeArray<A>([this, that])
  },

  alt<A>(this: Stream<A>, that: Stream<A>): Stream<A> {
    return mergeArray<A>([this, that])
  },

  show(): string {
    return 'Stream'
  },
}

/**
 * Create a stream from a subscription function. Each run calls it again.
 * Send events and completion to the sink; return a handle that stops your source.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function run(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
 *   function send(time: S.Time): void {
 *     sink.event(time, 1)
 *     sink.end(time)
 *   }
 *   return scheduler.asap(S.task(send))
 * }
 *
 * S.simulate(0)(S.stream(run)) // => [[0, 1]]
 * ```
 */
export function stream<A>(
  run: (sink: Sink<A>, scheduler: Scheduler) => Disposable,
): Stream<A> {
  return Object.assign(Object.create(proto) as Stream<A>, {
    run: (snk: Sink<A>, sch: Scheduler = defaultScheduler()) => run(snk, sch),
  })
}

function endAt<A>(): Stream<A> {
  return stream<A>((snk, sch) => sch.asap(task((t) => snk.end(t))))
}

/**
 * Create an empty stream with `Stream.empty()` or `Stream.zero()`. Both end without emitting.
 */
export const Stream: StreamTypeRep = {
  '@@type': 'Stream' as const,
  _shape: undefined as unknown as StreamShape,

  empty() {
    return endAt()
  },

  zero() {
    return endAt()
  },
}

Object.defineProperty(proto, 'constructor', { value: Stream })

/**
 * Check whether a value is a Stream created by this package.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.isStream(S.wrap(1)) // => true
 * S.isStream(1) // => false
 * ```
 */
export function isStream(value: unknown): value is Stream<unknown> {
  return typeof value === 'object' && value !== null &&
    Object.getPrototypeOf(value) === proto
}

/**
 * Create event and completion handlers for running a stream.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const events: string[] = []
 * function onEvent(time: S.Time, value: string): void {
 *   events.push(`${time}: ${value}`)
 * }
 * function onEnd(time: S.Time): void {
 *   events.push(`${time}: end`)
 * }
 * const [scheduler, advanceTo] = S.newVirtualScheduler()
 * S.wrap('hello').run(S.sink(onEvent, onEnd), scheduler)
 * advanceTo(0)
 * // events => ['0: hello', '0: end']
 * ```
 */
export function sink<A>(
  event: (time: Time, value: A) => void,
  end: (time: Time) => void,
): Sink<A> {
  return { event, end }
}

/**
 * Check whether a value has `event` and `end` properties.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.isSink(S.sink((_t: number, x: number) => x, () => {})) // => true
 * ```
 */
export function isSink(value: unknown): value is Sink<unknown> {
  return (
    value !== null && value !== undefined && typeof value === 'object' &&
    'event' in value && 'end' in value
  )
}

/**
 * Start a stream and receive each event with its time. Dispose the returned handle to stop listening.
 * Uses the default scheduler unless you supply one.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch, tick] = S.newVirtualScheduler()
 * const seen: number[] = []
 * S.subscribe((_t: number, v: number) => seen.push(v), undefined, sch)(S.wrap(42))
 * tick(0)
 * seen // => [ 42 ]
 * ```
 */
export function subscribe<A>(
  onEvent: (time: Time, value: A) => void,
  onEnd: (time: Time) => void = () => {},
  scheduler: Scheduler = defaultScheduler(),
): (strm: Stream<A>) => Disposable {
  return (strm) => strm.run(sink(onEvent, onEnd), scheduler)
}

/**
 * Run a stream on a fresh virtual clock and return `[time, value]` pairs through the horizon.
 * Use for scheduler-driven streams; this does not wait for Promises or external events.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.simulate(25)(S.periodic(10)) // => [ [ 10, 10 ], [ 20, 20 ] ]
 * S.simulate(0)(S.fromIterable([1, 2])) // => [ [ 0, 1 ], [ 0, 2 ] ]
 * ```
 */
export function simulate(
  horizon: Time,
): <A>(strm: Stream<A>) => [Time, A][] {
  return <A>(strm: Stream<A>): [Time, A][] => {
    const [sch, tick] = newVirtualScheduler()
    const out: [Time, A][] = []
    strm.run(sink<A>((t, v) => out.push([t, v]), () => {}), sch)
    tick(horizon)
    return out
  }
}

/**
 * Run a stream on a virtual clock through the horizon and return only its values.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.fromIterable([1, 2, 3])) // => [ 1, 2, 3 ]
 * ```
 */
export function values(horizon: Time): <A>(strm: Stream<A>) => A[] {
  return <A>(strm: Stream<A>): A[] => simulate(horizon)(strm).map(([, v]) => v)
}

/**
 * Start a stream on the default scheduler and collect its values until it ends.
 * Limit infinite streams with {@link take} before collecting.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * await S.collect(S.fromIterable([1, 2, 3])) // => [ 1, 2, 3 ]
 * ```
 */
export function collect<A>(strm: Stream<A>): Promise<A[]> {
  return new Promise<A[]>((resolve) => {
    const out: A[] = []
    strm.run(
      sink<A>((_t, v) => out.push(v), () => resolve(out)),
      defaultScheduler(),
    )
  })
}

/**
 * Create a stream that emits nothing and never ends.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.simulate(100)(S.never<number>()) // => []
 * ```
 */
export function never<A>(): Stream<A> {
  return stream<A>(() => disposeNone())
}

/**
 * Emit one value after a finite, nonnegative delay in milliseconds, then end.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.simulate(30)(S.at(20)('x')) // => [ [ 20, "x" ] ]
 * ```
 */
export function at(time: Time): <A>(value: A) => Stream<A> {
  finiteFrom('at', 0, time)
  return <A>(value: A): Stream<A> =>
    stream<A>((snk, sch) =>
      sch.delay(time, task((t) => (snk.event(t, value), snk.end(t))))
    )
}

/**
 * Schedule one value at the current time, then end.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.simulate(0)(S.wrap(42)) // => [ [ 0, 42 ] ]
 * ```
 */
export function wrap<A>(value: A): Stream<A> {
  return stream<A>((snk, sch) =>
    sch.asap(task((t) => (snk.event(t, value), snk.end(t))))
  )
}

/**
 * Emit the iterable in order at one scheduled time, then end.
 * Use a reusable iterable if the stream will run more than once.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.simulate(0)(S.fromIterable([1, 2])) // => [ [ 0, 1 ], [ 0, 2 ] ]
 * ```
 */
export function fromIterable<A>(iterable: Iterable<A>): Stream<A> {
  return stream<A>((snk, sch) => {
    let active = true
    const d = sch.asap(task((t) => {
      for (const value of iterable) {
        if (!active) return
        snk.event(t, value)
      }
      if (active) snk.end(t)
    }))
    return disposable(() => {
      active = false
      dispose(d)
    })
  })
}

/**
 * Emit a Promise result as `Right(value)` or `Left(reason)`, then end normally.
 * The Promise is already created; subscribing again observes the same Promise.
 *
 * @example
 * ```ts
 * import * as P from '@algosail/ply'
 * import * as S from '@algosail/plystream'
 *
 * const got = await S.collect(S.fromPromise(Promise.resolve(42)))
 * got.map(P.show) // => [ "Right (42)" ]
 * ```
 */
export function fromPromise<A>(
  promise: Promise<A>,
): Stream<Either<unknown, A>> {
  return stream<Either<unknown, A>>((snk, sch) => {
    let active = true
    let settled = false
    let scheduled = disposeNone()

    const settle = (e: Either<unknown, A>) => {
      if (settled || !active) return
      settled = true
      scheduled = sch.asap(task((t) => {
        if (!active) return
        active = false
        snk.event(t, e)
        snk.end(t)
      }))
    }

    promise.then(
      (value) => settle(right(value)),
      (reason) => settle(left(reason)),
    )

    return disposable(() => {
      active = false
      dispose(scheduled)
    })
  })
}

/**
 * Emit elapsed milliseconds at each interval, starting after the first interval.
 * Use a positive finite period for repeating events; zero schedules one event without ending.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(30)(S.periodic(10)) // => [ 10, 20, 30 ]
 * ```
 */
export function periodic(period: number): Stream<Time> {
  finiteFrom('periodic', 0, period)
  return stream<Time>((snk, sch) => {
    const origin = sch.currentTime()
    return sch.periodic(period, task((t) => snk.event(t, t - origin)))
  })
}

function* generateRange(
  count: number,
  start: number,
  step: number,
): Generator<number> {
  const length = Math.max(0, Math.floor(count))
  let value = start
  for (let i = 0; i < length; i++) {
    yield value
    value += step
  }
}

/**
 * Emit `count` numbers, starting at `start` and adding `step` each time.
 * The arguments are `(step, start, count)`. Supply a finite count or limit with {@link take}.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.range(2, 0, 4)) // => [ 0, 2, 4, 6 ]
 * ```
 */
export function range(
  step = 1,
  start = 0,
  count = Number.POSITIVE_INFINITY,
): Stream<number> {
  finiteFrom('range', -Infinity, step)
  finiteFrom('range', -Infinity, start)
  if (count !== Number.POSITIVE_INFINITY) integer('range', count)
  return fromIterable(generateRange(count, start, step))
}

/**
 * Repeat a value at the current scheduled time. Limit with {@link take} to stop it.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.take(3)(S.repeat('x'))) // => [ "x", "x", "x" ]
 * ```
 */
export function repeat<A>(value: A): Stream<A> {
  return stream<A>((snk, sch) => {
    let active = true
    let d = disposeNone()
    const pump = task((t) => {
      if (!active) return
      snk.event(t, value)
      if (active) d = sch.asap(pump)
    })
    d = sch.asap(pump)
    return disposable(() => {
      active = false
      dispose(d)
    })
  })
}

/**
 * Subscribe to every stream and emit values as they arrive. End when all sources end.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.simulate(40)(S.mergeArray([S.at(30)('slow'), S.at(10)('fast')]))
 * // => [ [ 10, "fast" ], [ 30, "slow" ] ]
 * ```
 */
export function mergeArray<A>(streams: readonly Stream<A>[]): Stream<A> {
  return stream<A>((snk, sch) => {
    if (streams.length === 0) return endAt<A>().run(snk, sch)

    let open = streams.length
    let ended = false
    const dsps: Disposable[] = []

    for (const strm of streams) {
      if (ended) break
      let done = false
      const onEnd = (t: Time) => {
        if (done || ended) return
        done = true
        if (--open > 0) return
        ended = true
        snk.end(t)
      }
      dsps.push(strm.run(sink<A>(snk.event, onEnd), sch))
    }

    return disposable(() => {
      if (ended) return
      ended = true
      dsps.forEach((d) => dispose(d))
    })
  })
}

/**
 * Listen to both streams together and emit values in arrival order.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(40)(S.merge(S.at(30)('slow'))(S.at(10)('fast')))
 * // => [ "fast", "slow" ]
 * ```
 */
export function merge<A>(a: Stream<A>): <B>(b: Stream<B>) => Stream<A | B> {
  return <B>(b: Stream<B>): Stream<A | B> => mergeArray<A | B>([a, b])
}

/**
 * Add an origin to event and completion times before forwarding them to a sink.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const seen: number[] = []
 * S.relativeSink(100, S.sink((t: number) => seen.push(t), () => {})).event(5, 'x')
 * seen // => [ 105 ]
 * ```
 */
export function relativeSink<A>(origin: Time, snk: Sink<A>): Sink<A> {
  return {
    event: (t, value) => snk.event(t + origin, value),
    end: (t) => snk.end(t + origin),
  }
}

/**
 * Start the next stream when the source ends. Its delays begin at that completion time.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function next(): S.Stream<string> {
 *   return S.at(10)('b')
 * }
 * S.simulate(40)(S.continueWith(next)(S.at(20)('a')))
 * // => [[20, 'a'], [30, 'b']]
 * ```
 */
export function continueWith<B>(
  next: () => Stream<B>,
): <A>(strm: Stream<A>) => Stream<A | B> {
  return <A>(strm: Stream<A>): Stream<A | B> =>
    stream<A | B>((snk, sch) => {
      let second: Disposable | null = null
      let disposed = false

      const first = strm.run(
        sink<A>(snk.event, (t) => {
          if (disposed) return
          second = next().run(relativeSink(t, snk), sch.relative(t))
        }),
        sch,
      )

      return disposable(() => {
        disposed = true
        dispose(first)
        if (second !== null) dispose(second)
      })
    })
}

/**
 * Deliver every event and completion after a finite, nonnegative delay in milliseconds.
 * The source starts immediately.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.simulate(40)(S.delay(15)(S.at(10)('x'))) // => [ [ 25, "x" ] ]
 * ```
 */
export function delay(ms: number): <A>(strm: Stream<A>) => Stream<A> {
  finiteFrom('delay', 0, ms)
  return <A>(strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      const scheduled: Disposable[] = []
      const later = (f: (t: Time) => void) =>
        scheduled.push(sch.delay(ms, task(f)))

      const d = strm.run(
        sink<A>(
          (_t, v) => later((t) => snk.event(t, v)),
          () => later((t) => snk.end(t)),
        ),
        sch,
      )

      return disposable(() => {
        dispose(d)
        scheduled.forEach((s) => dispose(s))
      })
    })
}

/**
 * Emit the updated accumulator after each event. The seed itself is not emitted.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function add(total: number, value: number): number {
 *   return total + value
 * }
 *
 * const source = S.fromIterable([1, 2, 3])
 * S.values(0)(S.scan(add, 0)(source)) // => [1, 3, 6]
 * ```
 */
export function scan<Acc, A>(
  scanner: (acc: Acc, a: A) => Acc,
  seed: Acc,
): (strm: Stream<A>) => Stream<Acc> {
  return (strm: Stream<A>): Stream<Acc> =>
    stream<Acc>((snk, sch) => {
      let state = seed
      return strm.run(
        sink<A>((t, a) => {
          state = scanner(state, a)
          snk.event(t, state)
        }, snk.end),
        sch,
      )
    })
}

/**
 * Keep state while transforming events. Return `[nextState, output]` from the callback.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function update(total: number, value: number): [number, number] {
 *   return [total + value, total * value]
 * }
 *
 * const source = S.fromIterable([1, 2, 3])
 * S.values(0)(S.mapAccum(update, 1)(source)) // => [1, 4, 12]
 * ```
 */
export function mapAccum<S, A, B>(
  step: (state: S, a: A) => [S, B],
  seed: S,
): (strm: Stream<A>) => Stream<B> {
  return (strm: Stream<A>): Stream<B> =>
    stream<B>((snk, sch) => {
      let state = seed
      return strm.run(
        sink<A>((t, a) => {
          const [next, value] = step(state, a)
          state = next
          snk.event(t, value)
        }, snk.end),
        sch,
      )
    })
}

/**
 * Skip values that compare equal to the last emitted value.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function equal(total: number, value: number): boolean {
 *   return total === value
 * }
 *
 * const source = S.fromIterable([1, 1, 2, 2, 3])
 * S.values(0)(S.distinct(equal)(source)) // => [1, 2, 3]
 * ```
 */
export function distinct<A>(
  compare: (a: A, b: A) => boolean,
): (strm: Stream<A>) => Stream<A> {
  return (strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      let last: Maybe<A> = nothing()
      return strm.run(
        sink<A>((t, a) => {
          if (last.tag === 'nothing' || !compare(last.value, a)) {
            last = just(a)
            snk.event(t, a)
          }
        }, snk.end),
        sch,
      )
    })
}

/**
 * Pair each value with an index. Defaults to starting at zero and increasing by one.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.withIndex(0, 1)(S.fromIterable(['a', 'b'])))
 * // => [ [ 0, "a" ], [ 1, "b" ] ]
 * ```
 */
export function withIndex(
  start = 0,
  step = 1,
): <A>(strm: Stream<A>) => Stream<[number, A]> {
  return <A>(strm: Stream<A>): Stream<[number, A]> =>
    mapAccum<number, A, [number, A]>((i, v) => [i + step, [i, v]], start)(strm)
}

/**
 * Pair each value with its arrival count, starting at one.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.withCount(S.fromIterable(['a', 'b'])))
 * // => [ [ 1, "a" ], [ 2, "b" ] ]
 * ```
 */
export function withCount<A>(strm: Stream<A>): Stream<[number, A]> {
  return withIndex(1)(strm)
}

/**
 * Emit the number of events received so far, starting at one.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.count(S.fromIterable(['a', 'b', 'c']))) // => [ 1, 2, 3 ]
 * ```
 */
export function count<A>(strm: Stream<A>): Stream<number> {
  return withCount(strm).map(([i]) => i)
}

/**
 * Keep the first `n` events, then stop the source. Nonpositive integers produce an empty stream.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.take(2)(S.fromIterable([1, 2, 3, 4]))) // => [ 1, 2 ]
 * S.values(0)(S.take(0)(S.fromIterable([1, 2, 3]))) // => []
 * ```
 */
export function take(n: number): <A>(strm: Stream<A>) => Stream<A> {
  integer('take', n)
  return <A>(strm: Stream<A>): Stream<A> => {
    if (n <= 0) return Stream.empty<A>()

    return stream<A>((snk, sch) => {
      let remaining = n
      let done = false
      let d = disposeNone()

      const onEvent = (t: Time, a: A) => {
        if (done) return
        snk.event(t, a)
        if (--remaining <= 0) {
          done = true
          dispose(d)
          snk.end(t)
        }
      }

      const onEnd = (t: Time) => {
        if (done) return
        done = true
        snk.end(t)
      }

      d = strm.run(sink(onEvent, onEnd), sch)
      if (done) dispose(d)

      return disposable(() => {
        if (done) return
        done = true
        dispose(d)
      })
    })
  }
}

/**
 * Keep events through the first value that satisfies the predicate, including that value.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function reachedThree(value: number): boolean {
 *   return value >= 3
 * }
 * const source = S.fromIterable([1, 2, 3, 4])
 * S.values(60)(S.takeUntil(reachedThree)(source)) // => [1, 2, 3]
 * ```
 */
export function takeUntil<A>(
  predicate: (a: A) => boolean,
): (strm: Stream<A>) => Stream<A> {
  return (strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      let done = false
      let d = disposeNone()

      const onEvent = (t: Time, a: A) => {
        if (done) return
        snk.event(t, a)
        if (predicate(a)) {
          done = true
          dispose(d)
          snk.end(t)
        }
      }

      const onEnd = (t: Time) => {
        if (done) return
        done = true
        snk.end(t)
      }

      d = strm.run(sink(onEvent, onEnd), sch)
      if (done) dispose(d)

      return disposable(() => {
        if (done) return
        done = true
        dispose(d)
      })
    })
}

/**
 * Keep events while the predicate holds. Stop before the first rejected value.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function belowThree(value: number): boolean {
 *   return value < 3
 * }
 * const source = S.fromIterable([1, 2, 3, 4])
 * S.values(60)(S.takeWhile(belowThree)(source)) // => [1, 2]
 * ```
 */
export function takeWhile<A>(
  predicate: (a: A) => boolean,
): (strm: Stream<A>) => Stream<A> {
  return (strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      let done = false
      let d = disposeNone()

      const onEvent = (t: Time, a: A) => {
        if (done) return
        if (predicate(a)) {
          snk.event(t, a)
          return
        }
        done = true
        dispose(d)
        snk.end(t)
      }

      const onEnd = (t: Time) => {
        if (done) return
        done = true
        snk.end(t)
      }

      d = strm.run(sink(onEvent, onEnd), sch)
      if (done) dispose(d)

      return disposable(() => {
        if (done) return
        done = true
        dispose(d)
      })
    })
}

/**
 * Drop the first `n` events. Nonpositive integers leave all values in place.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.skip(2)(S.fromIterable([1, 2, 3, 4]))) // => [ 3, 4 ]
 * ```
 */
export function skip(n: number): <A>(strm: Stream<A>) => Stream<A> {
  integer('skip', n)
  return <A>(strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      let remaining = Math.max(0, n)
      return strm.run(
        sink<A>((t, a) => {
          if (remaining > 0) {
            remaining -= 1
            return
          }
          snk.event(t, a)
        }, snk.end),
        sch,
      )
    })
}

/**
 * Keep events from the zero-based start index up to the exclusive end index.
 * Omit the end to keep all remaining events.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.slice(1, 3)(S.fromIterable([1, 2, 3, 4, 5]))) // => [ 2, 3 ]
 * S.values(0)(S.slice(3)(S.fromIterable([1, 2, 3, 4, 5]))) // => [ 4, 5 ]
 * ```
 */
export function slice(
  start: number,
  end = Number.POSITIVE_INFINITY,
): <A>(strm: Stream<A>) => Stream<A> {
  integer('slice', start)
  if (end !== Number.POSITIVE_INFINITY) integer('slice', end)
  const from = Math.max(0, start)
  const to = Math.max(from, end)
  return <A>(strm: Stream<A>): Stream<A> => {
    if (to === from) return Stream.empty<A>()
    const rest = from === 0 ? strm : skip(from)(strm)
    return to === Number.POSITIVE_INFINITY ? rest : take(to - from)(rest)
  }
}

/**
 * Stop when the signal emits its first event. The signal value is discarded.
 * A signal that ends without emitting does not stop the source.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(60)(S.until(S.at(35)('stop'))(S.periodic(10))) // => [ 10, 20, 30 ]
 * ```
 */
export function until<B>(
  signal: Stream<B>,
): <A>(strm: Stream<A>) => Stream<A> {
  return <A>(strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      let done = false
      let source = disposeNone()
      let watch = disposeNone()

      const finish = (t: Time) => {
        if (done) return
        done = true
        dispose(source)
        dispose(watch)
        snk.end(t)
      }

      source = strm.run(
        sink<A>((t, a) => {
          if (!done) snk.event(t, a)
        }, finish),
        sch,
      )
      if (done) {
        dispose(source)
        return disposeNone()
      }

      watch = signal.run(sink<B>((t) => finish(t), () => {}), sch)
      if (done) dispose(watch)

      return disposable(() => {
        if (done) return
        done = true
        dispose(source)
        dispose(watch)
      })
    })
}

/**
 * Emit the latest value after a quiet interval in milliseconds.
 * If the source ends while a value is waiting, emit it immediately before completion.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const typing = S.mergeArray([S.at(0)('a'), S.at(5)('ab'), S.at(40)('abc')])
 * S.simulate(80)(S.debounce(15)(typing))
 * // => [ [ 20, "ab" ], [ 40, "abc" ] ]
 * ```
 */
export function debounce(ms: number): <A>(strm: Stream<A>) => Stream<A> {
  finiteFrom('debounce', 0, ms)
  return <A>(strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      let pending = disposeNone()
      let waiting = false
      let held: A
      let ended = false

      const onEvent = (_t: Time, a: A) => {
        if (ended) return
        dispose(pending)
        held = a
        waiting = true
        pending = sch.delay(
          ms,
          task((at) => {
            waiting = false
            if (!ended) snk.event(at, held)
          }),
        )
      }

      const onEnd = (t: Time) => {
        if (ended) return
        ended = true
        dispose(pending)
        if (waiting) {
          waiting = false
          snk.event(t, held)
        }
        snk.end(t)
      }

      const d = strm.run(sink(onEvent, onEnd), sch)

      return disposable(() => {
        if (ended) return
        ended = true
        dispose(pending)
        dispose(d)
      })
    })
}

/**
 * Emit an event, then ignore events for the next `ms` milliseconds.
 * Events exactly at the next window boundary are included.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(50)(S.throttle(25)(S.periodic(10))) // => [ 10, 40 ]
 * ```
 */
export function throttle(ms: number): <A>(strm: Stream<A>) => Stream<A> {
  finiteFrom('throttle', 0, ms)
  return <A>(strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      let openAt = Number.NEGATIVE_INFINITY
      return strm.run(
        sink<A>((t, a) => {
          if (t < openAt) return
          openAt = t + ms
          snk.event(t, a)
        }, snk.end),
        sch,
      )
    })
}

/**
 * Emit a value synchronously when subscribed, then listen to the source.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.startWith(0)(S.wrap(1))) // => [ 0, 1 ]
 * ```
 */
export function startWith<A>(value: A): (strm: Stream<A>) => Stream<A> {
  return (strm: Stream<A>): Stream<A> =>
    stream<A>((snk, sch) => {
      snk.event(sch.currentTime(), value)
      return strm.run(snk, sch)
    })
}

/**
 * Choose whether excess inner streams wait (`hold`), replace the active stream (`swap`), or are ignored (`drop`).
 */
export type JoinStrategy = 'hold' | 'swap' | 'drop'

function join(
  concurrency: number,
  strategy: JoinStrategy,
): <A>(s: Stream<Stream<A>>) => Stream<A> {
  return <A>(s: Stream<Stream<A>>): Stream<A> =>
    stream<A>((outer, sch) => {
      let outerClosed = false
      let lastTime = 0
      const queue: Stream<A>[] = []
      const running = new Map<Sink<A>, Disposable>()

      const startInner = (strm: Stream<A>): void => {
        const onEnd = (t: Time) => {
          lastTime = Math.max(lastTime, t)
          running.delete(innerSnk)
          if (running.size < concurrency && queue.length > 0) {
            startInner(queue.shift() as Stream<A>)
          }
          if (outerClosed && running.size === 0) outer.end(lastTime)
        }

        const innerSnk = sink<A>(outer.event, onEnd)
        running.set(innerSnk, disposeNone())
        const d = strm.run(innerSnk, sch)
        if (running.has(innerSnk)) running.set(innerSnk, d)
        else dispose(d)
      }

      const onEvent = (t: Time, strm: Stream<A>): void => {
        lastTime = Math.max(lastTime, t)
        if (running.size < concurrency) return startInner(strm)

        if (strategy === 'hold') {
          queue.push(strm)
          return
        }

        if (strategy === 'swap') {
          queue.length = 0
          const oldest = running.entries().next()
          if (!oldest.done) {
            const [oldestSink, oldestDsp] = oldest.value
            running.delete(oldestSink)
            dispose(oldestDsp)
          }
          startInner(strm)
          return
        }

        // 'drop': the newcomer is discarded
      }

      const onEnd = (t: Time) => {
        lastTime = Math.max(lastTime, t)
        outerClosed = true
        if (running.size === 0 && queue.length === 0) outer.end(lastTime)
      }

      const d = s.run(sink(onEvent, onEnd), sch)

      return disposable(() => {
        queue.length = 0
        running.forEach((inner) => dispose(inner))
        running.clear()
        if (!outerClosed) dispose(d)
      })
    })
}

/**
 * Merge inner streams with at most `concurrency` active at once.
 * Extra streams wait in arrival order. Use a positive limit; the default is unlimited.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(0)(S.mergeConcurrently(2)(S.fromIterable([S.wrap(1), S.wrap(2)])))
 * // => [ 1, 2 ]
 * ```
 */
export function mergeConcurrently(
  concurrency = Number.POSITIVE_INFINITY,
): <A>(s: Stream<Stream<A>>) => Stream<A> {
  return join(concurrency, 'hold')
}

/**
 * Listen to the newest inner stream, disposing the previous subscription on each arrival.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(60)(S.switchLatest(S.fromIterable([S.at(30)('slow'), S.at(10)('fast')])))
 * // => [ "fast" ]
 * ```
 */
export function switchLatest<A>(s: Stream<Stream<A>>): Stream<A> {
  return join(1, 'swap')(s)
}

/**
 * Listen to one inner stream at a time. Discard new streams while it is running.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.values(60)(S.exhaustLatest(S.fromIterable([S.at(50)('first'), S.at(10)('second')])))
 * // => [ "first" ]
 * ```
 */
export function exhaustLatest<A>(s: Stream<Stream<A>>): Stream<A> {
  return join(1, 'drop')(s)
}

/**
 * Turn events into streams and merge their values. Optionally limit concurrent inner streams.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function double(value: number): S.Stream<number> {
 *   return S.wrap(value * 2)
 * }
 * const source = S.fromIterable([1, 2, 3])
 * S.values(60)(S.flatmap(double)(source)) // => [2, 4, 6]
 * ```
 */
export function flatmap<A, B>(
  fn: (a: A) => Stream<B>,
  concurrency = Number.POSITIVE_INFINITY,
): (strm: Stream<A>) => Stream<B> {
  return (strm: Stream<A>): Stream<B> =>
    mergeConcurrently(concurrency)(strm.map(fn))
}

/**
 * Turn each event into a stream and switch to it, disposing the previous subscription.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function arriveAfter(value: number): S.Stream<number> {
 *   return S.at(value)(value)
 * }
 * const source = S.fromIterable([30, 10])
 * S.values(60)(S.switchmap(arriveAfter)(source)) // => [10]
 * ```
 */
export function switchmap<A, B>(
  fn: (a: A) => Stream<B>,
): (strm: Stream<A>) => Stream<B> {
  return (strm: Stream<A>): Stream<B> => switchLatest(strm.map(fn))
}

/**
 * Turn events into streams; ignore new inner streams while one is running.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function arriveAfter(value: number): S.Stream<number> {
 *   return S.at(value)(value)
 * }
 * const source = S.fromIterable([50, 10])
 * S.values(60)(S.exhaustmap(arriveAfter)(source)) // => [50]
 * ```
 */
export function exhaustmap<A, B>(
  fn: (a: A) => Stream<B>,
): (strm: Stream<A>) => Stream<B> {
  return (strm: Stream<A>): Stream<B> => exhaustLatest(strm.map(fn))
}

/**
 * Combine pairs in arrival order, waiting for both values. End when no more pairs are possible.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function add(total: number, value: number): number {
 *   return total + value
 * }
 *
 * const first = S.fromIterable([1, 2, 3])
 * const second = S.fromIterable([10, 20])
 * S.values(0)(S.zipWith(add)(first)(second)) // => [11, 22]
 * ```
 */
export function zipWith<A, B, C>(
  fn: (a: A, b: B) => C,
): (strmA: Stream<A>) => (strmB: Stream<B>) => Stream<C> {
  return (strmA) => (strmB) =>
    stream<C>((snk, sch) => {
      let aDone = false
      let bDone = false
      const as: A[] = []
      const bs: B[] = []

      let ended = false
      let dspA = disposeNone()
      let dspB = disposeNone()

      const finish = (t: Time) => {
        if (ended) return
        if (!((aDone && as.length === 0) || (bDone && bs.length === 0))) return
        ended = true
        dispose(dspA)
        dispose(dspB)
        snk.end(t)
      }

      const send = (t: Time) => {
        while (as.length > 0 && bs.length > 0) {
          snk.event(t, fn(as.shift() as A, bs.shift() as B))
        }
        finish(t)
      }

      dspA = strmA.run(
        sink<A>(
          (t, a) => (as.push(a), send(t)),
          (t) => (aDone = true, finish(t)),
        ),
        sch,
      )
      if (ended) {
        dispose(dspA)
        return disposeNone()
      }

      dspB = strmB.run(
        sink<B>(
          (t, b) => (bs.push(b), send(t)),
          (t) => (bDone = true, finish(t)),
        ),
        sch,
      )
      if (ended) dispose(dspB)

      return disposable(() => {
        if (ended) return
        ended = true
        dispose(dspA)
        dispose(dspB)
      })
    })
}

function notify<A>(
  sinks: ReadonlyMap<Sink<A>, unknown>,
  deliver: (snk: Sink<A>) => void,
): void {
  batch(() => {
    for (const snk of [...sinks.keys()]) {
      if (sinks.has(snk)) deliver(snk)
    }
  })
}

/**
 * Share one source subscription among listeners. Past events are not replayed.
 * The last listener leaving stops the source; after completion, later listeners only receive the end.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * let runs = 0
 * function run(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
 *   runs += 1
 *   return S.wrap(1).run(sink, scheduler)
 * }
 * const shared = S.multicast(S.stream(run))
 * S.values(0)(S.merge(shared)(shared)) // => [1, 1]
 * // runs => 1
 * ```
 */
export function multicast<A>(strm: Stream<A>): Stream<A> {
  let sourceDsp = disposeNone()
  let sourceDone = false
  const sinks = new Map<Sink<A>, Disposable>()

  const broadcast = sink<A>(
    (t, value) => notify(sinks, (s) => s.event(t, value)),
    (t) => {
      sourceDone = true
      sourceDsp = disposeNone()
      notify(sinks, (s) => s.end(t))
      sinks.clear()
    },
  )

  return stream<A>((snk, sch) => {
    if (sourceDone) {
      snk.end(sch.currentTime())
      return disposeNone()
    }

    const existing = sinks.get(snk)
    if (existing !== undefined) return existing

    const d = disposable(() => {
      sinks.delete(snk)
      if (sinks.size === 0) {
        dispose(sourceDsp)
        sourceDsp = disposeNone()
      }
    })

    sinks.set(snk, d)
    if (sinks.size === 1) sourceDsp = strm.run(broadcast, sch)
    return d
  })
}

/**
 * Create a shared stream and a function that emits to its current listeners synchronously.
 * Past values are not replayed, and the stream does not end on its own.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch, tick] = S.newVirtualScheduler()
 * const [events, emit] = S.bus<string>()
 * const seen: string[] = []
 * S.subscribe((_t: number, v: string) => seen.push(v), undefined, sch)(events)
 * emit('a')
 * emit('b')
 * seen // => [ "a", "b" ]
 * ```
 */
export function bus<A>(): [Stream<A>, (value: A) => void] {
  const sinks = new Map<Sink<A>, Scheduler>()

  const emit = (value: A) =>
    notify(
      sinks,
      (s) => s.event((sinks.get(s) as Scheduler).currentTime(), value),
    )

  const strm = stream<A>((snk, sch) => {
    sinks.set(snk, sch)
    return disposable(() => sinks.delete(snk))
  })

  return [strm, emit]
}

/**
 * Split events into matching and nonmatching streams, sharing the source.
 * Subscribe to both branches before the source emits; past values are not replayed.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function isEven(value: number): boolean {
 *   return value % 2 === 0
 * }
 * const [source, emit] = S.bus<number>()
 * const [matching, other] = S.partition(isEven)(source)
 * const first: number[] = []
 * const second: number[] = []
 * S.subscribe((_time: S.Time, value: number) => first.push(value))(matching)
 * S.subscribe((_time: S.Time, value: number) => second.push(value))(other)
 * emit(1)
 * emit(2)
 * // first => [2]
 * // second => [1]
 * ```
 */
export function partition<A>(
  predicate: (a: A) => boolean,
): (strm: Stream<A>) => [Stream<A>, Stream<A>] {
  return (strm: Stream<A>): [Stream<A>, Stream<A>] => {
    const shared = multicast(strm)
    return [shared.filter(predicate), shared.filter((a: A) => !predicate(a))]
  }
}

const filterMapStream =
  <A, B>(f: (a: A) => Maybe<B>) => (strm: Stream<A>): Stream<B> =>
    stream<B>((snk, sch) =>
      strm.run(
        sink<A>(
          (t, a) => f(a).match(() => {}, (b) => snk.event(t, b)),
          snk.end,
        ),
        sch,
      )
    )

/**
 * Split transformed events into unwrapped Left and Right streams, sharing the source.
 * Subscribe to both branches before the source emits; past values are not replayed.
 *
 * @example
 * ```ts
 * import * as P from '@algosail/ply'
 * import * as S from '@algosail/plystream'
 *
 * function parse(text: string): P.Either<string, number> {
 *   const value = Number(text)
 *   return Number.isNaN(value) ? P.left(text) : P.right(value)
 * }
 * const [source, emit] = S.bus<string>()
 * const [matching, other] = S.partitionMap(parse)(source)
 * const first: string[] = []
 * const second: number[] = []
 * S.subscribe((_time: S.Time, value: string) => first.push(value))(matching)
 * S.subscribe((_time: S.Time, value: number) => second.push(value))(other)
 * emit('1')
 * emit('x')
 * // first => ['x']
 * // second => [1]
 * ```
 */
export function partitionMap<A, B, C>(
  split: (a: A) => Either<B, C>,
): (strm: Stream<A>) => [Stream<B>, Stream<C>] {
  return (strm: Stream<A>): [Stream<B>, Stream<C>] => {
    const shared = multicast(strm)
    const keepLeft = (a: A): Maybe<B> =>
      split(a).match((b: B) => just(b), (): Maybe<B> => nothing())
    const keepRight = (a: A): Maybe<C> =>
      split(a).match((): Maybe<C> => nothing(), (c: C) => just(c))
    return [
      filterMapStream(keepLeft)(shared),
      filterMapStream(keepRight)(shared),
    ]
  }
}
