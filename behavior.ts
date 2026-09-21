/**
 * Hold current values, observe updates, and combine state with event streams.
 * Start with {@link constant}, {@link stepper}, or {@link accum}.
 * @module
 */

import type {
  ApplyMethods,
  ChainMethods,
  ComonadMethods,
  ExtendMethods,
  FunctorMethods,
  MonadTypeRep,
  Satisfies,
  Shape,
  Shaped,
  ShowMethods,
} from '@algosail/ply/define'
import type { Scheduler, Time } from './scheduler.ts'
import type { Stream } from './stream.ts'
import { disposable, dispose, disposeNone } from './disposable.ts'
import { sink, stream } from './stream.ts'
import { defaultScheduler } from './scheduler.ts'
import {
  batch,
  cancelNotification,
  cell,
  notifyLater,
  touch,
} from './transaction.ts'

/**
 * Receive a behavior’s current value or an update.
 */
export type Observer<A> = (value: A) => void

/**
 * A value you can read now and observe over time.
 */
export interface Behavior<A> extends BehaviorMethods<A>, Disposable {
  /** Read the current value without registering an observer. */
  readonly sample: () => A
  /** Receive the current value immediately and subscribe to updates. */
  readonly observe: (o: Observer<A>) => Disposable
}

/**
 * Type mapping for using Behavior with generic ply operations.
 */
export interface BehaviorShape extends Shape<'Behavior'> {
  /** The Behavior type for the supplied value slot. */
  readonly out: Behavior<this['slotA']>
}

/**
 * The Behavior representative accepted by generic ply operations.
 */
export type BehaviorTypeRep = Satisfies<
  BehaviorStatics & Shaped<BehaviorShape>,
  MonadTypeRep<BehaviorShape>
>

interface BehaviorStatics {
  of<A>(a: A): Behavior<A>
}

interface BehaviorMethods<A>
  extends
    ShowMethods<Behavior<A>, A>,
    FunctorMethods<Behavior<A>, A>,
    ApplyMethods<Behavior<A>, A>,
    ChainMethods<Behavior<A>, A>,
    ExtendMethods<Behavior<A>, A>,
    ComonadMethods<Behavior<A>, A> {
  readonly '@@type': 'Behavior'
  readonly _shape: BehaviorShape
  readonly _A?: (_: never) => A
  readonly constructor: BehaviorTypeRep
}

type BehaviorProto = Omit<
  BehaviorMethods<unknown>,
  '_shape' | '_A' | 'constructor'
>

const proto: BehaviorProto = {
  '@@type': 'Behavior' as const,

  map<A, B>(this: Behavior<A>, f: (a: A) => B): Behavior<B> {
    const read = () => f(this.sample())
    return behavior<B>(read, (o) => {
      const notice = cell(() => o(read()))
      let live = false
      const d = this.observe(() => {
        if (live) notifyLater(notice)
      })
      live = true
      o(read())
      return disposable(() => {
        live = false
        cancelNotification(notice)
        dispose(d)
      })
    })
  },

  ap<A, B>(this: Behavior<A>, bf: Behavior<(a: A) => B>): Behavior<B> {
    const read = () => bf.sample()(this.sample())
    return behavior<B>(read, (o) => {
      const notice = cell(() => o(read()))
      let live = false

      const df = bf.observe(() => {
        if (live) notifyLater(notice)
      })
      const da = this.observe(() => {
        if (live) notifyLater(notice)
      })

      live = true
      o(read())

      return disposable(() => {
        live = false
        cancelNotification(notice)
        dispose(df)
        dispose(da)
      })
    })
  },

  chain<A, B>(this: Behavior<A>, f: (a: A) => Behavior<B>): Behavior<B> {
    const read = () => f(this.sample()).sample()
    return behavior<B>(read, (o) => {
      const notice = cell(() => o(read()))
      let inner = disposeNone()
      let live = false

      const outer = this.observe((a) => {
        dispose(inner)
        inner = f(a).observe(() => {
          if (live) notifyLater(notice)
        })
        if (live) notifyLater(notice)
      })

      live = true
      o(read())

      return disposable(() => {
        live = false
        cancelNotification(notice)
        dispose(outer)
        dispose(inner)
      })
    })
  },

  extract<A>(this: Behavior<A>): A {
    return this.sample()
  },

  extend<A, B>(this: Behavior<A>, f: (w: Behavior<A>) => B): Behavior<B> {
    const read = () => f(this)
    return behavior<B>(read, (o) => {
      const notice = cell(() => o(read()))
      let live = false
      const d = this.observe(() => {
        if (live) notifyLater(notice)
      })
      live = true
      o(read())
      return disposable(() => {
        live = false
        cancelNotification(notice)
        dispose(d)
      })
    })
  },

  show(): string {
    return 'Behavior'
  },
}

/**
 * Create a value that can be sampled and observed.
 * Your observer function must send the current value immediately and return a cleanup handle.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function read(): number {
 *   return 5
 * }
 * function observe(observer: S.Observer<number>): Disposable {
 *   observer(read())
 *   return S.disposeNone()
 * }
 * const five = S.behavior(read, observe)
 * S.sample(five) // => 5
 * ```
 */
export function behavior<A>(
  sample: () => A,
  observe: (o: Observer<A>) => Disposable,
  onDispose: () => void = () => {},
): Behavior<A> {
  return Object.assign(Object.create(proto) as Behavior<A>, {
    sample,
    observe,
    [Symbol.dispose]: onDispose,
  })
}

/**
 * Create a behavior whose current value never changes.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.sample(S.constant(42)) // => 42
 * ```
 */
export function constant<A>(value: A): Behavior<A> {
  return behavior<A>(() => value, (o) => (o(value), disposeNone()))
}

/**
 * Create a constant behavior with `Behavior.of(value)` or use this representative with ply.
 */
export const Behavior: BehaviorTypeRep = {
  '@@type': 'Behavior' as const,
  _shape: undefined as unknown as BehaviorShape,

  of(value) {
    return constant(value)
  },
}

Object.defineProperty(proto, 'constructor', { value: Behavior })

/**
 * Check whether a value is a Behavior created by this package.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.isBehavior(S.constant(1)) // => true
 * S.isBehavior(S.wrap(1)) // => false
 * ```
 */
export function isBehavior(value: unknown): value is Behavior<unknown> {
  return typeof value === 'object' && value !== null &&
    Object.getPrototypeOf(value) === proto
}

/**
 * Read the current value of a behavior.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.sample(S.constant('now')) // => now
 * ```
 */
export function sample<A>(b: Behavior<A>): A {
  return b.sample()
}

/**
 * Receive the current value immediately, then future updates.
 * Dispose the returned handle to stop observing.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const seen: number[] = []
 * S.observe((v: number) => seen.push(v))(S.constant(7))
 * // seen => [ 7 ]
 * ```
 */
export function observe<A>(
  o: Observer<A>,
): (b: Behavior<A>) => Disposable {
  return (b) => b.observe(o)
}

/**
 * Start listening immediately and hold the latest event, using `init` until the first arrival.
 * Dispose the behavior to stop its source subscription.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch, tick] = S.newVirtualScheduler()
 * const latest = S.stepper('none', sch)(S.at(10)('hello'))
 *
 * S.sample(latest) // => none
 * tick(10)
 * S.sample(latest) // => hello
 * ```
 */
export function stepper<A>(
  init: A,
  scheduler: Scheduler = defaultScheduler(),
): (strm: Stream<A>) => Behavior<A> {
  return (strm) => {
    let current = init
    const observers = new Set<Observer<A>>()

    const source = strm.run(
      sink<A>((_t: Time, value: A) => {
        current = value
        touch()
        batch(() => {
          for (const o of [...observers]) {
            if (observers.has(o)) o(value)
          }
        })
      }, () => {}),
      scheduler,
    )

    return behavior<A>(
      () => current,
      (o) => {
        o(current)
        observers.add(o)
        return disposable(() => observers.delete(o))
      },
      () => {
        observers.clear()
        dispose(source)
      },
    )
  }
}

/**
 * Start listening immediately and update state with each event.
 * The behavior starts with `init`; dispose it to stop the source.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch] = S.newVirtualScheduler()
 * const [clicks, click] = S.bus<void>()
 * function increment(count: number): number {
 *   return count + 1
 * }
 * const count = S.accum(increment, 0, sch)(clicks)
 *
 * click()
 * click()
 * S.sample(count) // => 2
 * ```
 */
export function accum<Acc, A>(
  step: (acc: Acc, a: A) => Acc,
  init: Acc,
  scheduler: Scheduler = defaultScheduler(),
): (strm: Stream<A>) => Behavior<Acc> {
  return (strm) => {
    let state = init
    return stepper(init, scheduler)(
      stream<Acc>((snk, sch) =>
        strm.run(
          sink<A>((t, a) => {
            state = step(state, a)
            snk.event(t, state)
          }, snk.end),
          sch,
        )
      ),
    )
  }
}

/**
 * Fold a behavior into a behavior, keeping what the fold last produced.
 * The result starts folded with the value the source already holds; dispose it to stop observing.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [clicks, click] = S.bus<void>()
 * const count = S.accum((n: number) => n + 1, 0)(clicks)
 * const total = S.accumFrom((sum: number, n: number) => sum + n, 0)(count)
 *
 * click()
 * click()
 * S.sample(total) // => 3
 * ```
 */
export function accumFrom<Acc, A>(
  step: (acc: Acc, a: A) => Acc,
  init: Acc,
): (source: Behavior<A>) => Behavior<Acc> {
  return (source) => {
    const observers = new Set<Observer<Acc>>()
    let current = init
    let live = false

    const notice = cell(() => {
      for (const o of [...observers]) {
        if (observers.has(o)) o(current)
      }
    })

    const d = source.observe((a: A) => {
      current = step(current, a)
      if (live) notifyLater(notice)
    })
    live = true

    return behavior<Acc>(
      () => current,
      (o) => {
        o(current)
        observers.add(o)
        return disposable(() => observers.delete(o))
      },
      () => {
        observers.clear()
        cancelNotification(notice)
        dispose(d)
      },
    )
  }
}

/**
 * Emit future behavior updates, omitting the initial value. This stream does not end on its own.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch, tick] = S.newVirtualScheduler()
 * const latest = S.stepper(0, sch)(S.periodic(10))
 * const seen: number[] = []
 * S.subscribe((_t: number, v: number) => seen.push(v), undefined, sch)(S.changes(latest))
 * tick(25)
 * // seen => [ 10, 20 ]
 * ```
 */
export function changes<A>(b: Behavior<A>): Stream<A> {
  return stream<A>((snk, sch) => {
    let first = true
    return b.observe((value) => {
      if (first) {
        first = false
        return
      }
      snk.event(sch.currentTime(), value)
    })
  })
}

/**
 * Combine each stream event with the current behavior value, keeping the event time.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch] = S.newVirtualScheduler()
 * const [ticks, tick] = S.bus<string>()
 * function increment(count: number): number {
 *   return count + 1
 * }
 * function labelCount(label: string, count: number): string {
 *   return `${label}${count}`
 * }
 * const count = S.accum(increment, 0, sch)(ticks)
 * const seen: string[] = []
 * S.subscribe((_t: number, v: string) => seen.push(v), undefined, sch)(
 *   S.snapshotWith(labelCount)(count)(ticks),
 * )
 * tick('x')
 * // seen => [ "x1" ]
 * ```
 */
export function snapshotWith<A, B, C>(
  f: (a: A, b: B) => C,
): (b: Behavior<B>) => (strm: Stream<A>) => Stream<C> {
  return (b) => (strm) =>
    stream<C>((snk, sch) =>
      strm.run(
        sink<A>((t, a) => snk.event(t, f(a, b.sample())), snk.end),
        sch,
      )
    )
}

/**
 * Replace each stream event with the current behavior value, keeping the event time.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const seen = S.values(30)(S.snapshot(S.constant('v'))(S.at(10)('tick')))
 * // seen => [ "v" ]
 * ```
 */
export function snapshot<B>(
  b: Behavior<B>,
): <A>(strm: Stream<A>) => Stream<B> {
  return <A>(strm: Stream<A>) =>
    snapshotWith((_a: A, value: B) => value)(b)(strm)
}

/**
 * Start with one behavior, then follow each new behavior arriving from the stream.
 * Dispose the result to stop listening to the switching stream.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch, tick] = S.newVirtualScheduler()
 * const swapped = S.switcher(S.constant('before'), sch)(
 *   S.at(10)(S.constant('after')),
 * )
 *
 * S.sample(swapped) // => before
 * tick(10)
 * S.sample(swapped) // => after
 * ```
 */
export function switcher<A>(
  init: Behavior<A>,
  scheduler: Scheduler = defaultScheduler(),
): (strm: Stream<Behavior<A>>) => Behavior<A> {
  return (strm) => {
    const held = stepper(init, scheduler)(strm)
    const flat = held.chain((b: Behavior<A>) => b)
    return behavior<A>(
      flat.sample,
      flat.observe,
      () => dispose(held),
    )
  }
}

/**
 * Listen to the stream currently held by a behavior. Dispose the old subscription when it changes.
 * An inner stream ending does not end the result.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const [sch, tick] = S.newVirtualScheduler()
 * const source = S.stepper(S.periodic(10), sch)(
 *   S.at(25)(S.periodic(3)),
 * )
 * const seen: number[] = []
 * S.subscribe((t: number) => seen.push(t), undefined, sch)(S.shiftCurrent(source))
 * tick(31)
 * // seen => [ 10, 20, 28, 31 ]
 * ```
 */
export function shiftCurrent<A>(b: Behavior<Stream<A>>): Stream<A> {
  return stream<A>((snk, sch) => {
    let inner = disposeNone()

    const outer = b.observe((current) => {
      dispose(inner)
      inner = current.run(sink<A>(snk.event, () => {}), sch)
    })

    return disposable(() => {
      dispose(outer)
      dispose(inner)
    })
  })
}

/**
 * Sample each arriving behavior and emit its current value at the same event time.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const carried = S.at(10)(S.constant('what it said'))
 * S.values(20)(S.selfie(carried)) // => [ "what it said" ]
 * ```
 */
export function selfie<A>(strm: Stream<Behavior<A>>): Stream<A> {
  return stream<A>((snk, sch) =>
    strm.run(
      sink<Behavior<A>>((t: Time, b) => snk.event(t, b.sample()), snk.end),
      sch,
    )
  )
}
