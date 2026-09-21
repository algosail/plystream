import { assertEquals, assertThrows } from '@std/assert'
import * as P from '@algosail/ply'
import * as S from './mod.ts'

// Examples

Deno.test('simulate: at records the emission time', () => {
  assertEquals(S.simulate(30)(S.at(20)('x')), [[20, 'x']])
})

Deno.test('nothing happens before its time', () => {
  assertEquals(S.simulate(19)(S.at(20)('x')), [])
  assertEquals(S.simulate(20)(S.at(20)('x')), [[20, 'x']])
})

Deno.test('the same stream simulated twice gives the same answer', () => {
  const s = S.merge(S.periodic(7))(S.periodic(10))
  assertEquals(S.simulate(30)(s), S.simulate(30)(s))
})

Deno.test('never: nothing, forever', () => {
  assertEquals(S.simulate(1000)(S.never<number>()), [])
})

Deno.test('fromIterable: emits every item at the same time', () => {
  assertEquals(S.simulate(0)(S.fromIterable([1, 2, 3])), [
    [0, 1],
    [0, 2],
    [0, 3],
  ])
})

Deno.test('range: uses the supplied step', () => {
  assertEquals(S.values(0)(S.range(2, 0, 4)), [0, 2, 4, 6])
})

Deno.test('range: starts from the supplied value', () => {
  assertEquals(S.values(0)(S.range(1, 5, 3)), [5, 6, 7])
})

Deno.test('periodic: emits each interval through the horizon', () => {
  assertEquals(S.values(50)(S.periodic(10)), [10, 20, 30, 40, 50])
})

Deno.test('repeat: has to be cut', () => {
  assertEquals(S.values(0)(S.take(3)(S.repeat('x'))), ['x', 'x', 'x'])
})

Deno.test('at: refuses a time it cannot keep', () => {
  assertThrows(() => S.at(-1))
  assertThrows(() => S.at(Number.NaN))
})

Deno.test('fromPromise: emits a resolved value as Right', async () => {
  const ok = await S.collect(S.fromPromise(Promise.resolve(42)))
  assertEquals(ok.map(P.show), ['Right (42)'])
})

Deno.test('fromPromise: emits a rejection as Left', async () => {
  const bad = await S.collect(S.fromPromise(Promise.reject('nope')))
  assertEquals(bad.map(P.show), ['Left ("nope")'])
})

Deno.test('map: changes values while preserving event times', () => {
  assertEquals(S.simulate(25)(S.periodic(10).map(double)), [[10, 20], [20, 40]])
})

Deno.test('filter: preserves the times of accepted events', () => {
  function isMultipleOfTwenty(value: number): boolean {
    return value % 20 === 0
  }

  assertEquals(
    S.simulate(45)(S.periodic(10).filter(isMultipleOfTwenty)),
    [[20, 20], [40, 40]],
  )
})

Deno.test('scan: emits a running total', () => {
  function add(total: number, value: number): number {
    return total + value
  }

  assertEquals(
    S.values(0)(
      S.scan(add, 0)(
        S.fromIterable([1, 2, 3]),
      ),
    ),
    [1, 3, 6],
  )
})

Deno.test('mapAccum: updates state and emits a separate output', () => {
  function updateTotal(total: number, value: number): [number, number] {
    return [total + value, total * value]
  }

  assertEquals(
    S.values(0)(
      S.mapAccum(
        updateTotal,
        1,
      )(
        S.fromIterable([1, 2, 3]),
      ),
    ),
    [1, 4, 12],
  )
})

Deno.test('skipRepeats: drops a value equal to the one just emitted', () => {
  function equal(first: number, second: number): boolean {
    return first === second
  }

  assertEquals(
    S.values(0)(
      S.skipRepeats(equal)(
        S.fromIterable([1, 1, 2, 2, 3]),
      ),
    ),
    [1, 2, 3],
  )
})

Deno.test('skipRepeats: a value that comes back after another gets through', () => {
  function equal(first: number, second: number): boolean {
    return first === second
  }

  assertEquals(
    S.values(0)(S.skipRepeats(equal)(S.fromIterable([1, 2, 1]))),
    [1, 2, 1],
  )
})

Deno.test('withCount: pairs values with counts starting at one', () => {
  assertEquals(S.values(0)(S.withCount(S.fromIterable(['a', 'b']))), [
    [1, 'a'],
    [2, 'b'],
  ])
})

Deno.test('count: emits the number of arrivals', () => {
  assertEquals(S.values(0)(S.count(S.fromIterable(['a', 'b', 'c']))), [1, 2, 3])
})

Deno.test('take: keeps the first events', () => {
  assertEquals(S.values(0)(S.take(2)(S.fromIterable([1, 2, 3, 4]))), [1, 2])
})

Deno.test('take: limits an infinite stream', () => {
  assertEquals(S.values(100)(S.take(2)(S.periodic(10))), [10, 20])
})

Deno.test('delay: shifts delivery times and preserves values', () => {
  assertEquals(S.simulate(60)(S.delay(15)(S.periodic(10))), [
    [25, 10],
    [35, 20],
    [45, 30],
    [55, 40],
  ])
})

Deno.test('continueWith: starts the next timeline at source completion', () => {
  assertEquals(
    S.simulate(60)(S.continueWith(() => S.at(10)('b'))(S.at(20)('a'))),
    [[20, 'a'], [30, 'b']],
  )
})

Deno.test('zipWith: combines pairs until the shorter stream is exhausted', () => {
  function add(total: number, value: number): number {
    return total + value
  }

  const zip = S.zipWith(add)
  assertEquals(
    S.values(0)(zip(S.fromIterable([1, 2, 3]))(S.fromIterable([10, 20]))),
    [11, 22],
  )
})

Deno.test('mergeArray: emits in arrival order', () => {
  assertEquals(
    S.simulate(40)(S.mergeArray([S.at(30)('slow'), S.at(10)('fast')])),
    [
      [10, 'fast'],
      [30, 'slow'],
    ],
  )
})

Deno.test('mergeArray ends on the last source, not the first', () => {
  const merged = S.mergeArray([S.at(10)('a'), S.at(30)('b')])
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const ends: S.Time[] = []

  S.subscribe(() => {}, (t: S.Time) => ends.push(t), scheduler)(merged)
  advanceTo(40)
  assertEquals(ends, [30])
})

Deno.test('switchLatest drops the one before', () => {
  assertEquals(
    S.values(60)(
      S.switchLatest(S.fromIterable([S.at(30)('slow'), S.at(10)('fast')])),
    ),
    ['fast'],
  )
})

Deno.test('exhaustLatest sees the one in flight through', () => {
  assertEquals(
    S.values(60)(
      S.exhaustLatest(S.fromIterable([S.at(50)('first'), S.at(10)('second')])),
    ),
    ['first'],
  )
})

Deno.test('flatMap: merges transformed values', () => {
  function doubleAsStream(value: number): S.Stream<number> {
    return S.wrap(value * 2)
  }

  assertEquals(
    S.values(0)(
      S.flatMap(doubleAsStream)(S.fromIterable([1, 2, 3])),
    ),
    [2, 4, 6],
  )
})

Deno.test('exhaustMap: ignores arrivals while the first stream runs', () => {
  function arriveAfter(value: number): S.Stream<number> {
    return S.at(value)(value)
  }

  assertEquals(
    S.values(60)(
      S.exhaustMap(arriveAfter)(S.fromIterable([50, 10])),
    ),
    [50],
  )
})

Deno.test('partition: separates matching and nonmatching values', () => {
  function isEven(value: number): boolean {
    return value % 2 === 0
  }

  const [evens, odds] = S.partition(isEven)(
    S.fromIterable([1, 2, 3, 4]),
  )
  assertEquals(collectTogether(0, evens, odds), [[2, 4], [1, 3]])
})

Deno.test('partitionMap: separates unwrapped errors and parsed values', () => {
  function parse(s: string) {
    return Number.isNaN(Number(s))
      ? P.left<string, number>(s)
      : P.right<string, number>(Number(s))
  }
  const [errs, oks] = S.partitionMap(parse)(S.fromIterable(['1', 'x', '2']))
  assertEquals(collectTogether(0, errs, oks), [['x'], [1, 2]])
})

Deno.test('merge: concat is the merge, not a carrying on', () => {
  assertEquals(S.simulate(40)(S.at(30)('a').concat(S.at(10)('b'))), [
    [10, 'b'],
    [30, 'a'],
  ])
  assertEquals(
    S.simulate(40)(S.continueWith(() => S.at(10)('b'))(S.at(30)('a'))),
    [[30, 'a'], [40, 'b']],
  )
})

Deno.test('filter: keeps accepted values', () => {
  function aboveTwo(value: number): boolean {
    return value > 2
  }

  const src = S.fromIterable([1, 2, 3, 4])
  assertEquals(S.values(0)(src.filter(aboveTwo)), [3, 4])
})

Deno.test('filter: rejecting every value produces no events', () => {
  const src = S.fromIterable([1, 2, 3, 4])
  assertEquals(S.values(0)(src.filter(() => false)), [])
})

Deno.test('collect: waits for iterable completion', async () => {
  assertEquals(await S.collect(S.fromIterable([1, 2, 3])), [1, 2, 3])
})

Deno.test('collect: collects a limited periodic stream', async () => {
  assertEquals(await S.collect(S.take(2)(S.periodic(1))), [1, 2])
})

Deno.test('takeWhile: excludes the first rejected value', () => {
  function belowThree(value: number): boolean {
    return value < 3
  }

  assertEquals(
    S.values(0)(
      S.takeWhile(belowThree)(S.fromIterable([1, 2, 3, 4])),
    ),
    [1, 2],
  )
})

Deno.test('takeWhile: keeps every accepted value', () => {
  assertEquals(S.values(0)(S.takeWhile(() => true)(S.fromIterable([1, 2]))), [
    1,
    2,
  ])
})

Deno.test('takeWhile: can reject the first value', () => {
  assertEquals(
    S.values(0)(S.takeWhile(() => false)(S.fromIterable([1, 2]))),
    [],
  )
})

Deno.test('takeThrough: includes the first matching value', () => {
  function reachedThree(value: number): boolean {
    return value >= 3
  }

  assertEquals(
    S.values(0)(
      S.takeThrough(reachedThree)(S.fromIterable([1, 2, 3, 4])),
    ),
    [1, 2, 3],
  )
})

Deno.test('skip: drops the first events', () => {
  assertEquals(S.values(0)(S.skip(2)(S.fromIterable([1, 2, 3, 4]))), [3, 4])
})

Deno.test('skip: can skip all events', () => {
  assertEquals(S.values(0)(S.skip(99)(S.fromIterable([1, 2]))), [])
})

Deno.test('slice: uses an exclusive end index', () => {
  assertEquals(S.values(0)(S.slice(1, 3)(S.fromIterable([1, 2, 3, 4, 5]))), [
    2,
    3,
  ])
})

Deno.test('slice: allows an omitted end', () => {
  assertEquals(S.values(0)(S.slice(3)(S.fromIterable([1, 2, 3, 4, 5]))), [4, 5])
})

Deno.test('slice: equal bounds produce no values', () => {
  assertEquals(S.values(0)(S.slice(2, 2)(S.fromIterable([1, 2, 3]))), [])
})

Deno.test('slice: selects arrivals from a periodic source', () => {
  assertEquals(S.values(100)(S.slice(1, 4)(S.periodic(10))), [20, 30, 40])
})

Deno.test('until ends on the signal and throws its value away', () => {
  assertEquals(S.simulate(60)(S.until(S.at(35)('stop'))(S.periodic(10))), [
    [10, 10],
    [20, 20],
    [30, 30],
  ])
})

Deno.test('until: keeps listening when the signal never emits', () => {
  assertEquals(S.values(45)(S.until(S.never<string>())(S.periodic(10))), [
    10,
    20,
    30,
    40,
  ])
})

Deno.test('until: ignores signal completion without a value', () => {
  assertEquals(
    S.values(45)(S.until(S.Stream.empty<string>())(S.periodic(10))),
    [10, 20, 30, 40],
  )
})

Deno.test('debounce collapses a burst into its last value', () => {
  const typing = S.mergeArray([S.at(0)('a'), S.at(5)('ab'), S.at(40)('abc')])
  assertEquals(S.simulate(80)(S.debounce(15)(typing)), [
    [20, 'ab'],
    [40, 'abc'],
  ])
})

Deno.test('throttle: keeps the first event of each window', () => {
  assertEquals(S.simulate(50)(S.throttle(25)(S.periodic(10))), [
    [10, 10],
    [40, 40],
  ])
})

Deno.test('map: handles Left values without ending the stream', () => {
  function recoverValue(result: P.Either<string, number>): number {
    return result.match(() => -1, identity)
  }
  const src = S.fromIterable([success(1), failure('x'), success(3)])
  assertEquals(
    S.values(0)(src.map(recoverValue)),
    [1, -1, 3],
  )
})

Deno.test('filter: can keep only Right values', () => {
  const src = S.fromIterable([success(1), failure('x'), success(3)])
  assertEquals(
    S.values(0)(src.filter((e) => e.tag === 'right')).map(P.show),
    ['Right (1)', 'Right (3)'],
  )
})

Deno.test('flatMap: can replace a Left with several recovery values', () => {
  const src = S.fromIterable([success(1), failure('x'), success(3)])
  function recover(e: P.Either<string, number>) {
    function fallback(): S.Stream<number> {
      return S.fromIterable([-1, -2])
    }
    return e.match(fallback, S.wrap)
  }

  assertEquals(
    S.values(0)(
      S.flatMap(recover)(src),
    ),
    [1, -1, -2, 3],
  )
})

// Laws

Deno.test('map: the Functor laws', async (test) => {
  const sources: [string, S.Stream<number>][] = [
    ['iterable', S.fromIterable([1, 2, 3])],
    ['single value', S.wrap(1)],
    ['empty', S.Stream.empty<number>()],
    ['periodic', S.take(3)(S.periodic(10))],
  ]
  function doubleThenIncrement(value: number): number {
    return increment(double(value))
  }
  for (const [name, source] of sources) {
    await test.step(`${name}: identity`, () => {
      assertEquals(S.simulate(50)(source.map(identity)), S.simulate(50)(source))
    })
    await test.step(`${name}: composition`, () => {
      assertEquals(
        S.simulate(50)(source.map(doubleThenIncrement)),
        S.simulate(50)(source.map(double).map(increment)),
      )
    })
  }
})

Deno.test('concat: grouping does not change merged events', () => {
  const a = S.take(2)(S.periodic(7))
  const b = S.at(10)(99)
  const c = S.at(25)(7)
  assertEquals(
    S.simulate(60)(a.concat(b).concat(c)),
    S.simulate(60)(a.concat(b.concat(c))),
    'associative',
  )
})

Deno.test('concat: empty is a left identity', () => {
  const a = S.take(2)(S.periodic(7))
  const empty = S.Stream.empty<number>()
  assertEquals(S.simulate(60)(empty.concat(a)), S.simulate(60)(a))
})

Deno.test('concat: empty is a right identity', () => {
  const a = S.take(2)(S.periodic(7))
  const empty = S.Stream.empty<number>()
  assertEquals(S.simulate(60)(a.concat(empty)), S.simulate(60)(a))
})

Deno.test('alt: grouping does not change merged events', () => {
  const a = S.take(2)(S.periodic(7))
  const b = S.at(10)(99)
  const c = S.at(25)(7)
  assertEquals(
    S.simulate(60)(a.alt(b).alt(c)),
    S.simulate(60)(a.alt(b.alt(c))),
    'associative',
  )
})

Deno.test('alt: mapping distributes over both branches', () => {
  const a = S.take(2)(S.periodic(7))
  const b = S.at(10)(99)
  assertEquals(
    S.simulate(60)(a.alt(b).map(increment)),
    S.simulate(60)(a.map(increment).alt(b.map(increment))),
    'distributive',
  )
})

Deno.test('alt: zero is a left identity', () => {
  const a = S.take(2)(S.periodic(7))
  const zero = S.Stream.zero<number>()
  assertEquals(S.simulate(60)(zero.alt(a)), S.simulate(60)(a))
})

Deno.test('alt: zero is a right identity', () => {
  const a = S.take(2)(S.periodic(7))
  const zero = S.Stream.zero<number>()
  assertEquals(S.simulate(60)(a.alt(zero)), S.simulate(60)(a))
})

Deno.test('filter: successive predicates compose', () => {
  function betweenOneAndFour(value: number): boolean {
    return value > 1 && value < 4
  }
  function aboveOne(value: number): boolean {
    return value > 1
  }
  function belowFour(value: number): boolean {
    return value < 4
  }

  const src = S.fromIterable([1, 2, 3, 4])
  assertEquals(
    S.values(0)(src.filter(aboveOne).filter(belowFour)),
    S.values(0)(src.filter(betweenOneAndFour)),
    'composes',
  )
})

// Timing, subscriptions, and disposal

Deno.test('simulate: periodic records each scheduled time', () => {
  assertEquals(S.simulate(25)(S.periodic(10)), [[10, 10], [20, 20]])
})

Deno.test('a stream is a recipe: every run is its own', () => {
  let runs = 0
  function runSource(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
    runs += 1
    return S.wrap(1).run(sink, scheduler)
  }

  const counted = S.stream<number>(runSource)
  S.simulate(0)(counted)
  S.simulate(0)(counted)
  assertEquals(runs, 2)
})

Deno.test('take shuts the source down once it has its quota', () => {
  let live = true
  function runSource(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
    const d = S.periodic(10).run(sink, scheduler)
    function stopSource() {
      live = false
      S.dispose(d)
    }

    return S.disposable(stopSource)
  }

  const source = S.stream<number>(runSource)
  assertEquals(S.values(100)(S.take(2)(source)), [10, 20])
  assertEquals(live, false)
})

Deno.test('startWith fires at the moment of subscription', () => {
  assertEquals(S.simulate(30)(S.startWith(0)(S.at(20)(1))), [[0, 0], [20, 1]])
})

Deno.test('mergeConcurrently: queues streams when the limit is one', () => {
  const inners = S.fromIterable([S.at(30)('a'), S.at(10)('b')])
  assertEquals(S.values(100)(S.mergeConcurrently(1)(inners)), ['a', 'b'])
})

Deno.test('mergeConcurrently: runs streams together when capacity allows', () => {
  const inners = S.fromIterable([S.at(30)('a'), S.at(10)('b')])
  assertEquals(S.values(100)(S.mergeConcurrently(2)(inners)), ['b', 'a'])
})

Deno.test('switchMap: keeps the newest inner stream', () => {
  function arriveAfter(value: number): S.Stream<number> {
    return S.at(value)(value)
  }

  assertEquals(
    S.values(60)(
      S.switchMap(arriveAfter)(S.fromIterable([30, 10])),
    ),
    [10],
  )
})

Deno.test('multicast: one run for everybody', () => {
  let runs = 0
  function runSource(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
    runs += 1
    return S.periodic(10).run(sink, scheduler)
  }

  const counted = S.stream<number>(runSource)
  const shared = S.multicast(counted)
  assertEquals(S.values(25)(S.merge(shared)(shared)), [10, 10, 20, 20])
  assertEquals(runs, 1)
})

Deno.test('without multicast each branch runs the source itself', () => {
  let runs = 0
  function runSource(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
    runs += 1
    return S.periodic(10).run(sink, scheduler)
  }

  const counted = S.stream<number>(runSource)
  S.values(25)(S.merge(counted)(counted))
  assertEquals(runs, 2)
})

Deno.test('bus: pushed by hand, shared, never ends on its own', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const [events, emit] = S.bus<string>()
  const a: [S.Time, string][] = []
  const b: [S.Time, string][] = []
  S.subscribe((t: S.Time, v: string) => a.push([t, v]), undefined, scheduler)(
    events,
  )
  S.subscribe((t: S.Time, v: string) => b.push([t, v]), undefined, scheduler)(
    events,
  )

  emit('one')
  advanceTo(10)
  emit('two')

  assertEquals(a, [[0, 'one'], [10, 'two']])
  assertEquals(b, a)
})

Deno.test('disposing a subscription reaches the source', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const shut: string[] = []
  function runSource(sink: S.Sink<number>, s: S.Scheduler): Disposable {
    const d = S.periodic(10).run(sink, s)
    function stopSource() {
      shut.push('source')
      S.dispose(d)
    }

    return S.disposable(stopSource)
  }

  const source = S.stream<number>(runSource)

  const seen: number[] = []
  const handle = S.subscribe(
    (_t: S.Time, v: number) => seen.push(v),
    undefined,
    scheduler,
  )(
    source.map(double).filter(() => true),
  )
  advanceTo(25)
  assertEquals(seen, [20, 40])

  S.dispose(handle)
  advanceTo(100)
  assertEquals(seen, [20, 40], 'nothing arrives after disposal')
  assertEquals(shut, ['source'])
})

Deno.test('mergeArray: disposal stops every source', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const disposed: string[] = []
  function tagged(name: string): S.Stream<number> {
    function run(sink: S.Sink<number>, clock: S.Scheduler): Disposable {
      const subscription = S.periodic(10).run(sink, clock)
      function stop(): void {
        disposed.push(name)
        S.dispose(subscription)
      }
      return S.disposable(stop)
    }
    return S.stream(run)
  }
  const subscription = S.subscribe(() => {}, undefined, scheduler)(
    S.mergeArray([tagged('a'), tagged('b')]),
  )
  advanceTo(15)
  S.dispose(subscription)
  assertEquals(disposed, ['a', 'b'])
})

Deno.test('takeWhile shuts the source down when it stops', () => {
  function beforeTwentyFive(time: number): boolean {
    return time < 25
  }

  let live = true
  function runSource(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
    const d = S.periodic(10).run(sink, scheduler)
    function stop(): void {
      live = false
      S.dispose(d)
    }
    return S.disposable(stop)
  }

  const source = S.stream<number>(runSource)
  assertEquals(S.values(100)(S.takeWhile(beforeTwentyFive)(source)), [
    10,
    20,
  ])
  assertEquals(live, false)
})

Deno.test('slice shuts the source down once its window closes', () => {
  let live = true
  function runSource(sink: S.Sink<number>, scheduler: S.Scheduler): Disposable {
    const d = S.periodic(10).run(sink, scheduler)
    function stop(): void {
      live = false
      S.dispose(d)
    }
    return S.disposable(stop)
  }

  const source = S.stream<number>(runSource)
  assertEquals(S.values(100)(S.slice(0, 2)(source)), [10, 20])
  assertEquals(live, false)
})

Deno.test('until: disposing the result stops both source and signal', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const disposed: string[] = []
  function runSource(sink: S.Sink<number>, clock: S.Scheduler): Disposable {
    const subscription = S.periodic(10).run(sink, clock)
    function stop(): void {
      disposed.push('source')
      S.dispose(subscription)
    }
    return S.disposable(stop)
  }
  function runSignal(sink: S.Sink<string>, clock: S.Scheduler): Disposable {
    const subscription = S.at(100)('stop').run(sink, clock)
    function stop(): void {
      disposed.push('signal')
      S.dispose(subscription)
    }
    return S.disposable(stop)
  }
  const seen: number[] = []
  const source = S.stream(runSource)
  const signal = S.stream(runSignal)
  const subscription = S.subscribe(
    (_time: S.Time, value: number) => seen.push(value),
    undefined,
    scheduler,
  )(S.until(signal)(source))
  advanceTo(15)
  assertEquals(seen, [10])
  S.dispose(subscription)
  advanceTo(200)
  assertEquals(seen, [10])
  assertEquals(disposed, ['source', 'signal'])
})

Deno.test('throttle needs no timer of its own', () => {
  function scheduledTimerCount(source: S.Stream<number>): number {
    let timers = 0
    const timer: S.Timer = {
      now: () => 0,
      setTimer(callback, delay) {
        timers += 1
        return setTimeout(callback, delay)
      },
      clearTimer(handle) {
        clearTimeout(handle as number)
      },
    }

    const subscription = S.subscribe(
      () => {},
      undefined,
      S.newScheduler(timer),
    )(source)
    S.dispose(subscription)
    return timers
  }
  const source = S.periodic(10)
  const baseline = scheduledTimerCount(source)
  assertEquals(scheduledTimerCount(S.throttle(25)(source)), baseline)
  assertEquals(scheduledTimerCount(S.delay(25)(source)) >= baseline, true)
})

// Edge cases

Deno.test('isStream: accepts streams and rejects unrelated values', () => {
  assertEquals(S.isStream(S.wrap(1)), true)
  assertEquals(S.isStream(1), false)
  assertEquals(S.isStream(null), false)
})

Deno.test('isSink: accepts event handlers and rejects an empty object', () => {
  assertEquals(S.isSink(S.sink(() => {}, () => {})), true)
  assertEquals(S.isSink({}), false)
})

Deno.test('simulate: wrap emits at time zero', () => {
  assertEquals(S.simulate(0)(S.wrap(42)), [[0, 42]])
})

Deno.test('fromIterable: accepts an empty iterable', () => {
  assertEquals(S.simulate(0)(S.fromIterable<number>([])), [])
})

Deno.test('range: rejects a fractional count', () => {
  assertThrows(() => S.range(1, 0, 1.5))
})

Deno.test('periodic: rejects NaN', () => {
  assertThrows(() => S.periodic(Number.NaN))
})

Deno.test('withIndex: pairs values with indices starting at zero', () => {
  assertEquals(S.values(0)(S.withIndex(0, 1)(S.fromIterable(['a', 'b']))), [
    [0, 'a'],
    [1, 'b'],
  ])
})

Deno.test('take: zero produces an empty stream', () => {
  assertEquals(S.values(0)(S.take(0)(S.fromIterable([1, 2, 3]))), [])
})

Deno.test('take: rejects a fractional limit', () => {
  assertThrows(() => S.take(1.5))
})

Deno.test('delay: rejects a negative duration', () => {
  assertThrows(() => S.delay(-1))
})

Deno.test('continueWith: starts after an empty source', () => {
  assertEquals(
    S.simulate(60)(
      S.continueWith(() => S.at(5)('b'))(S.Stream.empty<string>()),
    ),
    [[5, 'b']],
  )
})

Deno.test('zipWith: an empty stream produces no pairs', () => {
  function add(total: number, value: number): number {
    return total + value
  }

  const zip = S.zipWith(add)
  assertEquals(
    S.values(0)(zip(S.fromIterable([1, 2, 3]))(S.Stream.empty<number>())),
    [],
  )
})

Deno.test('mergeArray: accepts an empty array', () => {
  assertEquals(S.simulate(10)(S.mergeArray<number>([])), [])
})

Deno.test('concat: merging empty streams stays empty', () => {
  const empty = S.Stream.empty<number>()
  assertEquals(S.simulate(60)(empty.concat(empty)), [])
})

Deno.test('map: zero stays empty', () => {
  const zero = S.Stream.zero<number>()
  assertEquals(S.simulate(60)(zero.map(increment)), [])
})

Deno.test('collect: resolves with an empty array for an empty stream', async () => {
  assertEquals(await S.collect(S.Stream.empty<number>()), [])
})

Deno.test('skip: zero keeps every event', () => {
  assertEquals(S.values(0)(S.skip(0)(S.fromIterable([1, 2]))), [1, 2])
})

Deno.test('skip: rejects a fractional count', () => {
  assertThrows(() => S.skip(1.5))
})

Deno.test('slice: rejects a fractional end', () => {
  assertThrows(() => S.slice(0, 1.5))
})

Deno.test('debounce: flushes the pending value on source completion', () => {
  const typing = S.mergeArray([S.at(0)('a'), S.at(5)('ab'), S.at(40)('abc')])
  assertEquals(S.simulate(120)(S.debounce(50)(typing)), [[40, 'abc']])
})

Deno.test('debounce: rejects a negative duration', () => {
  assertThrows(() => S.debounce(-1))
})

Deno.test('throttle: zero keeps every event', () => {
  assertEquals(S.values(35)(S.throttle(0)(S.periodic(10))), [10, 20, 30])
})

Deno.test('throttle: rejects NaN', () => {
  assertThrows(() => S.throttle(Number.NaN))
})

Deno.test('a failure does not end the stream', () => {
  const timed = S.mergeArray([
    S.at(10)(success(1)),
    S.at(20)(failure('x')),
    S.at(30)(success(3)),
  ])
  function recover(e: P.Either<string, number>) {
    function fallback(): S.Stream<number> {
      return S.at(5)(-1)
    }
    return e.match(fallback, S.wrap)
  }

  assertEquals(
    S.simulate(60)(
      S.flatMap(recover)(timed),
    ),
    [[10, 1], [25, -1], [30, 3]],
  )
})

// Integration with ply and type contracts

Deno.test('Stream: the representative', () => {
  assertEquals(S.Stream['@@type'], 'Stream')
  assertEquals(S.simulate(0)(S.Stream.empty<number>()), [])
  assertEquals(S.simulate(0)(S.Stream.zero<number>()), [])

  const rep = S.Stream as unknown as Record<string, unknown>
  assertEquals(Object.keys(rep).sort(), ['@@type', '_shape', 'empty', 'zero'])
  for (const k of ['of', 'ap', 'chain', 'is', 'map', 'concat', 'run']) {
    assertEquals(rep[k], undefined, k)
  }
})

Deno.test('Stream: the shape of a value', () => {
  const sample = S.wrap(1)
  const proto = Object.getPrototypeOf(sample) as Record<string, unknown>

  assertEquals(Object.getPrototypeOf(S.Stream.empty<number>()), proto)
  assertEquals(Object.getPrototypeOf(S.never<number>()), proto)
  assertEquals(Object.keys(sample as unknown as Record<string, unknown>), [
    'run',
  ])

  assertEquals(Object.keys(proto).sort(), [
    '@@type',
    'alt',
    'concat',
    'filter',
    'map',
    'show',
  ])
  for (
    const m of ['of', 'ap', 'chain', 'reduce', 'traverse', 'equals', 'lte']
  ) {
    assertEquals(typeof proto[m], 'undefined', m)
  }

  const ctor = Object.getOwnPropertyDescriptor(proto, 'constructor')
  assertEquals(ctor?.value === S.Stream, true)
  assertEquals(ctor?.enumerable, false)
})

Deno.test('ply map: works with Stream', () => {
  const src = S.fromIterable([1, 2, 3])
  assertEquals(S.values(0)(P.map(double)(src)), [2, 4, 6])
})

Deno.test('ply filter: works with Stream', () => {
  function aboveTwo(value: number): boolean {
    return value > 2
  }

  const src = S.fromIterable([1, 2, 3])
  assertEquals(S.values(0)(P.filter(aboveTwo)(src)), [3])
})

Deno.test('ply filterMap: works with Stream', () => {
  function scaleAboveOne(value: number): P.Maybe<number> {
    return value > 1 ? P.just(value * 10) : P.nothing()
  }

  const src = S.fromIterable([1, 2, 3])
  assertEquals(
    S.values(0)(
      P.filterMap(scaleAboveOne)(
        src,
      ) as S.Stream<number>,
    ),
    [20, 30],
  )
})

Deno.test('ply concat: works with Stream', () => {
  assertEquals(
    S.simulate(40)(P.concat(S.at(10)('b'))(S.at(30)('a'))),
    [[10, 'b'], [30, 'a']],
  )
})

Deno.test('ply alt: works with Stream', () => {
  assertEquals(S.simulate(40)(P.alt(S.at(10)('b'))(S.at(30)('a'))), [
    [10, 'b'],
    [30, 'a'],
  ])
})

Deno.test('ply empty: works with Stream', () => {
  assertEquals(
    S.simulate(0)(P.empty<S.StreamShape, never, number>(S.Stream)),
    [],
  )
})

Deno.test('ply zero: works with Stream', () => {
  assertEquals(
    S.simulate(0)(P.zero<S.StreamShape, never, number>(S.Stream)),
    [],
  )
})

Deno.test('ply altAll: works with Stream', () => {
  assertEquals(
    S.simulate(40)(
      P.altAll<S.StreamShape, never>(S.Stream)([S.at(30)('a'), S.at(10)('b')]),
    ),
    [[10, 'b'], [30, 'a']],
  )
})

Deno.test('ply show: works with Stream', () => {
  assertEquals(P.show(S.wrap(1)), 'Stream')
})

Deno.test('ply of: rejects unsupported Stream operations', () => {
  assertThrows(
    () => P.of(S.Stream as never)(1),
    TypeError,
    'of: Stream has no Applicative',
  )
})

Deno.test('ply ap: rejects unsupported Stream operations', () => {
  const st = S.fromIterable([1, 2, 3])
  assertThrows(
    () => P.ap(S.wrap(increment) as never)(st as never),
    TypeError,
    'ap: Stream has no Apply',
  )
})

Deno.test('ply chain: rejects unsupported Stream operations', () => {
  const st = S.fromIterable([1, 2, 3])
  assertThrows(
    () => P.chain((n: number) => S.wrap(n))(st as never),
    TypeError,
    'chain: Stream has no Chain',
  )
})

Deno.test('ply reduce: rejects unsupported Stream operations', () => {
  function add(total: number, value: number): number {
    return total + value
  }

  const st = S.fromIterable([1, 2, 3])
  assertThrows(
    () => P.reduce(add)(0)(st as never),
    TypeError,
    'reduce: Stream has no Foldable',
  )
})

Deno.test('ply traverse: rejects unsupported Stream operations', () => {
  const st = S.fromIterable([1, 2, 3])
  assertThrows(
    () => P.traverse(S.Stream as never)((a: never) => a)(st as never),
    TypeError,
    'traverse: Stream has no Traversable',
  )
})

Deno.test('ply lte: rejects unsupported Stream operations', () => {
  const st = S.fromIterable([1, 2, 3])
  assertThrows(() => P.lte(st as never)(st as never), TypeError, 'has no Ord')
})

Deno.test('ply contramap: rejects unsupported Stream operations', () => {
  const st = S.fromIterable([1, 2, 3])
  assertThrows(
    () => P.contramap((s: string) => s.length)(st as never),
    TypeError,
    'contramap: Stream has no Contravariant',
  )
})

// Test support

function increment(n: number): number {
  return n + 1
}

function double(n: number): number {
  return n * 2
}

function identity<A>(a: A): A {
  return a
}

function collectTogether<A, B>(
  horizon: S.Time,
  a: S.Stream<A>,
  b: S.Stream<B>,
): [A[], B[]] {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const xs: A[] = []
  const ys: B[] = []
  S.subscribe((_t: S.Time, v: A) => xs.push(v), undefined, scheduler)(a)
  S.subscribe((_t: S.Time, v: B) => ys.push(v), undefined, scheduler)(b)
  advanceTo(horizon)
  return [xs, ys]
}

function success(n: number) {
  return P.right<string, number>(n)
}

function failure(e: string) {
  return P.left<string, number>(e)
}
