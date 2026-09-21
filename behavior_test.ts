import { assertEquals, assertThrows } from '@std/assert'
import * as P from '@algosail/ply'
import * as S from './mod.ts'

// Examples

Deno.test('isBehavior tells the two types apart', () => {
  assertEquals(S.isBehavior(S.constant(1)), true)
  assertEquals(S.isBehavior(S.wrap(1)), false)
  assertEquals(S.isStream(S.constant(1)), false)
  assertEquals(S.isBehavior(1), false)
})

Deno.test('constant: the same value, always', () => {
  const b = S.constant(42)
  assertEquals(S.sample(b), 42)
  const seen: number[] = []
  S.observe((v: number) => seen.push(v))(b)
  assertEquals(seen, [42], 'the current value arrives at once')
})

Deno.test('stepper: init until the stream says otherwise', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const latest = S.stepper('none', scheduler)(S.at(10)('hello'))

  assertEquals(S.sample(latest), 'none')
  advanceTo(9)
  assertEquals(S.sample(latest), 'none')
  advanceTo(10)
  assertEquals(S.sample(latest), 'hello')
})

Deno.test('two observers of one behavior never disagree', () => {
  const { click, count } = createCounter()
  const a: number[] = []
  const b: number[] = []
  S.observe((n: number) => a.push(n))(count)
  S.observe((n: number) => b.push(n))(count)
  click()
  click()
  assertEquals(a, b)
})

Deno.test('accum builds state from events', () => {
  function add(total: number, value: number): number {
    return total + value
  }

  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const total = S.accum(add, 0, scheduler)(S.periodic(10))

  assertEquals(S.sample(total), 0)
  advanceTo(25)
  assertEquals(S.sample(total), 30)
})

Deno.test('ap and chain keep up as their parents change', () => {
  function formatCount(name: string) {
    return function withCount(count: number): string {
      return `${name} (${count})`
    }
  }

  const { click, count } = createCounter()
  const shown = P.lift2(formatCount)(
    S.constant('Ada'),
  )(count)

  const seen: string[] = []
  S.observe((v: string) => seen.push(v))(shown)
  click()
  click()
  assertEquals(seen, ['Ada (0)', 'Ada (1)', 'Ada (2)'])
})

Deno.test('chain switches which behavior is being lived', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const which = S.stepper(false, scheduler)(S.at(10)(true))
  function chooseBehavior(b: boolean) {
    return b ? S.constant('yes') : S.constant('no')
  }

  const picked = which.chain(chooseBehavior)

  const seen: string[] = []
  S.observe((v: string) => seen.push(v))(picked)
  assertEquals(S.sample(picked), 'no')
  advanceTo(10)
  assertEquals(S.sample(picked), 'yes')
  assertEquals(seen, ['no', 'yes'])
})

Deno.test('changes: the updates, without the value it already had', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const latest = S.stepper(0, scheduler)(S.periodic(10))
  const seen: [S.Time, number][] = []
  S.subscribe(
    (t: S.Time, v: number) => seen.push([t, v]),
    undefined,
    scheduler,
  )(
    S.changes(latest),
  )
  advanceTo(25)
  assertEquals(seen, [[10, 10], [20, 20]])
})

Deno.test('snapshot: the stream says when, the behavior says what', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const latest = S.stepper('a', scheduler)(S.at(15)('b'))
  const seen: [S.Time, string][] = []
  S.subscribe(
    (t: S.Time, v: string) => seen.push([t, v]),
    undefined,
    scheduler,
  )(
    S.snapshot(latest)(S.periodic(10)),
  )
  advanceTo(25)
  assertEquals(seen, [[10, 'a'], [20, 'b']])
})

Deno.test('snapshotWith combines the event with the value', () => {
  const { scheduler, click, count } = createCounter()
  const [ticks, advanceTo] = S.bus<string>()
  const seen: string[] = []
  S.subscribe((_t: S.Time, v: string) => seen.push(v), undefined, scheduler)(
    S.snapshotWith((a: string, n: number) => `${a}${n}`)(count)(ticks),
  )

  advanceTo('x')
  click()
  advanceTo('y')
  assertEquals(seen, ['x0', 'y1'])
})

Deno.test('switcher lives as one behavior and then another', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const swapped = S.switcher(S.constant('before'), scheduler)(
    S.at(10)(S.constant('after')),
  )

  const seen: string[] = []
  S.observe((v: string) => seen.push(v))(swapped)
  assertEquals(S.sample(swapped), 'before')
  advanceTo(10)
  assertEquals(S.sample(swapped), 'after')
  assertEquals(seen, ['before', 'after'])
})

Deno.test('shiftCurrent does not end when an inner stream does', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const source = S.stepper(S.at(5)('a'), scheduler)(S.at(20)(S.at(5)('b')))
  const seen: string[] = []
  const ends: string[] = []
  S.subscribe(
    (_t: S.Time, v: string) => seen.push(v),
    () => ends.push('end'),
    scheduler,
  )(S.shiftCurrent(source))
  advanceTo(30)
  assertEquals(seen, ['a', 'b'])
  assertEquals(ends, [])
})

Deno.test('selfie reads the carried behavior at the moment it arrives', () => {
  function increment(value: number): number {
    return value + 1
  }

  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const [ticks, tick_] = S.bus<void>()
  const count = S.accum(increment, 0, scheduler)(ticks)

  const carried = S.fromIterable([count])
  assertEquals(S.values(0)(S.selfie(carried)), [0])

  tick_()
  tick_()
  assertEquals(S.values(0)(S.selfie(S.fromIterable([count]))), [2])
  advanceTo(0)
})

Deno.test('sample stays consistent, as it always did', () => {
  function timesTen(value: number): number {
    return value * 10
  }
  function plusHundred(value: number): number {
    return value + 100
  }
  function formatPair(first: number) {
    return function withSecond(second: number): string {
      return `${first}/${second}`
    }
  }

  const [scheduler] = S.newVirtualScheduler()
  const [nums, push] = S.bus<number>()
  const n = S.stepper(0, scheduler)(nums)
  const both = P.lift2(formatPair)(
    n.map(timesTen),
  )(n.map(plusHundred))

  const sampled: string[] = []
  ;[1, 2, 3].forEach((x) => {
    push(x)
    sampled.push(S.sample(both))
  })
  assertEquals(sampled, ['10/101', '20/102', '30/103'])
})

Deno.test('transact makes several changes into one move', () => {
  function fullName(first: string) {
    return function withLast(last: string): string {
      return `${first} ${last}`
    }
  }

  const [scheduler] = S.newVirtualScheduler()
  const [first, setFirst] = S.bus<string>()
  const [last, setLast] = S.bus<string>()
  const full = P.lift2(fullName)(
    S.stepper('', scheduler)(first),
  )(S.stepper('', scheduler)(last))

  const seen: string[] = []
  S.observe((v: string) => seen.push(v))(full)

  setFirst('Ada')
  setLast('Lovelace')
  assertEquals(
    seen,
    [' ', 'Ada ', 'Ada Lovelace'],
    'the half-filled step shows',
  )

  seen.length = 0
  function updateTogether() {
    setFirst('Grace')
    setLast('Hopper')
  }

  S.batch(updateTogether)
  assertEquals(seen, ['Grace Hopper'], 'one frame for the whole move')
})

Deno.test('accumFrom: folds a behavior the way accum folds a stream', () => {
  const { click, count } = createCounter()
  const total = S.accumFrom((sum: number, n: number) => sum + n, 0)(count)

  assertEquals(S.sample(total), 0, 'folded with what the source already held')
  click()
  click()
  assertEquals(S.sample(total), 3, '0 + 1 + 2')
})

Deno.test('accumFrom: one fold however many are watching', () => {
  let steps = 0
  const { click, count } = createCounter()
  const total = S.accumFrom(
    (sum: number, n: number) => (steps += 1, sum + n),
    0,
  )(
    count,
  )

  const a: number[] = []
  const b: number[] = []
  S.observe((n: number) => a.push(n))(total)
  S.observe((n: number) => b.push(n))(total)
  click()

  assertEquals([a, b], [[0, 1], [0, 1]], 'both see the same values')
  assertEquals(
    steps,
    2,
    'the seed and the one click, not one pair per observer',
  )
})

Deno.test('accumFrom: reading it twice does not move it', () => {
  const { click, count } = createCounter()
  const total = S.accumFrom((sum: number, n: number) => sum + n, 0)(count)
  click()
  assertEquals([S.sample(total), S.sample(total)], [1, 1])
})

Deno.test('accumFrom: an observer arriving late gets the state as it is', () => {
  const { click, count } = createCounter()
  const total = S.accumFrom((sum: number, n: number) => sum + n, 0)(count)
  click()
  click()
  const seen: number[] = []
  S.observe((n: number) => seen.push(n))(total)
  assertEquals(seen, [3])
})

Deno.test('accumFrom: a batch delivers the fold once, at its last value', () => {
  const [pushes, push] = S.bus<number>()
  const source = S.stepper(0)(pushes)
  const total = S.accumFrom((sum: number, n: number) => sum + n, 0)(source)

  const seen: number[] = []
  S.observe((n: number) => seen.push(n))(total)
  S.batch(() => {
    push(1)
    push(2)
  })
  assertEquals([seen, S.sample(total)], [[0, 3], 3])
})

Deno.test('accumFrom: disposing it stops observing the source', () => {
  let steps = 0
  const { click, count } = createCounter()
  const total = S.accumFrom(
    (sum: number, n: number) => (steps += 1, sum + n),
    0,
  )(
    count,
  )
  click()
  S.dispose(total)
  click()
  assertEquals(steps, 2, 'the seed and the first click only')
})

Deno.test('extract: the current value, under the name ply knows', () => {
  const { click, count } = createCounter()
  click()
  assertEquals(P.extract(count), S.sample(count))
})

Deno.test('extend: hands the function the behavior rather than the value', () => {
  const { click, count } = createCounter()
  const doubled = P.extend((w: S.Behavior<number>) => S.sample(w) * 2)(count)

  assertEquals(S.sample(doubled), 0)
  click()
  assertEquals(S.sample(doubled), 2)
})

Deno.test('extend: for this type that is map through a constant', () => {
  const f = (w: S.Behavior<number>) => S.sample(w) * 10
  const [pushes, push] = S.bus<number>()
  const source = S.stepper(1)(pushes)

  const whole: number[] = []
  const part: number[] = []
  S.observe((n: number) => whole.push(n))(
    P.extend(f)(source) as S.Behavior<number>,
  )
  S.observe((n: number) => part.push(n))(
    P.map((a: number) => f(S.constant(a)))(source),
  )
  push(2)
  push(3)

  assertEquals(whole, part)
  assertEquals(whole, [10, 20, 30])
})

// Laws

Deno.test('extend: the left identity law', () => {
  const { click, count } = createCounter()
  const whole = (w: S.Behavior<number>) => S.sample(w) + 1
  click()
  assertEquals(P.extract(P.extend(whole)(count)), whole(count))
})

Deno.test('extend: the right identity law', () => {
  const { click, count } = createCounter()
  click()
  const same = P.extend(P.extract)(count) as S.Behavior<number>
  assertEquals(S.sample(same), S.sample(count))
  click()
  assertEquals(S.sample(same), S.sample(count))
})

Deno.test('extend: identity is as faithful as map and chain, no less', () => {
  const deliveries = (make: (b: S.Behavior<number>) => S.Behavior<number>) => {
    const [pushes, push] = S.bus<number>()
    const source = S.stepper(1)(pushes)
    const seen: number[] = []
    S.observe((n: number) => seen.push(n))(make(source))
    S.batch(() => {
      push(2)
      push(3)
    })
    return seen
  }

  const straight = deliveries((b) => b)
  assertEquals(straight, [1, 2, 3], 'the source itself delivers every step')
  for (
    const [name, make] of [
      ['map', (b: S.Behavior<number>) => P.map((n: number) => n)(b)],
      [
        'chain',
        (b: S.Behavior<number>) => b.chain((n: number) => S.constant(n)),
      ],
      [
        'extend',
        (b: S.Behavior<number>) => P.extend(P.extract)(b) as S.Behavior<number>,
      ],
    ] as const
  ) {
    assertEquals(deliveries(make), [1, 3], name)
  }
})

Deno.test('extend: composition is associative', () => {
  const { click, count } = createCounter()
  const f = (w: S.Behavior<number>) => S.sample(w) + 1
  const g = (w: S.Behavior<number>) => S.sample(w) * 2
  click()
  const left = P.extend(f)(P.extend(g)(count) as S.Behavior<number>)
  const right = P.extend((w: S.Behavior<number>) =>
    f(P.extend(g)(w) as S.Behavior<number>)
  )(count)
  assertEquals(S.sample(left as S.Behavior<number>), S.sample(right))
})

Deno.test('extend: map is extend through extract', () => {
  const { click, count } = createCounter()
  const f = (n: number) => n * 3
  click()
  assertEquals(
    S.sample(
      P.extend((w: S.Behavior<number>) => f(P.extract(w)))(
        count,
      ) as S.Behavior<number>,
    ),
    S.sample(count.map(f)),
  )
})

Deno.test('map: identity preserves the current value', () => {
  const { click, count } = createCounter()
  click()
  assertEquals(S.sample(count.map(identity)), S.sample(count))
})

Deno.test('map: composition matches successive maps', () => {
  function doubleThenIncrement(value: number): number {
    return increment(double(value))
  }

  const { click, count } = createCounter()
  click()
  assertEquals(
    S.sample(count.map(doubleThenIncrement)),
    S.sample(count.map(double).map(increment)),
  )
})

Deno.test('ap: identity preserves the value', () => {
  const five = S.constant(5)
  assertEquals(S.sample(five.ap(S.constant(identity<number>))), 5, 'identity')
})

Deno.test('ap: applying constants matches applying their values', () => {
  assertEquals(
    S.sample(S.constant(5).ap(S.constant(increment))),
    S.sample(S.constant(increment(5))),
    'homomorphism',
  )
})

Deno.test('ap: interchange preserves the result', () => {
  function applyToFive(transform: (value: number) => number): number {
    return transform(5)
  }
  const five = S.constant(5)
  const fns = S.constant(increment)
  assertEquals(
    S.sample(five.ap(fns)),
    S.sample(fns.ap(S.constant(applyToFive))),
    'interchange',
  )
})

Deno.test('ap is exactly what chain gives', () => {
  const { click, count } = createCounter()
  click()
  const fns = S.constant(double)
  function mapCount(transform: (value: number) => number): S.Behavior<number> {
    return count.map(transform)
  }

  assertEquals(
    S.sample(count.ap(fns)),
    S.sample(fns.chain(mapCount)),
  )
})

Deno.test('chain: constant is a left identity', () => {
  function timesTen(n: number) {
    return S.constant(n * 10)
  }
  assertEquals(
    S.sample(S.constant(3).chain(timesTen)),
    S.sample(timesTen(3)),
    'left identity',
  )
})

Deno.test('chain: constant is a right identity', () => {
  const { click, count } = createCounter()
  click()
  click()
  assertEquals(
    S.sample(count.chain(S.constant)),
    S.sample(count),
    'right identity',
  )
})

Deno.test('chain: grouping preserves the result', () => {
  const { click, count } = createCounter()
  click()
  click()
  function timesTen(n: number) {
    return S.constant(n * 10)
  }
  function timesTenThenPlusOne(value: number): S.Behavior<number> {
    return timesTen(value).chain(plusOne)
  }
  function plusOne(n: number) {
    return S.constant(n + 1)
  }
  assertEquals(
    S.sample(count.chain(timesTen).chain(plusOne)),
    S.sample(count.chain(timesTenThenPlusOne)),
    'associative',
  )
})

// Timing, subscriptions, and disposal

Deno.test('a behavior is shared: a late observer is not a late arrival', () => {
  const { click, count } = createCounter()

  const early: number[] = []
  const late: number[] = []

  S.observe((n: number) => early.push(n))(count)
  click()
  S.observe((n: number) => late.push(n))(count)
  click()

  assertEquals(early, [0, 1, 2])
  assertEquals(late, [1, 2], 'starts from what is true now, not from init')
  assertEquals(S.sample(count), 2)
})

Deno.test('disposing a root stops it; derived ones have nothing to stop', () => {
  function add(total: number, value: number): number {
    return total + value
  }

  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const total = S.accum(add, 0, scheduler)(S.periodic(10))
  const doubled = total.map(double)

  advanceTo(10)
  assertEquals(S.sample(doubled), 20)

  S.dispose(total)
  advanceTo(100)
  assertEquals(S.sample(total), 10, 'frozen where it was')
  assertEquals(S.sample(doubled), 20)

  S.dispose(doubled)
  assertEquals(S.sample(doubled), 20)
})

Deno.test('shiftCurrent listens to whichever stream is current', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const source = S.stepper(S.periodic(10), scheduler)(S.at(25)(S.periodic(3)))
  const seen: number[] = []
  S.subscribe((t: S.Time) => seen.push(t), undefined, scheduler)(
    S.shiftCurrent(source),
  )
  advanceTo(31)
  assertEquals(seen, [10, 20, 28, 31])
})

// Edge cases

Deno.test('a diamond gives no intermediate states', () => {
  function timesTen(value: number): number {
    return value * 10
  }
  function plusHundred(value: number): number {
    return value + 100
  }
  function formatPair(first: number) {
    return function withSecond(second: number): string {
      return `${first}/${second}`
    }
  }

  const [scheduler] = S.newVirtualScheduler()
  const [nums, push] = S.bus<number>()
  const n = S.stepper(0, scheduler)(nums)

  const left = n.map(timesTen)
  const right = n.map(plusHundred)
  const both = P.lift2(formatPair)(left)(right)

  const seen: string[] = []
  S.observe((v: string) => seen.push(v))(both)
  push(1)
  push(2)
  push(3)

  assertEquals(seen, ['0/100', '10/101', '20/102', '30/103'])
})

Deno.test('paths of different length still give one frame', () => {
  function identity<A>(value: A): A {
    return value
  }
  function addCurried(first: number) {
    return function addSecond(second: number): number {
      return first + second
    }
  }

  const [scheduler] = S.newVirtualScheduler()
  const [nums, push] = S.bus<number>()
  const n = S.stepper(0, scheduler)(nums)

  const near = n.map(identity)
  const far = n.map(identity).map(identity).map(identity)
  const both = P.lift2(addCurried)(near)(far)

  const seen: number[] = []
  S.observe((v: number) => seen.push(v))(both)
  push(5)
  assertEquals(seen, [0, 10])
})

Deno.test('one stream event moving two roots is one frame', () => {
  function formatStatus(result: string) {
    return function withBusy(busy: boolean): string {
      return `${busy ? '…' : ' '}${result}`
    }
  }

  const [scheduler] = S.newVirtualScheduler()
  const [answers, answer] = S.bus<string>()
  const shared = S.multicast(answers)

  const rows = S.stepper('—', scheduler)(shared)
  const busy = S.stepper(true, scheduler)(shared.map(() => false))
  const view = P.lift2(formatStatus)(
    rows,
  )(busy)

  const seen: string[] = []
  S.observe((v: string) => seen.push(v))(view)
  answer('ready')

  assertEquals(seen, ['…—', ' ready'])
})

Deno.test('a disposed observer is not told during a flush', () => {
  function double(value: number): number {
    return value * 2
  }

  const [scheduler] = S.newVirtualScheduler()
  const [nums, push] = S.bus<number>()
  const n = S.stepper(0, scheduler)(nums)
  const view = n.map(double)

  const seen: number[] = []
  const handle = S.observe((v: number) => seen.push(v))(view)
  S.dispose(handle)
  push(5)
  assertEquals(seen, [0])
})

Deno.test('observe: matches sample through a diamond during reentrant updates', () => {
  const [scheduler] = S.newVirtualScheduler()
  const [firstEvents, pushFirst] = S.bus<number>()
  const [secondEvents, pushSecond] = S.bus<number>()
  const first = S.stepper(0, scheduler)(firstEvents)
  S.stepper(0, scheduler)(secondEvents)
  function identity(value: number): number {
    return value
  }
  function formatPair(left: number) {
    return function withRight(right: number): string {
      return `${left}/${right}`
    }
  }
  function timesTen(value: number): number {
    return value * 10
  }
  function plusHundred(value: number): number {
    return value + 100
  }
  const view = P.lift2(formatPair)(first.map(timesTen))(first.map(plusHundred))
  const observed: string[] = []
  const sampled: string[] = []
  function record(value: string): void {
    observed.push(value)
    sampled.push(S.sample(view))
  }
  S.observe(record)(view)
  let deep = first.map(identity)
  for (let depth = 0; depth < 5; depth++) deep = deep.map(identity)
  function updateSecond(value: number): void {
    if (value > 0 && value < 5) pushSecond(value * 10)
  }
  S.observe(updateSecond)(deep)
  pushFirst(1)
  pushFirst(2)
  pushSecond(7)
  pushFirst(3)
  assertEquals(observed.length > 1, true)
  assertEquals(observed, sampled)
})

Deno.test('observe: matches sample through a uneven paths during reentrant updates', () => {
  const [scheduler] = S.newVirtualScheduler()
  const [firstEvents, pushFirst] = S.bus<number>()
  const [secondEvents, pushSecond] = S.bus<number>()
  const first = S.stepper(0, scheduler)(firstEvents)
  S.stepper(0, scheduler)(secondEvents)
  function identity(value: number): number {
    return value
  }
  function formatPair(left: number) {
    return function withRight(right: number): string {
      return `${left}/${right}`
    }
  }
  const near = first.map(identity)
  const far = first.map(identity).map(identity).map(identity)
  const view = P.lift2(formatPair)(near)(far)
  const observed: string[] = []
  const sampled: string[] = []
  function record(value: string): void {
    observed.push(value)
    sampled.push(S.sample(view))
  }
  S.observe(record)(view)
  let deep = first.map(identity)
  for (let depth = 0; depth < 5; depth++) deep = deep.map(identity)
  function updateSecond(value: number): void {
    if (value > 0 && value < 5) pushSecond(value * 10)
  }
  S.observe(updateSecond)(deep)
  pushFirst(1)
  pushFirst(2)
  pushSecond(7)
  pushFirst(3)
  assertEquals(observed.length > 1, true)
  assertEquals(observed, sampled)
})

Deno.test('observe: matches sample through a two roots during reentrant updates', () => {
  const [scheduler] = S.newVirtualScheduler()
  const [firstEvents, pushFirst] = S.bus<number>()
  const [secondEvents, pushSecond] = S.bus<number>()
  const first = S.stepper(0, scheduler)(firstEvents)
  const second = S.stepper(0, scheduler)(secondEvents)
  function identity(value: number): number {
    return value
  }
  function formatPair(left: number) {
    return function withRight(right: number): string {
      return `${left}/${right}`
    }
  }
  const view = P.lift2(formatPair)(first)(second)
  const observed: string[] = []
  const sampled: string[] = []
  function record(value: string): void {
    observed.push(value)
    sampled.push(S.sample(view))
  }
  S.observe(record)(view)
  let deep = first.map(identity)
  for (let depth = 0; depth < 5; depth++) deep = deep.map(identity)
  function updateSecond(value: number): void {
    if (value > 0 && value < 5) pushSecond(value * 10)
  }
  S.observe(updateSecond)(deep)
  pushFirst(1)
  pushFirst(2)
  pushSecond(7)
  pushFirst(3)
  assertEquals(observed.length > 1, true)
  assertEquals(observed, sampled)
})

Deno.test('observe: matches sample through a switched behavior during reentrant updates', () => {
  const [scheduler] = S.newVirtualScheduler()
  const [firstEvents, pushFirst] = S.bus<number>()
  const [secondEvents, pushSecond] = S.bus<number>()
  const first = S.stepper(0, scheduler)(firstEvents)
  const second = S.stepper(0, scheduler)(secondEvents)
  function identity(value: number): number {
    return value
  }
  function choose(value: number): S.Behavior<string> {
    return value > 1 ? second.map(String) : S.constant('low')
  }
  const view = first.chain(choose)
  const observed: string[] = []
  const sampled: string[] = []
  function record(value: string): void {
    observed.push(value)
    sampled.push(S.sample(view))
  }
  S.observe(record)(view)
  let deep = first.map(identity)
  for (let depth = 0; depth < 5; depth++) deep = deep.map(identity)
  function updateSecond(value: number): void {
    if (value > 0 && value < 5) pushSecond(value * 10)
  }
  S.observe(updateSecond)(deep)
  pushFirst(1)
  pushFirst(2)
  pushSecond(7)
  pushFirst(3)
  assertEquals(observed.length > 1, true)
  assertEquals(observed, sampled)
})

Deno.test('a broken observer does not wedge the rest', () => {
  function double(value: number): number {
    return value * 2
  }
  function plusThousand(value: number): number {
    return value + 1000
  }

  const [scheduler] = S.newVirtualScheduler()
  const [nums, push] = S.bus<number>()
  const n = S.stepper(0, scheduler)(nums)

  let armed = false
  function onUpdate() {
    if (armed) throw new Error('boom')
  }

  S.observe(onUpdate)(n.map(double))

  const seen: number[] = []
  S.observe((v: number) => seen.push(v))(n.map(plusThousand))
  armed = true

  let thrown = 0
  for (const v of [1, 2, 3]) {
    try {
      push(v)
    } catch {
      thrown += 1
    }
  }

  assertEquals(thrown, 3, 'the failure still surfaces, every time')
  assertEquals(seen, [1000, 1001, 1002, 1003], 'and everybody else is told')
})

Deno.test('an observer that writes starts a new update, and both are seen', () => {
  function identity<A>(value: A): A {
    return value
  }

  const [scheduler] = S.newVirtualScheduler()
  const [c, pushC] = S.bus<number>()
  const bc = S.stepper(0, scheduler)(c)
  const seen: number[] = []
  let once = true
  function onUpdate(v: number) {
    seen.push(v)
    if (once && v === 1) {
      once = false
      pushC(99)
    }
  }

  S.observe(onUpdate)(bc.map(identity))
  pushC(1)
  assertEquals(seen, [0, 1, 99])
})

// Integration with ply and type contracts

Deno.test('Behavior: the representative', () => {
  assertEquals(S.Behavior['@@type'], 'Behavior')
  assertEquals(S.sample(S.Behavior.of(42)), 42)

  const rep = S.Behavior as unknown as Record<string, unknown>
  assertEquals(Object.keys(rep).sort(), ['@@type', '_shape', 'of'])
  for (const k of ['empty', 'zero', 'map', 'ap']) {
    assertEquals(rep[k], undefined, k)
  }
})

Deno.test('Behavior: the shape of a value', () => {
  const sample = S.constant(1)
  const proto = Object.getPrototypeOf(sample) as Record<string, unknown>

  assertEquals(Object.keys(proto).sort(), [
    '@@type',
    'ap',
    'chain',
    'extend',
    'extract',
    'map',
    'show',
  ])
  for (const m of ['filter', 'concat', 'alt', 'reduce', 'traverse', 'equals']) {
    assertEquals(typeof proto[m], 'undefined', m)
  }

  const ctor = Object.getOwnPropertyDescriptor(proto, 'constructor')
  assertEquals(ctor?.value === S.Behavior, true)
  assertEquals(ctor?.enumerable, false)
})

Deno.test('ply lift2: combines current values immediately', () => {
  function addCurried(first: number) {
    return function addSecond(second: number): number {
      return first + second
    }
  }

  assertEquals(
    S.sample(
      P.lift2(addCurried)(S.constant(1))(
        S.constant(2),
      ),
    ),
    3,
  )
})

Deno.test('ply lift3: combines current values immediately', () => {
  function addThree(first: number) {
    return function addSecond(second: number) {
      return function addThird(third: number): number {
        return first + second + third
      }
    }
  }

  assertEquals(
    S.sample(
      P.lift3(addThree)(
        S.constant(1),
      )(S.constant(2))(S.constant(3)),
    ),
    6,
  )
})

Deno.test('ply: of creates a Behavior', () => {
  assertEquals(S.sample(P.of<S.BehaviorShape, never>(S.Behavior)(7)), 7)
})

Deno.test('ply: map transforms a Behavior', () => {
  assertEquals(S.sample(P.map(double)(S.constant(21))), 42)
})

Deno.test('ply: ap applies a Behavior of functions', () => {
  assertEquals(S.sample(P.ap(S.constant(increment))(S.constant(41))), 42)
})

Deno.test('ply: chain selects a Behavior', () => {
  assertEquals(
    S.sample(P.chain((n: number) => S.constant(n * 2))(S.constant(21))),
    42,
  )
})

Deno.test('ply: lift2 combines Behaviors', () => {
  function addCurried(first: number) {
    return function addSecond(second: number): number {
      return first + second
    }
  }

  assertEquals(
    S.sample(
      P.lift2(addCurried)(S.constant(1))(
        S.constant(2),
      ),
    ),
    3,
  )
})

Deno.test('ply: apFirst keeps the first Behavior value', () => {
  assertEquals(S.sample(P.apFirst(S.constant(1))(S.constant(2))), 1)
})

Deno.test('ply: show describes a Behavior', () => {
  assertEquals(P.show(S.constant(1)), 'Behavior')
})

Deno.test('ply filter: rejects unsupported Behavior operations', () => {
  const b = S.constant(1)
  assertThrows(
    () => P.filter((n: number) => n > 0)(b as never),
    TypeError,
    'filter: Behavior has no Filterable',
  )
})

Deno.test('ply concat: rejects unsupported Behavior operations', () => {
  const b = S.constant(1)
  assertThrows(
    () => P.concat(b as never)(b as never),
    TypeError,
    'concat: Behavior has no Semigroup',
  )
})

Deno.test('ply alt: rejects unsupported Behavior operations', () => {
  const b = S.constant(1)
  assertThrows(
    () => P.alt(b as never)(b as never),
    TypeError,
    'alt: Behavior has no Alt',
  )
})

Deno.test('ply empty: rejects unsupported Behavior operations', () => {
  assertThrows(
    () => P.empty(S.Behavior as never),
    TypeError,
    'empty: Behavior has no Monoid',
  )
})

Deno.test('ply reduce: rejects unsupported Behavior operations', () => {
  function add(total: number, value: number): number {
    return total + value
  }

  const b = S.constant(1)
  assertThrows(
    () => P.reduce(add)(0)(b as never),
    TypeError,
    'reduce: Behavior has no Foldable',
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

function createCounter() {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const [clicks, click] = S.bus<void>()
  const count = S.accum((n: number) => n + 1, 0, scheduler)(clicks)
  return { scheduler, advanceTo, click, count }
}
