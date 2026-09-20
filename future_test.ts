import { assertEquals } from '@std/assert'
import * as P from '@algosail/ply'
import * as F from '@algosail/plyfuture'
import * as S from './mod.ts'
import type { Ended } from './future.ts'
import {
  ended,
  flatFutures,
  flatFuturesLatest,
  flatFuturesOrdered,
  fromFuture,
  isEnded,
  next,
  switchTo,
} from './future.ts'

// Examples

Deno.test('Ended carries the moment the stream gave up at', () => {
  assertEquals(ended(20).time, 20)
  assertEquals(ended(20)['@@type'], 'Ended')
  assertEquals(isEnded(ended(0)), true)
  assertEquals(isEnded('nope'), false)
  assertEquals(isEnded(null), false)
})

Deno.test('fromFuture: emits a success as Right', async () => {
  assertEquals(
    (await S.collect(fromFuture(F.resolved<string, number>(42)))).map(P.show),
    ['Right (42)'],
  )
})

Deno.test('fromFuture: emits a rejection as Left', async () => {
  assertEquals(
    (await S.collect(fromFuture(F.rejected<string, number>('nope')))).map(
      P.show,
    ),
    ['Left ("nope")'],
  )
})

Deno.test('next: waits for a delayed event', async () => {
  assertEquals(
    await F.promise(next(S.defaultScheduler())(S.at(1)('hello'))),
    'hello',
  )
})

Deno.test('next: takes only the first iterable value', async () => {
  assertEquals(
    await F.promise(next(S.defaultScheduler())(S.fromIterable([1, 2, 3]))),
    1,
  )
})

Deno.test('switchTo: follows the replacement when the task resolves', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  let resolveTask: (value: S.Behavior<string>) => void = () => {}
  function run(
    _reject: (error: string) => void,
    resolve: (value: S.Behavior<string>) => void,
  ): void {
    resolveTask = resolve
  }
  const status = switchTo(S.constant('loading'), scheduler)(F.future(run))
  const seen: string[] = []
  S.observe((value: string) => seen.push(value))(status)
  assertEquals(S.sample(status), 'loading')
  resolveTask(S.constant('ready'))
  advanceTo(0)
  assertEquals(S.sample(status), 'ready')
  assertEquals(seen, ['loading', 'ready'])
})

Deno.test('switchTo: a task that fails changes nothing', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const failed = F.rejected<string, S.Behavior<string>>('nope')
  const status = switchTo(S.constant('loading'), scheduler)(failed)

  advanceTo(10)
  assertEquals(S.sample(status), 'loading')
})

Deno.test('flatFutures: answers in the order they land', async () => {
  assertEquals(await collectResults(flatFutures(threeTasks())), [
    'quick',
    'mid',
    'slow',
  ])
})

Deno.test('flatFuturesOrdered: answers in the order they were asked for', async () => {
  assertEquals(
    await collectResults(flatFuturesOrdered(threeTasks())),
    ['slow', 'quick', 'mid'],
  )
})

Deno.test('flatFutures: waits for all pending tasks before completion', async () => {
  assertEquals((await collectResults(flatFutures(threeTasks()))).length, 3)
})

// Timing, subscriptions, and disposal

Deno.test('fromFuture: each run forks the task again', async () => {
  let forks = 0
  function run(
    _reject: (error: string) => void,
    resolve: (value: number) => void,
  ): void {
    forks += 1
    resolve(forks)
  }
  const counted = F.future(run)
  const s = fromFuture(counted)

  assertEquals((await S.collect(s)).map(P.show), ['Right (1)'])
  assertEquals((await S.collect(s)).map(P.show), ['Right (2)'])
  assertEquals(forks, 2)
})

Deno.test('fromFuture: disposing stops the delivery', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: unknown[] = []
  const handle = S.subscribe(
    (_t: S.Time, v: unknown) => seen.push(v),
    undefined,
    scheduler,
  )(
    fromFuture(F.resolved<string, number>(1)),
  )
  S.dispose(handle)
  advanceTo(10)
  assertEquals(seen, [])
})

Deno.test('next: nothing runs until it is forked, and it stops after one', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  let runs = 0
  let live = false
  function runSource(sink: S.Sink<number>, s: S.Scheduler): Disposable {
    runs += 1
    live = true
    const d = S.periodic(10).run(sink, s)
    function stopSource() {
      live = false
      S.dispose(d)
    }

    return S.disposable(stopSource)
  }

  const source = S.stream<number>(runSource)

  const f = next(scheduler)(source)
  assertEquals(runs, 0, 'lazy until forked')

  const got: number[] = []

  F.fork<Ended>(() => {})<number>((v) => got.push(v))(f)
  assertEquals(runs, 1)

  advanceTo(25)
  assertEquals(got, [10])
  assertEquals(live, false, 'shut down as soon as it had one')
})

Deno.test('flatFuturesLatest: only the newest is still wanted', async () => {
  assertEquals(await collectResults(flatFuturesLatest(threeTasks())), ['mid'])
})

// Edge cases

Deno.test('fromFuture: failure crosses over as a value, not as an end', () => {
  const [errs, oks] = S.partitionMap((e: P.Either<string, number>) => e)(
    fromFuture(F.rejected<string, number>('boom')),
  )
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seenErr: string[] = []
  const seenOk: number[] = []
  const ends: string[] = []
  S.subscribe(
    (_t: S.Time, v: string) => seenErr.push(v),
    () => ends.push('err'),
    scheduler,
  )(errs)
  S.subscribe(
    (_t: S.Time, v: number) => seenOk.push(v),
    () => ends.push('ok'),
    scheduler,
  )(oks)

  advanceTo(0)

  assertEquals(seenErr, ['boom'])
  assertEquals(seenOk, [])
  assertEquals(ends.sort(), ['err', 'ok'], 'both branches end normally')
})

Deno.test('next: an empty stream rejects with Ended', async () => {
  const failed = await F.promise(
    next(S.defaultScheduler())(S.Stream.empty<number>()),
  ).then(() => 'resolved', (e) => e)
  assertEquals(isEnded(failed), true)
})

Deno.test('the two sides keep their own idea of failure', () => {
  const asStream = fromFuture(F.rejected<string, number>('x'))
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const events: string[] = []
  S.subscribe(
    (_t: S.Time, e: P.Either<string, number>) =>
      events.push(`event:${P.show(e)}`),
    () => events.push('end'),
    scheduler,
  )(asStream)

  advanceTo(0)
  assertEquals(events, ['event:Left ("x")', 'end'])
})

Deno.test('flatFutures: ends for an empty source', async () => {
  assertEquals((await collectResults(flatFutures(S.Stream.empty()))).length, 0)
})

Deno.test('a failed task comes out on the left, and the rest carry on', async () => {
  const mixed = S.fromIterable([
    F.rejected<string, string>('boom'),
    F.resolved<string, string>('fine'),
  ])
  assertEquals((await collectResults(flatFuturesOrdered(mixed))).sort(), [
    '!boom',
    'fine',
  ])
})

// Test support

function delayedValue(ms: number, name: string) {
  return F.after(ms)<string, string>(name)
}

function threeTasks() {
  return S.fromIterable([
    delayedValue(30, 'slow'),
    delayedValue(10, 'quick'),
    delayedValue(20, 'mid'),
  ])
}

async function collectResults(s: S.Stream<P.Either<string, string>>) {
  function formatError(error: string): string {
    return `!${error}`
  }
  function keepValue(value: string): string {
    return value
  }
  function formatResult(result: P.Either<string, string>): string {
    return result.match(formatError, keepValue)
  }
  const results = await S.collect(s)
  return results.map(formatResult)
}
