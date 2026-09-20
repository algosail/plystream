import { assertEquals } from '@std/assert'
import * as S from './mod.ts'

// Examples

Deno.test('delay: runs once, and is told the time it was due', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  scheduler.delay(25, scheduledTask)

  advanceTo(24)
  assertEquals(seen, [])
  advanceTo(25)
  assertEquals(seen, [25])
  advanceTo(1000)
  assertEquals(seen, [25], 'one shot stays one shot')
})

Deno.test('periodic: every period, from one period out', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  scheduler.periodic(10, scheduledTask)
  advanceTo(35)
  assertEquals(seen, [10, 20, 30])
  advanceTo(50)
  assertEquals(seen, [10, 20, 30, 40, 50])
})

Deno.test('relativeTo: the inner timeline starts where the origin is', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  advanceTo(100)
  const local = scheduler.relative(100)

  assertEquals(local.currentTime(), 0)
  assertEquals(scheduler.currentTime(), 100)

  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  local.delay(10, scheduledTask)
  advanceTo(110)
  assertEquals(seen, [10], 'the task reads local time')
  assertEquals(scheduler.currentTime(), 110, 'the outer clock is untouched')
})

// Laws

Deno.test('relativeTo: origins compose', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  advanceTo(50)
  const inner = scheduler.relative(20).relative(30)
  assertEquals(inner.currentTime(), 0)

  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  inner.periodic(10, scheduledTask)
  advanceTo(80)
  assertEquals(seen, [10, 20, 30])
})

// Timing, subscriptions, and disposal

Deno.test('newVirtualScheduler: the clock only moves when moved', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  assertEquals(scheduler.currentTime(), 0)
  advanceTo(40)
  assertEquals(scheduler.currentTime(), 40)
  advanceTo(10)
  assertEquals(scheduler.currentTime(), 40, 'never runs backwards')
})

Deno.test('delay: the due time is the scheduled one, even when the clock advances past it', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  scheduler.delay(10, scheduledTask)
  advanceTo(500)
  assertEquals(seen, [10])
})

Deno.test('tasks run in time order, whatever order they arrived in', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: string[] = []
  scheduler.delay(30, S.task(() => seen.push('c')))
  scheduler.delay(10, S.task(() => seen.push('a')))
  scheduler.delay(20, S.task(() => seen.push('b')))
  advanceTo(100)
  assertEquals(seen, ['a', 'b', 'c'])
})

Deno.test('ties keep the order they were scheduled in', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: string[] = []
  scheduler.delay(10, S.task(() => seen.push('first')))
  scheduler.delay(10, S.task(() => seen.push('second')))
  advanceTo(10)
  assertEquals(seen, ['first', 'second'])
})

Deno.test('asap: at the current moment, not before it', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  advanceTo(7)
  scheduler.asap(scheduledTask)
  assertEquals(seen, [], 'not during scheduling')
  advanceTo(7)
  assertEquals(seen, [7])
})

Deno.test('disposing a scheduled task cancels it and cleans it up', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: string[] = []
  let cleaned = 0
  const handle = scheduler.delay(10, {
    run: () => seen.push('ran'),
    dispose: () => (cleaned += 1),
  })

  S.dispose(handle)
  advanceTo(100)
  assertEquals(seen, [])
  assertEquals(cleaned, 1)

  S.dispose(handle)
  assertEquals(cleaned, 1, 'disposing twice is safe')
})

Deno.test('disposing one task leaves its neighbours alone', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: string[] = []
  scheduler.delay(10, S.task(() => seen.push('a')))
  const b = scheduler.delay(20, S.task(() => seen.push('b')))
  scheduler.delay(30, S.task(() => seen.push('c')))
  S.dispose(b)
  advanceTo(100)
  assertEquals(seen, ['a', 'c'])
})

Deno.test('a periodic task stops when its handle is disposed', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  const handle = scheduler.periodic(10, scheduledTask)
  advanceTo(25)
  assertEquals(seen, [10, 20])
  S.dispose(handle)
  advanceTo(100)
  assertEquals(seen, [10, 20])
})

Deno.test('work scheduled from inside a task still lands during the same clock advance', () => {
  const [scheduler, advanceTo] = S.newVirtualScheduler()
  const seen: string[] = []
  function scheduleInner() {
    seen.push('outer')
    scheduler.delay(5, S.task(() => seen.push('inner')))
  }

  scheduler.delay(
    10,
    S.task(scheduleInner),
  )
  advanceTo(100)
  assertEquals(seen, ['outer', 'inner'])
})

Deno.test('defaultScheduler: returns the shared instance', () => {
  assertEquals(S.defaultScheduler(), S.defaultScheduler())
})

Deno.test('newScheduler: runs a task using the host timer', async () => {
  const scheduler = S.newScheduler(S.defaultTimer)
  const before = scheduler.currentTime()
  const ran = await new Promise<S.Time>((resolve) => {
    scheduler.delay(5, S.task(resolve))
  })
  assertEquals(ran >= before + 5, true)
})

Deno.test('a custom timer is the only way the outside world gets in', () => {
  const calls: number[] = []
  const clock = { now: 0 }
  const box: { f: (() => void) | null } = { f: null }
  const timer: S.Timer = {
    now: () => clock.now,
    setTimer: (f, delay) => {
      calls.push(delay)
      box.f = f
      return f
    },
    clearTimer: () => (box.f = null),
  }

  const scheduler = S.newScheduler(timer)
  const seen: S.Time[] = []
  const scheduledTask = S.task((time) => seen.push(time))
  scheduler.delay(10, scheduledTask)
  assertEquals(calls, [10], 'one outstanding call, armed for the soonest')

  scheduler.delay(3, scheduledTask)
  assertEquals(calls, [10, 3], 're-armed when something sooner arrives')

  clock.now = 3
  box.f?.()
  assertEquals(seen, [3])
})

Deno.test('newScheduler: arms again when the host loses a timer', () => {
  let now: S.Time = 0
  const host: { armed: (() => void) | null } = { armed: null }
  const timer: S.Timer = {
    now: () => now,
    setTimer: (f) => {
      host.armed = f
      return f
    },
    clearTimer: () => {
      host.armed = null
    },
  }
  const fire = (): void => {
    const f = host.armed
    if (f === null) throw new Error('no timer is armed')
    host.armed = null
    f()
  }

  const scheduler = S.newScheduler(timer)
  const seen: S.Time[] = []
  scheduler.delay(300, S.task((time) => seen.push(time)))

  // The host dropped the timer without ever calling back. Cloudflare Workers
  // does exactly this when the request that armed it ends.
  host.armed = null
  now = 1000

  scheduler.delay(50, S.task((time) => seen.push(time)))
  assertEquals(host.armed !== null, true, 'the scheduler should arm again')

  now = 1050
  fire()
  assertEquals(seen, [300, 1050])
})
