/**
 * Group related state updates with {@link batch}.
 * @module
 */

/**
 * A queued notification.
 * @internal
 */
export interface Cell {
  /** Send this notification. */
  readonly emit: () => void
  /** The last update version delivered. */
  sentAt: number
}

let depth = 0
let version = 0
const pending = new Set<Cell>()

/**
 * Create a pending notification.
 * @internal
 */
export function cell(emit: () => void): Cell {
  return { emit, sentAt: -1 }
}

/**
 * Mark a new source update.
 * @internal
 */
export function touch(): void {
  version += 1
}

/**
 * Group synchronous updates so derived observers receive the final combined value.
 * Direct reads see changes immediately. Nested batches flush when the outermost batch finishes.
 *
 * @example
 * ```ts
 * import * as P from '@algosail/ply'
 * import * as S from '@algosail/plystream'
 *
 * const [sch] = S.newVirtualScheduler()
 * const [first, setFirst] = S.bus<string>()
 * const [last, setLast] = S.bus<string>()
 * function fullName(first: string) {
 *   return function withLast(last: string): string {
 *     return `${first} ${last}`
 *   }
 * }
 * const full = P.lift2(fullName)(
 *   S.stepper('', sch)(first),
 * )(S.stepper('', sch)(last))
 *
 * const seen: string[] = []
 * S.observe((v: string) => seen.push(v))(full)
 *
 * setFirst('Alice')
 * setLast('Lavley')
 * // seen: [ " ", "Alice ", "Alice Lavley" ]
 *
 * seen.length = 0
 * S.batch(() => {
 *   setFirst('Bob')
 *   setLast('Hopper')
 * })
 * // seen: [ "Bob Hopper" ]
 * ```
 */
export function batch(f: () => void): void {
  depth += 1
  try {
    f()
  } finally {
    if (depth === 1) {
      try {
        flush()
      } finally {
        depth -= 1
      }
    } else {
      depth -= 1
    }
  }
}

function flush(): void {
  let failure: unknown
  let failed = false

  while (pending.size > 0) {
    const cells = [...pending]
    pending.clear()
    for (const c of cells) {
      if (c.sentAt === version) continue
      c.sentAt = version
      try {
        c.emit()
      } catch (e) {
        if (!failed) {
          failed = true
          failure = e
        }
      }
    }
  }

  if (failed) throw failure
}

/**
 * Queue a notification during a batch, or send it immediately outside one.
 * @internal
 */
export function notifyLater(c: Cell): void {
  if (depth === 0) {
    c.sentAt = version
    c.emit()
    return
  }
  pending.add(c)
}

/**
 * Remove a queued notification.
 * @internal
 */
export function cancelNotification(c: Cell): void {
  pending.delete(c)
}
