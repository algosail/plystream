/**
 * Create and combine cleanup handles for subscriptions and scheduled tasks.
 * @module
 */

/**
 * Wrap a cleanup function in a disposable handle. Each disposal calls it again.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * let closed = false
 * function close(): void {
 *   closed = true
 * }
 * const handle = S.disposable(close)
 * S.dispose(handle)
 * // closed => true
 * ```
 */
export function disposable(onDispose: () => void): Disposable {
  return { [Symbol.dispose]: onDispose }
}

/**
 * Create a handle with no cleanup work.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.isDisposable(S.disposeNone()) // => true
 * ```
 */
export function disposeNone(): Disposable {
  return disposable(() => {})
}

/**
 * Run the cleanup and return the same handle.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * let closed = false
 * function close(): void {
 *   closed = true
 * }
 * const handle = S.disposable(close)
 * S.dispose(handle) === handle // => true
 * // closed => true
 * ```
 */
export function dispose(d: Disposable): Disposable {
  d[Symbol.dispose]()
  return d
}

/**
 * Wrap a handle so its cleanup runs at most once.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * let cleanups = 0
 * function cleanup(): void {
 *   cleanups += 1
 * }
 * const handle = S.disposeOnce(S.disposable(cleanup))
 * S.dispose(handle)
 * S.dispose(handle)
 * // cleanups => 1
 * ```
 */
export function disposeOnce(d: Disposable): Disposable {
  let done = false
  return disposable(() => {
    if (done) return
    done = true
    dispose(d)
  })
}

/**
 * Group handles for cleanup in array order. A thrown error stops the remaining cleanups.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * const shut: string[] = []
 * S.dispose(S.disposeAll([
 *   S.disposable(() => shut.push('a')),
 *   S.disposable(() => shut.push('b')),
 * ]))
 * // shut => [ "a", "b" ]
 * ```
 */
export function disposeAll(ds: readonly Disposable[]): Disposable {
  return disposable(() => ds.forEach((d) => dispose(d)))
}

/**
 * Check whether a value has a `Symbol.dispose` property.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * S.isDisposable(S.disposeNone()) // => true
 * S.isDisposable(1) // => false
 * ```
 */
export function isDisposable(value: unknown): value is Disposable {
  return (
    value !== null && value !== undefined && typeof value === 'object' &&
    Symbol.dispose in value
  )
}
