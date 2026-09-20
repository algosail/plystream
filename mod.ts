/**
 * Streams of events and behaviors with a current value.
 * Compose streams with `map` and `filter`, then subscribe or collect their values.
 * Use a virtual scheduler to explore timing without waiting.
 *
 * @example
 * ```ts
 * import * as S from '@algosail/plystream'
 *
 * function double(value: number): number {
 *   return value * 2
 * }
 * const ticks = S.take(3)(S.periodic(10)).map(double)
 * S.simulate(30)(ticks) // => [[10, 20], [20, 40], [30, 60]]
 * ```
 *
 * @module
 */

export * from './disposable.ts'
export * from './scheduler.ts'
export * from './stream.ts'
export * from './behavior.ts'
export { batch } from './transaction.ts'
