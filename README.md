# MAELSTROM

**A unified registry for reactive values, module resolution, dependency coordination, and deferred execution.**

MAELSTROM consolidates value storage, module loading, availability checks, subscriptions, and dependency scheduling into one registry. Each label identifies a `RiftLike` entry, and solver outcomes are recorded against that same entry.

## Core Design

| Component | Responsibility |
|---|---|
| `MAELSTROM` | Main registry, solver coordination, module resolution, and deferred scheduler |
| `Rift<T>` | In-process value slot with open state, claim state, promise-based waiting, and subscriptions |
| `CloudRift<T>` | Value slot backed by the shared `STORE` |
| `REACTIVE_STORE` | Shared record storage and subscriptions for cloud entries |
| `STORE` | Exported singleton instance of `REACTIVE_STORE` |
| `ORC_SHOUT` | Structured console logging with configurable themes |
| `MaybePromise` | Returns defined values directly and wraps `undefined` in a resolved promise |

The exported singleton is `maelstrom`.

## Quick Reference

### Registry and Rift Operations

| Method | Purpose |
|---|---|
| `get(label, store?)` | Get or create a registry entry |
| `read(label, store?)` | Read the current value and record the read |
| `get_value(label)` | Read a label's current value |
| `place(label, value, store?)` | Open a label with a value and record the result |
| `hasRift(label)` | Check whether the registry contains a label |
| `deleteRift(label)` | Remove an entry from the registry |
| `resetRift(label)` | Reset an existing entry |
| `clearRifts()` | Clear all registry entries |
| `size` | Number of registry entries |
| `inspect()` | Inspect entry names, open/claim state, and timestamps |
| `guard(label, fn, timeoutMs?)` | Wait for a value, then pass it to a callback |
| `FULFIL_THIS_NOW(root)` | Replace the machine's root reference |
| `REFRESH_ROOT()` | Attempt to acquire the current root |

### Rift State and Waiting

| Member | Purpose |
|---|---|
| `isOpen` | Whether the entry has been opened |
| `isClaimed` | Whether a solver has claimed the entry |
| `current` | Current value |
| `createdAt` | Entry creation timestamp |
| `openedAt` | Timestamp when the entry was opened |
| `open(value?)` | Open the entry; the first open wins until reset |
| `reset()` | Reset state and create a new wait promise |
| `claim()` | Claim the right to perform a one-time operation |
| `wait(timeoutMs?)` | Wait for the entry to open |
| `onOpen(listener)` | Subscribe to the entry opening; returns an unsubscribe function |

A `Rift` wait defaults to no timeout. Supplying a nonzero timeout makes the wait reject if the entry does not open in time. Resetting creates a new gate for subsequent waits.

### Resolution Solvers

| Method | Purpose |
|---|---|
| `smash_n_grab(label, module?, wait?, store?)` | Resolve a rift, wait for a value, or attempt module resolution |
| `create_rift(label, module, store?)` | Open a rift with a supplied value or module |
| `use_rift(label, wait?, apparation?, store?)` | Resolve an existing value, wait on a claimed entry, or use a fallback |
| `resolve_global_rift(name, wait?, fallback?)` | Resolve an available value or return a fallback |
| `resolve_rift_initial(name, value)` | Resolve a global rift with an initial fallback |
| `has(module)` | Check registry, loaded modules, and import availability |
| `probe(label, filePath, comp_name, fallback)` | Synchronous fallback with a one-time background named-export probe |
| `use_if_available(label, filePath, comp_name, fallback, opts?)` | Return a fallback immediately until the named export becomes available |

`use_if_available` is designed for a synchronous cold path. It claims the label once, starts a background import, and records the resulting export or fallback. Subsequent calls return the opened value directly.

### Optional Module Resolution

| Method | Purpose |
|---|---|
| `conjure(options)` | Resolve an optional module with caching enabled by default |
| `conjure_uncached(options)` | Resolve a module or fallback without using the cached conjuration path |
| `conjure_lib_rift(label, cache?, module?)` | Conjure a module namespace and associate it with a rift |
| `conjure_named_lib_rift(label, comp_name, cache?, module?)` | Conjure a named export and associate it with a rift |
| `conjure_lib_init(label, module?)` | Resolve a module without the cached rift wrapper |
| `conjure_named_lib_init(label, comp_name, module?)` | Resolve a named export without the cached rift wrapper |

`OPTIONAL_IMPORT_OPTIONS<T, F>` supports:

| Option | Default | Purpose |
|---|---|---|
| `moduleName` | — | Module specifier |
| `fallback` | Safe proxy factory | Value or factory used when resolution fails |
| `module` | `true` | Whether to resolve an external module |
| `cache` | `true` | Whether to reuse the cached rift |
| `preferRequire` | `false` | Prefer `require()` when available |
| `importPromise` | — | Supply a promise or value directly |
| `force` | — | Supply a value or factory before normal module resolution |
| `comp_name` | — | Select a named export |
| Additional properties | — | Environment-specific candidates |

The resolver attempts synchronous paths when possible and falls back to dynamic import when necessary. Missing modules can use the safe proxy rather than immediately failing on property access.

### Module Loading and Export Lookup

| Method | Purpose |
|---|---|
| `isLoaded(specifier)` | Check the loaded-module registry entry |
| `getLoaded(specifier)` | Get a previously loaded module |
| `load_module(specifier)` | Dynamically import and cache a module, deduplicating concurrent loads |
| `require_module(specifier)` | Synchronously require and cache a module when available |
| `export_of(name, specifier?)` | Find a named export in one module or across indexed loaded modules |
| `check(moduleName)` | Cache a module availability result as a boolean |

Loaded modules use `mod:` labels; synchronously required modules use `req:` labels. Export indexes and in-flight import promises are stored on their corresponding registry entries.

When `export_of(name)` finds the same export name in multiple loaded modules, it throws an ambiguity error rather than selecting one arbitrarily. Passing `specifier` restricts the lookup to that module.

### Results and Subscriptions

| Method | Purpose |
|---|---|
| `results` | Compute the list of recorded solver results |
| `result(label)` | Get a result record for one label |
| `resultsFrom(solver)` | Filter recorded results by solver |
| `onResult(listener)` | Subscribe to result notifications |
| `wait_for(deps, fn)` | Run a callback when one or more labels open |
| `cancel_wait(id)` | Cancel a pending multi-label waiter |
| `wait_and_promise(timeoutMs, key, isOpen, current)` | Subscribe and wait for a label to open |

Each `ResultRecord` contains:

| Field | Meaning |
|---|---|
| `label` | Registry label |
| `solver` | Solver that recorded the outcome |
| `store` | Store kind |
| `value` | Recorded value |
| `ok` | Whether the value is neither `undefined` nor `null` |
| `openedAt` | Recorded opening timestamp |

Supported solver tags are `get`, `place`, `smash_n_grab`, `create_rift`, `use_rift`, `resolve_global_rift`, `resolve_rift_initial`, `has`, `probe`, `use_if_available`, `conjure`, `check`, `load_module`, `require_module`, `export_of`, `wait_for`, `defer`, `detect_environment`, and `manual`.

`wait_for` accepts a single label or an array of labels. For multiple labels, the callback runs when all dependencies have opened.

### Deferred Execution

| Method | Purpose |
|---|---|
| `defer(deps, fn)` | Register work that runs when all dependencies are open |
| `provide(key, value)` | Publish or replace a dependency value |
| `provide(record)` | Publish multiple dependency values |
| `resolve(key)` | Mark a dependency available with an `undefined` value |
| `invalidate(key)` | Remove a dependency entry |
| `deferHas(key)` | Check whether a dependency is open |
| `deferGet(key)` | Read a dependency's current value |
| `cancel(id)` | Cancel a pending deferred task |
| `deferInspect()` | Inspect pending/running tasks and scheduler statistics |
| `deferClear()` | Cancel pending tasks |

Deferred task callbacks receive dependency values in the same order as the dependency list.

The scheduler tracks these counters:

| Counter | Meaning |
|---|---|
| `registered` | Tasks registered |
| `executed` | Tasks dispatched |
| `completed` | Tasks completed successfully |
| `failed` | Tasks that failed |
| `cancelled` | Pending tasks cancelled |

A task is dispatched once after all dependencies are open. Promise-returning tasks remain tracked until they settle. Clearing or cancelling pending work does not undo work that has already started.

### Environment Detection

| Method or constant | Purpose |
|---|---|
| `detect_environment(env_lib?)` | Scan for the runtime's module, platform, compiler, bundler, and adapters |
| `useEnvState()` | Read the current environment detection state |
| `detectCapability(name)` | Check a supported runtime capability |
| `MODULE_LEVEL_CANDIDATES` | Candidate module-level tools |
| `PLATFORM_CANDIDATES` | Candidate application platforms |
| `COMPILER_CANDIDATES` | Candidate compilers and build tools |
| `BUNDLER_CANDIDATES` | Bundler/package-manager identifiers |
| `ADAPTER_CANDIDATES` | Candidate communication and runtime adapters |
| `CAPABILITY_CANDIDATES` | Listed runtime capability names |

Environment detection runs asynchronously. `useEnvState()` is a read-only snapshot and reports whether detection has completed. Before completion, it returns the current defaults.

The returned environment state contains `moduleLevel`, `platform`, `compilers`, `bundler`, `adapters`, and `ready`. `useEnvState()` also supplies `rt: 'ENV_DETECTION'` and `subChannel: '[CSE_ENV]'`.

### Store Selection

`get(label, store?)` accepts three store kinds:

| Store | Behavior |
|---|---|
| `'local'` | Creates an in-process `Rift` |
| `'cloud'` | Creates a `CloudRift` backed by the shared `STORE` |
| `'auto'` | Detects support once and selects cloud or local storage |

The store kind is used when an entry is first created. Later calls for an existing label return that entry.

`STORE` exposes `get`, `set`, `sub`, `delete`, `clear`, and `size`. Its records are keyed by string, and subscriptions are notified when a record is set.

### Apparation Fallback Factory

`maelstrom.apparation` provides default values, no-op functions, and safe placeholder objects.

| Group | Available members |
|---|---|
| Basic values | `null`, `undefined`, `string`, `number`, `boolean`, `bigint`, `symbol`, `date`, `error` |
| Objects and collections | `object`, `plainObject`, `array`, `map`, `set`, `weakMap`, `weakSet` |
| Binary values | `typedArray`, `dataView`, `arrayBuffer`, `_ArrayBuffer`, `buffer` |
| URL values | `url`, `urlSearchParams` |
| Iteration | `iterable`, `iterator`, `asyncIterable` |
| Promises | `promise`, `noopPromise`, `booleanPromise`, `noopBooleanPromise`, `promiseInstance`, `thenable` |
| Functions | `func`, `funcVoid`, `function_`, `noopFunction`, `void`, `noopVoid` |
| Generic placeholders | `any`, `anyGeneric`, `anyProp`, `anyProp2`, `anyProp3`, `anyAs`, `universal`, `universalNoop`, `simpleNoop` |
| Other | `undefinedString`, `primitive` |

`what_is_this_item_exactly(value)` classifies common values, including thenables, async iterables, functions, arrays, collections, binary objects, dates, regular expressions, errors, URLs, and plain objects.

### Lifecycle

| Method | Purpose |
|---|---|
| `reset()` | Clear the registry, multi-label waiters, deferred tasks, scheduler statistics, cached store detection, and environment state |

`reset()` restores the machine's internal state but does not reset the root reference or clear the exported shared `STORE`.

## Basic Usage

```ts
import { maelstrom } from './maelstrom';

// Store and read a value.
maelstrom.place('config', { enabled: true });

const config = maelstrom.read<{ enabled: boolean }>('config');

// Subscribe to a value becoming available.
const unsubscribe = maelstrom.get('database').onOpen((database) => {
    // Use the resolved database.
});

// Register work that waits for dependencies.
maelstrom.defer(['config', 'database'], (config, database) => {
    // Runs after both entries are open.
});

// Publish a dependency.
maelstrom.provide('database', databaseInstance);

// Inspect recorded outcomes.
const results = maelstrom.results;
```

## Type Reference

| Type | Purpose |
|---|---|
| `RiftLike<T>` | Common interface implemented by local and cloud rifts |
| `RiftRecord<T>` | Shared-store record shape |
| `StoreListener<T>` | Listener for shared-store record updates |
| `StoreKind` | `'local' \| 'cloud' \| 'auto'` |
| `Solver` | Supported result provenance tags |
| `ResultRecord<T>` | Recorded solver outcome |
| `OPTIONAL_IMPORT_OPTIONS<T, F>` | Module-resolution configuration |
| `EnvDetectionState` | Environment detection result |
| `Waiter` | Multi-label waiter record |
| `DeferredTask` | Scheduled task record |
| `DeferredState` | `'pending' \| 'running'` |
| `DeferredStats` | Deferred scheduler counters |
| `MaybePromise<R>` | Value-or-promise shorthand |

## Exports

The module exports:

- `maelstrom` — the singleton `MAELSTROM` instance.
- `STORE` — the shared `REACTIVE_STORE` instance.
- `Rift`, `CloudRift`, and `REACTIVE_STORE` — the three classes.

The type aliases and interfaces are defined in the module but are not explicitly exported in the supplied source.
