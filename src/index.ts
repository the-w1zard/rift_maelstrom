// ═════════════════════════════════════════════════════════════════════════════════════════════════════
//                                                                               ══ MAELSTROM ══
// #region ─────────────────────────────────────────────────────────────────────────────────────
//   · ONE registry  — a single Map<label, Rift|CloudRift>. Every result every solver produces
//                     lives in it: plain values, loaded modules, module checks, conjures,
//                     defer deps, probe outcomes. No parallel results map, no static caches.
//   · ONE subscriber mechanism — each registry entry carries its own subscriber set. Every
//                     waiter, every promise-shaped wait, every results subscription goes
//                     through exactly that. No per-key waiter index, no second listener array.
//   · ONE scheduler state — the deferred task table (tasks, not results).
//
// The solvers, and the problem each one alone goes about solving:
//
//   get / place            — the plain registry read and write
//   smash_n_grab           — the bandit. rift → wait → open → nuke-and-import. Never throws.
//   create_rift            — the founder. Open a rift with a module in one move.
//   use_rift               — the traveler. The full resolution ladder.
//   resolve_global_rift    — the diplomat. Timeout + dignified fallback, no rejection.
//   resolve_rift_initial   — the bootstrap. First-paint value for a rift that may never open.
//   has                    — the scout. Four probes, one boolean.
//   probe / use_if_available — the locally sourced named import. Sync cold path, one
//                              claim-gated background probe ever, bare-read hot path.
//   conjure (+ family)     — the conjurer. Optional module, cached or not, safe proxy.
//   check                  — the verifier. One import per module, ever. Boolean out.
//   load_module / require_module — the porters. Inflight dedupe, export indexing.
//   export_of              — the index clerk. Ambiguous finds throw, never guess.
//   wait_for / signal      — the signal waiter. One list, fires when all deps land.
//   wait_and_promise       — the patient one. Subscribe first, then re-check isOpen.
//   defer (+ engine)       — the scheduler. Drain loop, versioned deps, stats, cancellation.
//   detect_environment     — the cartographer. Candidate lists scanned through check().
//
// Every solver's outcome lands in the SAME registry under the SAME label, and that single
// event simultaneously releases waiters, releases deferred tasks, and notifies subscribers.
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                           TYPE ALIASES (the shorthand table, carried forward)
// #region ─────────────────────────────────────────────────────────────────────────────────────
/**
 * MaybePromise, but only when the value does not equal undefined.
 * A real value returns as-is; an absent value returns a promise that may still land.
 * This one stays outside the class — that is just how this one is, because of how it works.
 */
type MaybePromise<R = any> = R;
function MaybePromise<R = any>(v: R): MaybePromise {
  return v !== undefined ? v as R : Promise.resolve(v) as Promise<R>
}
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                                                        Rift — the local shape
// #region ─────────────────────────────────────────────────────────────────────────────────────
//
// A named slot holding a value that may not have landed yet. In-process. It carries, on
// itself, everything the registry needs from it: the value, the open/claim state, the
// subscriber set (THE one subscription mechanism for this entry), the solver tag of whoever
// produced its value, and — when the entry holds a module — its export index and inflight
// promise. Nothing about a result is stored anywhere else.
class Rift<T = A> implements RiftLike<T> {

    // ── slot state ──────────────────────────────────────────────────────────────────────────────
    private opened: B = false;
    private claimed: B = false;
    private value: OPT<T>;
    private resolveFn!: (v: T) => V;
    private gate: Promise<T>;
    readonly createdAt: N = Date.now();
    openedAt: OPT<N>;

    // ── the ONE subscriber mechanism for this entry (cloud entries inherit it) ──────────────────
    protected listeners: ((v: T) => V)[] = [];

    protected notify(v: T): void {
        for (const l of this.listeners) { try { l(v); } catch { /* listener errors never break the rift */ } }
    }

    // ── provenance + registry bookkeeping (set by MAELSTROM when a solver records an outcome) ───
    solver: OPT<S>;
    store: OPT<S>;
    version: N = 0;
    inflight: OPT<Promise<A>>;
    exports: Map<S, Map<S, A>> = new Map();   // name → (moduleKey → export)  — lives ON the entry

    constructor() { this.gate = this.makeGate(); }

    private makeGate(): Promise<T> {
        return new Promise<T>((resolve) => { this.resolveFn = resolve; });
    }

    /**
     * Default is "wait forever". Explicit timeout = opt-in rejection.
     * A rift that has not landed is not an error — it is a promise.
     */
    wait(timeoutMs: N = 0): T | Promise<T> {
        if (!timeoutMs) return this.gate as T;
        let timer!: ReturnType<typeof setTimeout>;
        const timeout = new Promise<T>((_, reject) => {
            timer = setTimeout(() => reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`)), timeoutMs);
        });
        return Promise.race([this.gate, timeout]).finally(() => clearTimeout(timer)) as Promise<T>;
    }

    open(v?: T): V {
        if (this.opened) return;                     // idempotent — first open wins
        this.opened = true;
        this.openedAt = Date.now();
        this.value = v as T;
        this.resolveFn(v as T);
        this.notify(v as T);
    }

    reset(): V {
        this.opened = false;
        this.claimed = false;
        this.value = undefined;
        this.openedAt = undefined;
        this.gate = this.makeGate();
    }

    /** one solver wins the right to fetch this slot — everyone else just waits for it */
    claim(): B {
        if (this.claimed) return false;
        this.claimed = true;
        return true;
    }

    /** the one subscribe call for this entry */
    onOpen(listener: (v: T) => V): () => V {
        this.listeners.push(listener);
        return () => { const i = this.listeners.indexOf(listener); if (i !== -1) this.listeners.splice(i, 1); };
    }

    get isOpen(): B { return this.opened; }
    get isClaimed(): B { return this.claimed; }
    get current(): OPT<T> { return this.value; }
}
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                                                                REACTIVE_STORE
// #region ─────────────────────────────────────────────────────────────────────────────────────
// Same contract as Rift, but the slot lives in a shared store: writes are visible to every
// reader of the same label, and onOpen subscriptions fire across all holders. Its reactive
// store is the cloud shape's own backbone — one store, one subscription mechanism.
class REACTIVE_STORE {
    private records = new Map<S, RiftRecord<A>>();
    private listeners = new Map<S, Set<StoreListener<A>>>();

    get<T = A>(key: S): RiftRecord<T> | undefined { return this.records.get(key) as RiftRecord<T> | undefined; }
    set<T = A>(key: S, record: RiftRecord<T>): void {
        this.records.set(key, record as RiftRecord<A>);
        const subs = this.listeners.get(key);
        if (subs) for (const l of [...subs]) { try { l(record as RiftRecord<A>); } catch { /* one bad listener never kills the rest */ } }
    }
    sub<T = A>(key: S, fn: StoreListener<T>): () => V {
        let subs = this.listeners.get(key);
        if (!subs) { subs = new Set(); this.listeners.set(key, subs); }
        subs.add(fn as StoreListener<A>);
        return () => {
            const s = this.listeners.get(key);
            if (!s) return;
            s.delete(fn as StoreListener<A>);
            if (!s.size) this.listeners.delete(key);
        };
    }
    delete(key: S): void { this.records.delete(key); this.listeners.delete(key); }
    clear(): void { this.records.clear(); this.listeners.clear(); }
    get size(): N { return this.records.size; }
}
export const STORE = new REACTIVE_STORE();
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                                                   CloudRift — the cloud shape
// #region ─────────────────────────────────────────────────────────────────────────────────────
// ONE location. A CloudRift with a given name IS the pooled resource: the same value, the
// same gate, the same subscriber list as every other CloudRift of that name — because it IS
// the same entry. The machine's registry is the only address book; two tiny hooks wire the
// canonical lookup. No record map, no second subscriber map, no shadowing anywhere.
// ─────────────────────────────────────────────────────────────────────────────────────────────
function wait_and_promise<T = A>(timeoutMs: number, key: A, isOpen: A, current: A,) {
    return new Promise<T>((resolve, reject) => {// subscribe first, THEN re-check isOpen — closes the gap where the
        let timer: ReturnType<typeof setTimeout> | undefined;
        const unsub = STORE.sub<RiftRecord<T>>(key, (next) => {// record opened between the early-return check above and the subscribe.
            if (next?.opened) {
                if (timer) clearTimeout(timer);
                unsub();
                resolve(next.value as T);
            }
        });
        if (isOpen) {// opened while we were subscribing
            if (timer) clearTimeout(timer);
            unsub();
            resolve(current as T);
            return;
        }
        if (timeoutMs) {
            timer = setTimeout(() => {
                unsub();
                reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`));
            }, timeoutMs);
        }
    });
}
class CloudRift<T = A> implements RiftLike<T> {
    private readonly key: string;
    constructor(name: string) {
        this.key = `${name}`;
        if (!STORE.get<RiftRecord<T>>(name)) STORE.set<RiftRecord<T>>(name, this.emptyRecord());
    }
    private emptyRecord(): RiftRecord<T> {
        return { opened: false, claimed: false, value: undefined, createdAt: Date.now(), openedAt: undefined };
    }
    private record(): RiftRecord<T> { return get<RiftRecord<T>>(this.key) ?? this.emptyRecord(); }
    get createdAt() { return this.record().createdAt; }
    get openedAt() { return this.record().openedAt; }
    get isOpen() { return this.record().opened; }
    get isClaimed() { return this.record().claimed; }
    get current(): T | undefined { return this.record().value; }

    wait(timeoutMs = 0) {// : Promise<T> CHANGED: same default contract as Rift
        const rec = this.record();
        if (rec.opened) return rec.value as T;
        else {
            return wait_and_promise(timeoutMs, this.key, this.isOpen, this.current) as T
        }
    }
    public open(value?: T) {
        const rec = this.record();
        if (rec.opened) return; // best-effort idempotency — see claim() note above
        STORE.set<RiftRecord<T>>(this.key, { ...rec, opened: true, value, openedAt: Date.now() });
    }
    public reset() {
        const rec = this.record();
        STORE.set<RiftRecord<T>>(this.key, { ...this.emptyRecord(), createdAt: rec.createdAt });
    }
    public claim(): boolean {
        const rec = this.record();
        if (rec.claimed) return false;
        STORE.set<RiftRecord<T>>(this.key, { ...rec, claimed: true });
        return true;
    }
    public onOpen(listener: RiftListener<T>): () => void {
        return STORE.sub<RiftRecord<T>>(this.key, (next) => { if (next?.opened) listener(next.value as T); });
    }
    
}
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                     ORC_SHOUT — the real logger, carried in whole, unmodified
// #region ─────────────────────────────────────────────────────────────────────────────────────
class ORC_SHOUT {
    c = {
        emerald: { 10: "#002c22", 9: "#004f3b", 8: "#016630", 7: "#007a55", 6: "#009966", 5: "#00bc7d" },
        blue: { 10: "", 9: "", 8: "", 7: "", 6: "", 5: "" },
    };
    t = {
        deep: {
            bg: "#020817", txt: "#f8fafc", size: 12, title_size: 14, title_txt: "#94a3b8",
            payload_key: "#a4ed2c", payload_value: "#f8fafc", font: "monospace",
            border: "2px solid #101727", borderRadius: "6px", padding: "8px 16px", svg: "",
        },
        vintage: {
            bg: "#001B29", txt: "#e9e1ba", size: 12, title_size: 14, title_txt: "#ada785",
            payload_key: "#29c3a0", payload_value: "#fafafa", font: "monospace",
            border: "2px solid #112932", borderRadius: "6px", padding: "8px 16px", svg: "",
        },
        error: {
            bg: "#09090B", txt: "#e7000b", size: 12, title_size: 14, title_txt: "#9f0712",
            payload_key: "#a4ed2c", payload_value: "#f8fafc", font: "monospace",
            border: "2px solid #e7000b", borderRadius: "6px", padding: "8px 16px",
            svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e7000b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
        },
        warn: {
            bg: "#09090B", txt: "#e17100", size: 12, title_size: 14, title_txt: "#973c00",
            payload_key: "#a4ed2c", payload_value: "#f8fafc", font: "monospace",
            border: "2px solid #e17100", borderRadius: "6px", padding: "8px 16px",
            svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e17100" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`,
        },
        success: {
            bg: "#09090B", txt: "#009966", size: 12, title_size: 14, title_txt: "#016630",
            payload_key: "#a4ed2c", payload_value: "#f8fafc", font: "monospace",
            border: "2px solid #009966", borderRadius: "6px", padding: "8px 16px",
            svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#009966" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.801 10A10 10 0 1 1 17 3.335"/><path d="m9 11 3 3L22 4"/></svg>`,
        },
        info: {
            bg: "#09090B", txt: "#155dfc", size: 12, title_size: 14, title_txt: "#193cb8",
            payload_key: "#a4ed2c", payload_value: "#f8fafc", font: "monospace",
            border: "2px solid #155dfc", borderRadius: "6px", padding: "8px 16px",
            svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#155dfc" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
        },
    };
    log(msg: string | string[], p?: any, theme = this.t.deep) {
        const isArray = Array.isArray(msg);
        const title = isArray ? msg[0] : msg;
        const paragraphs = isArray ? msg.slice(1) : [];

        const containerStyle = `
              background: ${theme.bg};
              ${theme.border ? `border: ${theme.border};` : ""}
              ${theme.borderRadius ? `border-radius: ${theme.borderRadius};` : ""}
              ${theme.padding ? `padding: ${theme.padding};` : ""}
              font-family: ${theme.font};
              display: block;
              margin: 4px 0;
          `;

        const titleWithIcon = theme?.svg ? `${theme?.svg} ${title}` : title;
        const titleStyle = `
              color: ${theme.title_txt};
              font-size: ${theme.title_size}px;
              font-weight: bold;
              font-family: ${theme.font};
              display: flex;
              align-items: center;
              gap: 8px;
          `;

        const paragraphStyle = `
              color: ${theme.txt};
              font-size: ${theme.size}px;
              font-family: ${theme.font};
              margin-top: 4px;
          `;

        const keyStyle = `
              color: ${theme.payload_key};
              font-weight: bold;
              font-family: ${theme.font};
          `;
        const valueStyle = `
              color: ${theme.payload_value};
              font-family: ${theme.font};
          `;

        let payloadStr = "";
        if (p && typeof p === "object") {
            const entries = Object.entries(p);
            payloadStr = entries.map(([key, value]) => `%c${key}: %c${JSON.stringify(value, null, 2)}`).join("\n");
        }

        const logParts = [`%c${titleWithIcon}`, ...paragraphs.map((p) => `%c${p}`), payloadStr ? `\n${payloadStr}` : ""].filter(Boolean).join("\n");

        const styles = [
            containerStyle + titleStyle,
            ...paragraphs.map(() => paragraphStyle),
            ...(payloadStr ? [keyStyle, valueStyle] : []),
        ];

        console.log(logParts, ...styles);
    }
}
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                                  CLASS 3 · MAELSTROM — the everything machine
// #region ─────────────────────────────────────────────────────────────────────────────────────
//
// ONE registry. ONE subscriber mechanism. ONE scheduler state. Every helper that used to
// litter the lineage's files — blindImport, blindRequire, get_module, has_local, key_for,
// the safe proxy, fallback normalization, environment detection — lives INSIDE this class.
class MAELSTROM {
    public root: A = null;
    // ══ THE ONE REGISTRY ═══════════════════════════════════════════════════════════════════════
    // Every result every solver produces lives here, keyed by label. Rift values, loaded
    // modules (mod:…), sync-required modules (req:…), module checks (check:…), conjures,
    // defer deps, probe outcomes — all of it. There is no second results map anywhere.
    private rifts = new Map<S, RiftLike<A>>();
    // multi-dep waiters only — a waiter on ONE label is an entry subscription (Rift.onOpen);
    // only waiters spanning MULTIPLE labels need a table row. No parallel subscriber lists.
    private multiWaiters = new Set<Waiter>();
    private nextWaiterId = 0;
    // ══ THE ONE SCHEDULER STATE ═════════════════════════════════════════════════════════════════
    // Deferred tasks (not results — the results ARE the registry entries above).
    private deferTasks = new Map<N, DeferredTask>();
    private nextDeferId = 0;
    private draining = false;
    private drainAgain = false;
    private deferStats: DeferredStats = { registered: 0, executed: 0, completed: 0, failed: 0, cancelled: 0 };
    private autoCloud: B | undefined = undefined;
    private warnedUseBeforeReady = false;
    private ENV_STATE: EnvDetectionState = { moduleLevel: null, platform: null, compilers: [], bundler: null, adapters: [], ready: false };
    private shout = new ORC_SHOUT();
    constructor(root: A = null) {
        this.root = root;
        this.RITUAL_OF_BINDING();
    }
    public FULFIL_THIS_NOW(r: A): V { this.root = r; }
    public REFRESH_ROOT(): A {
        try { return (this.root as A)?.acquire_root?.('MAELSTROM') ?? this.root; }
        catch { return this.root; }
    }
    private RITUAL_OF_BINDING(): V { /* the machine binds itself — there is nothing else to import */ }
    // ── logging through ORC_SHOUT — never a stand-in ───────────────────────────────────────────
    private log(where: S, detail: R = {}, type: 'info' | 'success' | 'warn' | 'error' = 'info'): void {
        const payload: R = { ...detail };
        if (payload.error instanceof Error) payload.error = payload.error.message;
        this.shout.log(`[MAELSTROM][${where}]`, payload, this.shout.t[type] ?? this.shout.t.deep);
    }/** dynamic import with no static dependency — works in every module system */
    private static readonly blindImport = new Function('specifier', 'return import(specifier)') as (s: string) => Promise<any>;
    /** require with no static dependency — null where require does not exist (browser/ESM) */
    private static readonly blindRequire: ((s: string) => A) | null =  (typeof require !== 'undefined') ? (new Function('specifier', 'return require(specifier)') as (s: string) => A) : null;
    private static readonly has_require: B = MAELSTROM.blindRequire !== null;
    private static readonly hasNode: B = typeof process !== 'undefined' && !!(process as any)?.versions?.node;
    private static readonly hasBun: B = typeof process !== 'undefined' && !!(process as any)?.versions?.bun;
    /** resolve a specifier relative to THIS file, always. Absolute out. */
    private key_for(specifier: S): S {
        try { return (import.meta as any).resolve(specifier) as S; }
        catch { try { return new URL(specifier, import.meta.url).href; } catch { return specifier; } }
    }/** does a local file exist, resolved from cwd — the has_local probe */
    private has_local(filePath: S | undefined, module?: S): B {
        try {
            if (!MAELSTROM.hasNode && !MAELSTROM.hasBun) return false;
            if (!filePath) {
                this.log('HAS_LOCAL', { note: 'FILE PATH UNDEFINED — DEFINE A PATH', module, type: 'SYNC' }, 'error');
                return false;
            }
            const path = MAELSTROM.blindRequire!('path');
            const fs = MAELSTROM.blindRequire!('fs');
            const resolved = path.resolve(process.cwd(), filePath);
            return fs.existsSync(resolved) as B;
        } catch (e: A) {
            this.log('HAS_LOCAL', { module, type: 'SYNC', error: e }, 'error');
            return false;
        }
    }/**
     * get_module: require the specifier → local file read → builtin check → cwd-resolved
     * require → async import as the last resort. Every sync path is tried before anything
     * returns a promise.
     */
    private get_module<T = A>(module: S, filePath?: S): T | Promise<T> {
        const br = MAELSTROM.blindRequire;
        try {
            let v: A = null;

            try { v = br!(module); return MaybePromise(v) as T; }
            catch (e: A) { this.log('GET_MODULE', { module, type: 'SYNC', import_used: 'blindRequire', error: e }, 'error'); }

            if (module && this.has_local(filePath, module)) {
                const fs = br!('fs');
                const path = br!('path');
                const resolved = path.resolve(process.cwd(), filePath as S);
                if (fs.existsSync(resolved)) { v = fs.readFileSync(resolved, 'utf8').trim(); return MaybePromise(v) as T; }
            }

            if (this.has_local(filePath)) {
                const path = br!('path');
                const resolved = path.resolve(process.cwd(), filePath as S);
                try { v = br!(resolved); return MaybePromise(v) as T; }
                catch (e: A) { this.log('GET_MODULE', { module, filePath, type: 'SYNC', import_used: 'local', error: e }, 'error'); }
            }

            if (MAELSTROM.hasNode || MAELSTROM.hasBun) {
                const fs = br!('fs');
                const { isBuiltin } = br!('node:module');
                if (isBuiltin(module)) {
                    try { v = br!(module); return MaybePromise(v) as T; }
                    catch (e: A) { this.log('GET_MODULE', { module, type: 'SYNC', import_used: 'builtin', error: e }, 'error'); }
                }
                try {
                    const resolved = (br as A).resolve(module, { paths: [process.cwd()] });
                    if (fs.existsSync(resolved)) { v = br!(resolved); return MaybePromise(v) as T; }
                } catch (e: A) { this.log('GET_MODULE', { module, type: 'SYNC', import_used: 'cwd_resolve', error: e }, 'error'); }
            }

            this.log('GET_MODULE', { note: 'SYNC PATHS EXHAUSTED — ENTERING ASYNC LAST RESORT', module, type: 'SYNC' }, 'success');
            if (module) return Promise.resolve().then(() => MAELSTROM.blindImport(module)) as Promise<T>;
        } catch (e: A) {
            this.log('GET_MODULE', { module, type: 'SYNC', error: e }, 'error');
        }
        return undefined as A;
    }
    private is_require_available(): B { return MAELSTROM.blindRequire !== null; }
    private normalizeFallback<F>(fallback: F | (() => F) | undefined, fallbackDefault: () => F): () => F {
        if (typeof fallback === 'function') return fallback as unknown as () => F;
        if (fallback !== undefined) return () => fallback;
        return fallbackDefault;
    }/** the safe proxy — missing module access is suppressed, never crashed */
    private createSafeProxy = (): A =>
        new Proxy(function noop() {}, {
            get: (_, prop) => {
                if (prop === Symbol.toPrimitive) {
                    this.log('SAFE_PROXY', { note: `Missing module. Suppressed access to: ${String(prop)}` }, 'warn');
                    return () => '';
                }
                if (prop === 'valueOf' || prop === 'toString') {
                    this.log('SAFE_PROXY', { note: `Missing module. Suppressed access to: ${String(prop)}` }, 'warn');
                    return () => '[MAELSTROM:missing-module]';
                }
                if (prop === 'then' || prop === Symbol.iterator) return undefined;
                const noop = () => {};
                return new Proxy(noop, {
                    get: () => noop,
                    apply: () => this.createSafeProxy(),
                    construct: () => this.createSafeProxy(),
                });
            },
            apply: () => this.createSafeProxy(),
            construct: () => this.createSafeProxy(),
        });
    private toNamespace(m: A): A {
        if (m && typeof m === 'object') return m;
        return { default: m };
    }
    private extractNamed(ns: A, comp_name: S): A {
        const n = this.toNamespace(ns);
        return n[comp_name] ?? n.default?.[comp_name];
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // THE REGISTRY — every solver reads and writes through here, and nowhere else
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public rf(k: A): S { return `${k}_RIFTS_`; }
    private make<T = A>(name: S, store: StoreKind = 'local'): RiftLike<T> {
        if (store === 'cloud') return new CloudRift<T>(name) as RiftLike<T>;
        if (store === 'auto') {
            if (this.autoCloud === undefined) this.autoCloud = this.detectCloudSupport();
            return (this.autoCloud ? new CloudRift<T>(name) : new Rift<T>()) as RiftLike<T>;
        }
        return new Rift<T>() as RiftLike<T>;
    }/** true when a shared store can actually round-trip */
    private detectCloudSupport(): B {
        const g: A = typeof globalThis !== 'undefined' ? globalThis : {};
        return !!(g.window && g.window !== g) || typeof (g as A).document !== 'undefined';
    }
    public get<T = A>(name: S, store: StoreKind = 'local'): RiftLike<T> {
        if (!this.rifts.has(name)) this.rifts.set(name, this.make<T>(name, store));
        return this.rifts.get(name)! as RiftLike<T>;
    }
    public hasRift(name: S): B { return this.rifts.has(name); }
    public deleteRift(name: S): B { return this.rifts.delete(name); }
    public resetRift(name: S): V { this.rifts.get(name)?.reset(); }
    public clearRifts(): V { this.rifts.clear(); }
    public get size(): N { return this.rifts.size; }
    public inspect(): R {
        return [...this.rifts.entries()].map(([name, rift]) => ({
            name,
            isOpen: rift.isOpen,
            isClaimed: rift.isClaimed,
            createdAt: rift.createdAt,
            openedAt: rift.openedAt,
        })) as A;
    }/** guard — wait for a rift, then run a fn on its unlock value */
    public guard<T, RR>(name: S, fn: (unlockValue: T) => RR | Promise<RR>, timeoutMs?: N): Promise<RR> {
        return (async (): Promise<RR> => {
            const val = await this.get<T>(name).wait(timeoutMs) as T;
            return fn(val);
        })();
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // THE SHARED RESULTS SURFACE — a view onto the ONE registry, not a second map
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    /**
     * Record an outcome. This is the single funnel: it tags the registry entry, notifies the
     * entry's subscribers, advances every waiter on the label, and releases deferred tasks.
     * It writes to the ONE registry — it creates no maps.
     */
    private record<T>(label: S, solver: Solver, value: T | undefined, store: StoreKind = 'local'): void {
        const rift = this.get(label, store) as RiftLike<T> & { solver?: S; store?: S; version: N };
        rift.solver = solver;
        rift.store = store;
        rift.version++;
        const rec: ResultRecord<T> = {
            label, solver, store, value,
            ok: value !== undefined && value !== null,
            openedAt: rift.openedAt ?? Date.now(),
        };
        rift.open(value as T);                       // the entry itself notifies its own subscribers
        this.get<ResultRecord>('__results__').reset();      // sentinel re-fires per record (open is idempotent)
        this.get<ResultRecord>('__results__').open(rec);   // machine-level watchers ride the same mechanism
        this.releaseMultiWaiters(label, value);
        this.drainDefer(label);
    }/** the results view — computed from the ONE registry, stored nowhere */
    public get results(): RO<Array<ResultRecord>> {
        const out: ResultRecord[] = [];
        for (const [label, rift] of this.rifts) {
            const r = rift as RiftLike<A> & { solver?: S; store?: S };
            if (r.solver === undefined) continue;
            out.push({
                label,
                solver: r.solver as Solver,
                store: (r.store ?? 'local') as StoreKind,
                value: rift.current,
                ok: rift.current !== undefined && rift.current !== null,
                openedAt: rift.openedAt ?? rift.createdAt,
            });
        }
        return out;
    }
    public result<T = A>(label: S): ResultRecord<T> | undefined {
        const rift = this.rifts.get(label) as (RiftLike<A> & { solver?: S; store?: S }) | undefined;
        if (!rift || rift.solver === undefined) return undefined;
        return {
            label,
            solver: rift.solver as Solver,
            store: (rift.store ?? 'local') as StoreKind,
            value: rift.current,
            ok: rift.current !== undefined && rift.current !== null,
            openedAt: rift.openedAt ?? rift.createdAt,
        } as ResultRecord<T>;
    }
    public resultsFrom(solver: Solver): ResultRecord[] {
        return this.results.filter((r) => r.solver === solver);
    }/** "any result, any solver" — rides the ONE mechanism: the sentinel entry's own subscription */
    public onResult(listener: (r: ResultRecord) => V): () => V {
        return this.get<ResultRecord>('__results__').onOpen(() => listener(this.result('__results__') as ResultRecord));
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · get / place — the plain registry read and write
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public read<T = A>(label: S, store: StoreKind = 'local'): T | undefined {
        const rift = this.get<T>(label, store);
        const v = rift.current;
        this.record(label, 'get', v as T, store);
        return v as T | undefined;
    }
    public get_value<T = A>(label: S): T | undefined { return this.read<T>(label); }
    public place<T = A>(label: S, value: T, store: StoreKind = 'local'): V {
        this.get<T>(label, store).open(value);
        this.record(label, 'place', value, store);
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · smash_n_grab — the bandit. Never throws, always returns a usable envelope.
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public smash_n_grab<T = A>(label: S, module?: A, wait?: N, store: StoreKind = 'local'): A {
        let v: A;
        try {
            v = this.get<T>(label, store);
            if (v.isOpen) {                                                          // check rift registry, return current
                try { return MaybePromise({ is_waiting: false, isLoaded: true, get: (v as RiftLike<T>).current as T }); }
                catch (e: A) { this.log('SMASH_N_GRAB', { msg: '[CHECK RIFT REG, RETURN CURRENT][FAILED][MOVING ON...]', error: e }, 'error'); }
            }
            if (!module && wait) {                                                   // no module but a wait → wait for it
                try {
                    this.log('SMASH_N_GRAB', { msg: '[NO MODULE BUT WAIT SUPPLIED — WAITING]' }, 'info');
                    const raw = (v as RiftLike<T>).wait(wait) as A;
                    const envelope = (w: A) => MaybePromise({ is_waiting: !w, isLoaded: module ? this.hasRift(module as S) : false, mod: w });
                    if (raw instanceof Promise) {                                    // a rejected wait is a miss, not a crash
                        return MaybePromise((raw as Promise<A>).then(
                            (w) => envelope(w),
                            (e) => { this.log('SMASH_N_GRAB', { msg: '[WAIT TIMED OUT — RETURNING MISS]', error: e }, 'warn'); return envelope(undefined); },
                        ));
                    }
                    return envelope(raw);
                } catch (e: A) { this.log('SMASH_N_GRAB', { msg: '[WAIT PATH FAILED][MOVING ON...]', error: e }, 'error'); }
            }
            if (label && module && !wait) {                                          // label + module, no wait → open
                try { return MaybePromise({ is_waiting: false, isLoaded: true, mod: (v as RiftLike<T>).open(module as T) }); }
                catch (e: A) {                                                       // all else fails — nuke it
                    const value = this.get_module<T>(module as A);
                    this.log('SMASH_N_GRAB', { msg: 'try failed, attempting catch gate now...', error: e }, 'error');
                    (v as RiftLike<T>).open(value as T);
                    this.record(label, 'smash_n_grab', value as T, store);
                    return MaybePromise({ is_waiting: false, isLoaded: true, mod: value });
                }
            }
        } catch (e: A) {
            this.log('SMASH_N_GRAB', { error: e }, 'error');
        }
        const ap = (this.apparation.func as <T = A>() => () => T)();
        return MaybePromise({ is_waiting: false, isLoaded: false, mod: ap });
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · create_rift — the founder
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public create_rift<T = A>(label: S, module: T, store: StoreKind = 'local'): A {
        try {
            if (module) {
                this.get<T>(label, store).open(module);
                this.record(label, 'create_rift', module, store);
                return MaybePromise(module);
            }
        } catch (e: A) {
            const value = this.get_module<T>(module as A);
            if (value && !(value instanceof Promise)) {
                this.get<T>(label, store).open(value);
                this.record(label, 'create_rift', value, store);
                return MaybePromise(value);
            }
            this.log('CREATE_RIFT', { error: e, msg: 'try failed, attempting catch gate now...' }, 'error');
        }
        return undefined;
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · use_rift — the traveler. The full resolution ladder.
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public use_rift<T = A>(label: S, wait?: N, apparation?: T, store: StoreKind = 'local'): A {
        try {
            const rift = this.get<T>(label, store);

            if (rift.isOpen) {
                this.record(label, 'use_rift', rift.current as T, store);
                return MaybePromise(rift.current as T);
            }

            if (rift.isClaimed) {
                try { return MaybePromise(rift.wait(wait) as A); }
                catch (e: A) { this.log('USE_RIFT', { msg: 'try failed...', error: e }, 'error'); }
            }

            const value = this.get_module<T>(label);
            if (value && !(value instanceof Promise)) {
                rift.open(value);
                this.record(label, 'use_rift', value as T, store);
                return MaybePromise(value);
            }

            this.record(label, 'use_rift', apparation, store);
            return apparation as T;
        } catch (e: A) {
            this.log('USE_RIFT', { error: e }, 'error');
            return apparation as T;
        }
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · resolve_global_rift / resolve_rift_initial — the diplomat and the bootstrap
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public resolve_global_rift<T = A>(name: S, wait: N = 20000, fallback?: T): A {
        try {
            const rift = this.get<T>(name);
            if (rift.isOpen) return rift.current as T;
            if (rift.isClaimed) {
                try { return MaybePromise(rift.wait(wait) as A); }
                catch (e: A) { this.log('RESOLVE_GLOBAL_RIFT', { error: e }, 'error'); return fallback as T; }
            }
            return fallback as T;
        } catch (e: A) {
            this.log('RESOLVE_GLOBAL_RIFT', { error: e }, 'error');
            return fallback as T;
        }
    }
    public resolve_rift_initial<T = A>(name: S, v: T): A {
        try { return this.resolve_global_rift<T>(name, 15000, v); }
        catch (e: A) { this.log('RESOLVE_RIFT_INITIAL', { error: e }, 'error'); return (e as A)?.error ?? v; }
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · has — the scout. Four probes, one boolean.
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public has(module: S): B {
        if (this.hasRift(module)) return true;
        if (this.isLoaded(module)) return true;
        if (this.get<A>(module).isOpen) return true;
        try { MAELSTROM.blindRequire!(module); return true; }
        catch {
            try { void MAELSTROM.blindImport(module); return true; }
            catch { return false; }
        }
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · probe + use_if_available — the locally sourced named import
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //
    // Cold path: returns `fallback` synchronously. ZERO await. ZERO promise.
    // One probe per label, ever — every later call is a bare property read.
    // The background import resolves the rift; every subsequent call sees the real value.
    public probe<T = A>(label: S, filePath: S, comp_name: S, fallback: T): T {
        return this.use_if_available<T>(label, filePath, comp_name, fallback);
    }/**
     * The named-export form of use(). `label` is the rift key — one probe per name, ever.
     * `filePath` resolves relative to this file (the "locally sourced" anchor).
     * Cold: sync fallback. Hot: the real named export, as a plain property read.
     */
    public use_if_available<T = A>(label: S, filePath: S, comp_name: S, fallback: T, opts?: { store?: StoreKind }): T {
        const rift = this.get<T>(label, opts?.store ?? 'local');

        if (rift.isOpen) return rift.current as T;                             // hot path — done, sync

        if (rift.claim()) {                                                    // one-time fire-and-forget probe
            void (async () => {
                try {
                    const mod = await MAELSTROM.blindImport(this.key_for(filePath));
                    const named = mod?.[comp_name] !== undefined ? mod[comp_name] : fallback;
                    rift.open(named);
                    this.record(label, 'use_if_available', named, opts?.store ?? 'local');
                } catch {
                    rift.open(fallback);
                    this.record(label, 'use_if_available', fallback, opts?.store ?? 'local');
                }
            })();
        }

        return fallback;                                                       // cold path — instant
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · conjure — the conjurer. The full cached/uncached pair.
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    /** cached conjuration — one registry entry per moduleName, claimed once, waited on forever after */
    public conjure<T = A, F = A>(options: OPTIONAL_IMPORT_OPTIONS<T, F>): A {
        const { moduleName, fallback: rawFallback, module = true, cache = true, preferRequire = false, importPromise, ...userEnvImports } = options;
        const fallback = this.normalizeFallback<F>(rawFallback, this.createSafeProxy as unknown as () => F);

        if (!moduleName || !cache) {
            return this.conjure_uncached<T, F>({ moduleName, fallback: fallback(), module, preferRequire, importPromise, ...userEnvImports } as OPTIONAL_IMPORT_OPTIONS<T, F>);
        }

        const rift = this.get<T | F>(moduleName) as Rift<T | F>;
        if (rift.isOpen) return rift.wait();
        if (!rift.claim()) return rift.wait();

        const result = this.conjure_uncached<T, F>({ moduleName, fallback, module, preferRequire, importPromise, ...userEnvImports } as OPTIONAL_IMPORT_OPTIONS<T, F>);
        const settle = (resolved: A): A => { rift.open(resolved); this.record(moduleName, 'conjure', resolved); return resolved; };
        return MaybePromise(result instanceof Promise ? result.then(settle) : settle(result));
    }/** the agnostic auto importer: if it doesn't exist, it doesn't crash */
    public conjure_uncached<T = A, F = A>(options: OPTIONAL_IMPORT_OPTIONS<T, F>): A {
        const { moduleName, fallback: rawFallback, comp_name, module = true, preferRequire = false, importPromise, force, ...userEnvImports } = options;
        const fallback = this.normalizeFallback<F>(rawFallback, this.createSafeProxy as unknown as () => F);
        const forceNode = force !== undefined;

        const state = this.ENV_STATE;
        const priorityKeys: (S | null)[] = state.ready
            ? [...state.adapters, ...state.compilers, state.moduleLevel, state.platform, state.bundler]
            : [];

        let syncResult: A = undefined;
        let needsAsync = false;

        if (!module || (importPromise && !moduleName)) {
            if (importPromise && typeof (importPromise as A)?.then !== 'function') {
                syncResult = importPromise;
            } else if (importPromise) {
                needsAsync = true;
            } else {
                for (const value of Object.values(userEnvImports)) {
                    if (value && typeof value === 'object' && typeof (value as A).then === 'function') { needsAsync = true; break; }
                    if (value !== null && value !== undefined) { syncResult = value; break; }
                }
                if (!needsAsync && syncResult === undefined) syncResult = fallback();
            }
        } else {
            for (const key of priorityKeys) {
                if (key && userEnvImports[key as S]) {
                    const v = userEnvImports[key as S];
                    if (v && typeof v === 'object' && typeof (v as A).then === 'function') { needsAsync = true; break; }
                    if (v !== null && v !== undefined) { syncResult = v; break; }
                }
            }

            if (!needsAsync && syncResult === undefined && userEnvImports.default) {
                const v = userEnvImports.default;
                if (v && typeof v === 'object' && typeof (v as A).then === 'function') needsAsync = true;
                else if (v !== null && v !== undefined) syncResult = v;
            }

            if (!needsAsync && syncResult === undefined && moduleName) {
                if (forceNode) {
                    try {
                        const res = typeof force === 'function' ? (force as unknown as () => F)() : force;
                        if (res !== null && res !== undefined) syncResult = res;
                        else needsAsync = true;
                    } catch { needsAsync = true; }
                } else if (comp_name) {
                    if (this.is_require_available()) {
                        try {
                            const res = MAELSTROM.blindRequire!(moduleName);
                            if (res && res[comp_name] !== undefined) syncResult = res[comp_name];
                            else needsAsync = true;
                        } catch { needsAsync = true; }
                    } else needsAsync = true;
                } else if (preferRequire && MAELSTROM.blindRequire) {
                    try {
                        const res = MAELSTROM.blindRequire(moduleName);
                        if (res !== null && res !== undefined) syncResult = res;
                        else needsAsync = true;
                    } catch { needsAsync = true; }
                } else if (this.is_require_available() && MAELSTROM.blindRequire) {
                    try {
                        const res = MAELSTROM.blindRequire!(moduleName);
                        if (res !== null && res !== undefined) syncResult = res;
                        else needsAsync = true;
                    } catch { needsAsync = true; }
                } else needsAsync = true;
            }

            if (!needsAsync && syncResult === undefined) syncResult = fallback();
        }

        if (syncResult) return MaybePromise(syncResult);

        // async last resort — promise-driven with a 10s opt-out
        return MaybePromise(new Promise<T>((resolve, reject) => {
            let timer: ReturnType<typeof setTimeout> | undefined;
            const done = (v: A) => { if (timer) clearTimeout(timer); this.record(moduleName ?? '(anon)', 'conjure', v); resolve(v as T); };

            if (!module || (importPromise && !moduleName)) {
                if (importPromise) { done(importPromise); return; }
                for (const value of Object.values(userEnvImports)) {
                    if (value && typeof value === 'object' && typeof (value as A).then === 'function') { done(value); return; }
                }
            }

            if (moduleName) {
                if (comp_name) {
                    MAELSTROM.blindImport(moduleName).then((res: A) => {
                        if (res && res[comp_name] !== undefined) done(res[comp_name]);
                        else done(fallback());
                    }).catch(() => done(fallback()));
                    return;
                }
                MAELSTROM.blindImport(moduleName).then((res: A) => {
                    if (res !== null && res !== undefined) done(res);
                    else done(fallback());
                }).catch(() => done(fallback()));
                return;
            }

            if (userEnvImports.default) { done(userEnvImports.default); return; }

            if (timer) clearTimeout(timer);
            timer = setTimeout(() => reject(new Error(`[rift] wait() timed out after 10000ms`)), 10000);
        }));
    }/** conjure a rift for a whole module namespace */
    public conjure_lib_rift<T = A>(label: S, cache: B = true, module: B = true): A {
        const rift = this.get<T>(label);
        if (rift.isOpen) return rift;
        const raw = this.conjure<T>({ moduleName: label, module, cache });
        const result = MaybePromise(raw);
        const resolve = (resolved: A): A => {
            if (resolved === undefined || resolved === null) return resolved;
            rift.open(resolved);
            this.record(label, 'conjure', resolved);
            return (resolved as A)?.[label];
        };
        return MaybePromise(result instanceof Promise ? result.then(resolve) : resolve(result));
    }/** conjure a rift for a single named export */
    public conjure_named_lib_rift<T = A>(label: S, comp_name: S, cache: B = true, module: B = true): A {
        const rift = this.get(label);
        if (rift.isOpen) return rift;
        const raw = this.conjure({ moduleName: label, module, cache, comp_name });
        const result = MaybePromise(raw);
        const resolve = (resolved: A): A => {
            if (resolved === undefined || resolved === null) { this.log('CONJURE_NAMED', { msg: 'module_not_found', label }, 'warn'); return resolved; }
            if (typeof resolved !== 'object' || !(comp_name in resolved)) { this.log('CONJURE_NAMED', { msg: 'export_not_found', label, comp_name }, 'warn'); return resolved; }
            rift.open(resolved);
            this.record(label, 'conjure', resolved);
            return (resolved as A)[comp_name];
        };
        return MaybePromise(result instanceof Promise ? result.then(resolve) : resolve(result));
    }/** init-level conjuration: the module itself, no default candidate, no string leak */
    public conjure_lib_init(label: S, module: B = true): A {
        return this.conjure_uncached({ moduleName: label, module } as OPTIONAL_IMPORT_OPTIONS);
    }
    public conjure_named_lib_init<T = A>(label: S, comp_name: S, module: B = true): A {
        return this.conjure_uncached<T>({ moduleName: label, module, comp_name } as OPTIONAL_IMPORT_OPTIONS<T>);
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · check — the verifier. One probe per module, ever. Boolean out.
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public async check(moduleName: S): Promise<B> {
        const label = `check:${moduleName}`;
        const rift = this.get<B>(label) as Rift<B>;

        if (rift.claim()) {
            // check does the ONLY importing anywhere in this flow; open() just records the outcome
            (async () => {
                const result = await this.conjure_uncached<A>({ moduleName, fallback: () => null } as OPTIONAL_IMPORT_OPTIONS);
                const ok = result != null;
                rift.open(ok);
                this.record(label, 'check', ok);
            })();
        }

        return rift.wait() as Promise<B>;
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · load_module / require_module / export_of — the porters and the index clerk
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //
    // The module cache IS the registry: loaded modules live under `mod:` labels, sync-required
    // ones under `req:` labels, with the export index stored ON the entry itself. Inflight
    // dedupe lives on the entry too. No separate maps.
    public isLoaded(specifier: S): B { return this.get(`mod:${this.key_for(specifier)}`).isOpen; }
    public getLoaded<T = A>(specifier: S): OPT<T> { return this.get(`mod:${this.key_for(specifier)}`).current as T | undefined; }
    /**
     * Find a named export across every loaded module.
     * Ambiguous finds throw — the index never silently guesses.
     */
    public export_of<T = A>(name: S, specifier?: S): OPT<T> {
        if (specifier !== undefined) return (this.getLoaded<A>(specifier) as A)?.[name] as T | undefined;
        const sources = new Map<S, A>();
        for (const [label, rift] of this.rifts) {
            if (!label.startsWith('mod:') || !rift.isOpen) continue;
            const idx = (rift as A).exports as Map<S, Map<S, A>>;
            const found = idx?.get(name);
            if (found) for (const [k, v] of found) sources.set(k, v);
        }
        if (sources.size === 0) return undefined;
        if (sources.size > 1) throw new Error(`Ambiguous export "${name}" found in: ${[...sources.keys()].join(', ')}`);
        return sources.values().next().value as T | undefined;
    }
    private indexExports(rift: A, key: S, mod: A): void {
        if (!mod || (typeof mod !== 'object' && typeof mod !== 'function')) return;
        for (const name of Object.keys(mod)) {
            let sources = rift.exports.get(name);
            if (!sources) { sources = new Map<S, A>(); rift.exports.set(name, sources); }
            sources.set(key, (mod as A)[name]);
        }
    }/** async load with inflight dedupe — same key always resolves to the same promise */
    public load_module<T = A>(specifier: S): Promise<T> {
        const key = this.key_for(specifier);
        const rift = this.get(`mod:${key}`) as Rift<T> & { inflight?: Promise<A> };
        if (rift.isOpen) return Promise.resolve(rift.current as T);
        if (rift.inflight) return rift.inflight as Promise<T>;
        const p = MAELSTROM.blindImport(key).then((mod: A) => {
            rift.open(mod as T);
            this.indexExports(rift, key, mod);
            rift.inflight = undefined;
            this.record(`mod:${key}`, 'load_module', mod);
            return mod as T;
        }).catch((err: A) => {
            rift.inflight = undefined;               // let it retry later
            throw err;
        });
        rift.inflight = p as A;
        return p;
    }/** the porter's sync twin — blind require, cached, indexed */
    public require_module<T = A>(specifier: S): T {
        const rift = this.get(`req:${specifier}`) as Rift<T>;
        if (rift.isOpen) return rift.current as T;
        if (!MAELSTROM.blindRequire) {               // ESM / browser — no require, no crash
            this.log('REQUIRE_MODULE', { msg: 'require unavailable in this module system — returning undefined', specifier }, 'warn');
            return undefined as T;
        }
        const mod = MAELSTROM.blindRequire(specifier) as T;
        rift.open(mod);
        this.indexExports(rift, specifier, mod);
        this.record(`req:${specifier}`, 'require_module', mod);
        return mod;
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · wait_for / signal — the signal waiter. ONE list.
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    /**
     * register a waiter over one or more signal keys; fires when ALL of them have fired.
     * ONE label → the waiter IS an entry subscription (Rift.onOpen). No parallel list.
     * MANY labels → one row in the multi-waiter table, released by each entry's own
     * subscription firing. Every subscribe call goes through the entry, always.
     */
    public wait_for(deps: S | S[], fn: (v?: A) => V): N {
        const keys = [...new Set(Array.isArray(deps) ? deps : [deps])];
        const id = ++this.nextWaiterId;

        if (keys.length === 1) {                                    // single-label → pure subscription
            const unsubs: (() => V)[] = [];
            const rift = this.get<A>(keys[0]);
            if (rift.isOpen) { fn(rift.current); return id; }
            unsubs.push(rift.onOpen((v) => { fn(v); }));
            return id;
        }

        const entry: Waiter = { id, deps: keys, fn, ready: new Set() };   // multi-label → one table row
        this.multiWaiters.add(entry);
        for (const k of keys) {
            const rift = this.get<A>(k);
            if (rift.isOpen) entry.ready.add(k);
            else rift.onOpen((v) => this.releaseMultiWaiters(k, v));
        }
        if (entry.ready.size === keys.length) { fn(); this.multiWaiters.delete(entry); }
        return id;
    }/** advance every multi-label waiter on this label */
    private releaseMultiWaiters(k: S, v: A): void {
        for (const entry of [...this.multiWaiters]) {
            if (!entry.deps.includes(k)) continue;
            entry.ready.add(k);
            if (entry.ready.size !== entry.deps.length) continue;
            this.multiWaiters.delete(entry);
            try { entry.fn(v); }
            catch (e: A) { queueMicrotask(() => { throw e; }); }
        }
    }
    public cancel_wait(id: N): B {
        for (const entry of this.multiWaiters) {
            if (entry.id !== id) continue;
            this.multiWaiters.delete(entry);
            return true;
        }
        return false;
    }/** rift-shaped wait on a single label — subscribe first, then re-check isOpen */
    public wait_and_promise<T = A>(timeoutMs: N, key: S, isOpen: B, current: A): Promise<T> {
        const rift = this.get<T>(key);
        return new Promise<T>((resolve, reject) => {
            // subscribe first, THEN re-check isOpen — closes the gap where the record opens mid-subscribe
            let timer: ReturnType<typeof setTimeout> | undefined;
            const unsub = rift.onOpen((v) => {
                if (timer) clearTimeout(timer);
                unsub();
                this.record(key, 'wait_for', v as T);
                resolve(v as T);
            });
            if (isOpen) {                            // opened while we were subscribing
                if (timer) clearTimeout(timer);
                unsub();
                this.record(key, 'wait_for', current as T);
                resolve(current as T);
                return;
            }
            if (timeoutMs) {
                timer = setTimeout(() => {
                    unsub();
                    reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`));
                }, timeoutMs);
            }
        });
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · defer — the scheduler. Drain loop, versioned deps, stats, cancellation.
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //
    // Deferred dependency values are registry entries — `provide` opens a rift under the
    // dependency's label. No separate dependency map. The task table holds tasks only.
    private deferOnError: (e: A, id: N, deps: S[]) => V = (e, id, deps) => {
        this.log('DEFERRED', { msg: 'Task failed', id, deps, error: e }, 'error');
    };
    private deferOnSuccess: (id: N, deps: S[]) => V = (id, deps) => {
        this.log('DEFERRED', { msg: 'Task fulfilled', id, deps }, 'info');
    };/** register work. Executes immediately if every dependency is ready. */
    public defer(deps: S | S[], fn: (...v: A[]) => A): N {
        const keys = [...new Set(typeof deps === 'string' ? [deps] : deps)];
        const id = ++this.nextDeferId;
        this.deferStats.registered++;
        this.deferTasks.set(id, { id, deps: keys, fn, state: 'pending' });
        this.drain();
        return id;
    }/** publish or replace a dependency's value — this is place() with version churn */
    public provide(k: S, v: A): void;
    public provide(deps: Record<S, A>): void;
    public provide(k: S | Record<S, A>, v?: A): void {
        const entries: [S, A][] = typeof k === 'string' ? [[k as S, v as A]] : Object.entries(k as Record<S, A>);
        for (const [key, value] of entries) {
            const rift = this.get(key) as RiftLike<A> & { version: N };
            rift.reset();                            // replace, don't just first-open
            rift.open(value);
            rift.version++;
            this.record(key, 'manual', value);
        }
        this.drain();
    }/** mark a dependency as available without a meaningful value */
    public resolve(k: S): void { this.provide(k, undefined); }
    /** revoke availability. Already-dispatched work is unaffected. */
    public invalidate(k: S): B { return this.deleteRift(k); }
    public deferHas(k: S): B { return this.get(k).isOpen; }
    public deferGet<T = A>(k: S): OPT<T> { return this.get(k).current as T | undefined; }
    public cancel(id: N): B {
        const task = this.deferTasks.get(id);
        if (!task || task.state !== 'pending') return false;
        this.deferTasks.delete(id);
        this.deferStats.cancelled++;
        return true;
    }
    private deferReady(task: DeferredTask): B { return task.deps.every((k) => this.get(k).isOpen); }
    private drain(): void {
        if (this.draining) { this.drainAgain = true; return; }
        this.draining = true;
        try {
            do {
                this.drainAgain = false;
                for (const task of this.deferTasks.values()) {
                    if (task.state !== 'pending' || !this.deferReady(task)) continue;
                    task.state = 'running';          // claim before execution to prevent duplicate dispatch
                    this.deferStats.executed++;
                    const v = task.deps.map((k) => this.get(k).current);
                    try {
                        const result = task.fn(...v);
                        if (result != null && typeof (result as A).then === 'function') {
                            Promise.resolve(result).then(() => {
                                this.deferStats.completed++;
                                this.deferOnSuccess(task.id, task.deps);
                            }).catch((e: A) => {
                                this.deferStats.failed++;
                                this.reportDeferError(task, e);
                            }).finally(() => { this.deferTasks.delete(task.id); });
                        } else {
                            this.deferStats.completed++;
                            this.deferOnSuccess(task.id, task.deps);
                            this.deferTasks.delete(task.id);
                        }
                    } catch (e: A) {
                        this.deferStats.failed++;
                        this.reportDeferError(task, e);
                        this.deferTasks.delete(task.id);
                    }
                }
            } while (this.drainAgain);
        } finally {
            this.draining = false;
        }
    }/** a recorded result releases any deferred task waiting on it */
    private drainDefer(label: S): void {
        this.drain();
    }
    private reportDeferError(task: DeferredTask, e: A): void {
        try { this.deferOnError(e, task.id, task.deps); } catch { /* error reporting must not interrupt the scheduler */ }
    }
    public deferInspect(): R {
        const pending = [...this.deferTasks.values()].filter((x) => x.state === 'pending').map((x) => ({ id: x.id, deps: x.deps, missing: x.deps.filter((k) => !this.get(k).isOpen) }));
        const running = [...this.deferTasks.values()].filter((x) => x.state === 'running').map((x) => ({ id: x.id, deps: x.deps }));
        return {
            stats: { ...this.deferStats },
            pending,
            running,
        } as A;
    }
    public deferClear(): void {
        for (const [id, task] of this.deferTasks) {
            if (task.state === 'pending') { this.deferTasks.delete(id); this.deferStats.cancelled++; }
        }
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // SOLVER · detect_environment — the cartographer
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public static readonly MODULE_LEVEL_CANDIDATES = [
        'typescript', 'rollup', 'vite', 'eslint', 'babel', 'postcss', 'tailwindcss',
        '@babel/core', 'prettier', 'stylelint', 'sass', 'less', 'jest', 'vitest',
        'playwright', '@types/vscode',
    ] as const;
    public static readonly PLATFORM_CANDIDATES = [
        'PROJECT_PLATFORM_KILLER', 'react', 'preact', '@remix-run/react', '@react-router/react',
        'next', 'nuxt', 'svelte', 'solid', 'vite', 'arfa', 'vue', 'angular', 'astro',
        'gatsby', 'dojo', 'inferno', 'lupine', 'ember-source', 'backbone', 'alpinejs',
        'lit', 'marko', '@builder.io/qwik', 'solid-start', '@sveltejs/kit', 'meteor',
        'stencil', 'aurelia', 'nodiff', 'qwik', 'velo-type', 'webpack',
    ] as const;
    public static readonly COMPILER_CANDIDATES = [
        'rspack', 'webpack', 'vite', 'rollup', 'esbuild', 'swc-loader', 'babel', 'next',
        'nuxt', '@remix-run/react', '@react-router/react', 'svelte', 'solid', 'vue',
        '@swc/core', 'turbopack', 'parcel', 'tsup', 'unbuild', 'esbuild-loader',
        '@babel/core', 'terser', '@angular/core', '@angular/compile',
    ] as const;
    public static readonly BUNDLER_CANDIDATES = ['bun', 'pnpm', 'yarn', 'npm'] as const;
    public static readonly ADAPTER_CANDIDATES = [
        'websocket_dev_server', 'cross_app_ws_server', 'terminal_ngin_proxy_register',
        'terminal_ngin_proxy', 'workspace_hub_server', 'workspace_hub_client',
        'cross_app_ws_client', 'cse_cross_app_server', 'cse_cross_app_client',
        'shared_worker', 'service_worker', 'unix_worker', 'broadcastr_channel',
        'websocket', 'websocket_server', 'websocket_client', 'webrtc', 'webrtc_server',
        'webrtc_client', 'electron_server', 'electron_client', 'mqtt', 'sse',
        'named_pipe', 'worker_thread', 'native_msg_host', 'native_msg_client',
    ] as const;
    public static readonly CAPABILITY_CANDIDATES = [
        'shared_worker', 'service_worker', 'unix_worker', 'broadcastr_channel',
        'electron_server', 'electron_client', 'named_pipe', 'native_msg_host', 'native_msg_client',
    ] as const;
    public detectCapability(name: S): B {
        const g: A = typeof globalThis !== 'undefined' ? globalThis : {};
        switch (name) {
            case 'shared_worker': return typeof g.SharedWorker !== 'undefined';
            case 'service_worker': return typeof g.ServiceWorkerGlobalScope !== 'undefined' || (typeof g.navigator !== 'undefined' && 'serviceWorker' in g.navigator);
            case 'unix_worker': return typeof g.process !== 'undefined' && !!g.process?.versions?.node;
            case 'broadcastr_channel': return typeof g.BroadcastChannel !== 'undefined';
            case 'electron_server':
            case 'electron_client': return typeof g.process !== 'undefined' && !!g.process?.versions?.electron;
            case 'named_pipe':
            case 'native_msg_host':
            case 'native_msg_client': return typeof g.process !== 'undefined' && !!g.process?.versions?.node;
            default: return false;
        }
    }
    private detectBundler(): S | null {
        const proc: A = typeof process !== 'undefined' ? process : null;
        if (!proc || !proc.versions?.node) return null;   // .versions check distinguishes real Node from a browser shim
        if (proc.versions?.bun) return 'bun';
        const ua: S = proc.env?.npm_config_user_agent ?? '';
        if (ua.startsWith('pnpm')) return 'pnpm';
        if (ua.startsWith('yarn')) return 'yarn';
        if (ua.startsWith('npm')) return 'npm';
        return null;
    }
    private async detectFirstMatch(candidates: readonly S[]): Promise<S | null> {
        for (const name of candidates) { if (await this.check(name)) return name; }
        return null;
    }
    private async detectAll(candidates: readonly S[]): Promise<S[]> {
        const results = await Promise.all(candidates.map(async (name) => ((await this.check(name)) ? name : null)));
        return results.filter((r): r is S => r !== null);
    }/** runs once. Everything downstream just reads the result. */
    public detect_environment(env_lib?: S): void {
        void (async () => {
            const bundler = this.detectBundler();
            const [moduleLevel, platform, compilers, adapters] = await Promise.all([
                this.detectFirstMatch(env_lib ? [env_lib] : MAELSTROM.MODULE_LEVEL_CANDIDATES),
                this.detectFirstMatch(MAELSTROM.PLATFORM_CANDIDATES),
                this.detectAll(MAELSTROM.COMPILER_CANDIDATES),
                this.detectAll(MAELSTROM.ADAPTER_CANDIDATES),
            ]);
            this.ENV_STATE = { moduleLevel, platform, compilers, bundler, adapters, ready: true };
            this.record('ENV_DETECTION', 'detect_environment', this.ENV_STATE);
        })();
    }/** pure read — no imports, no re-checking, ever */
    public useEnvState(): EnvDetectionState & { rt: S; subChannel: S } {
        if (!this.ENV_STATE.ready && !this.warnedUseBeforeReady) {
            this.warnedUseBeforeReady = true;        // warn once, not spam
            this.log('USE_ENV_STATE', { note: 'called before detect_environment() finished — falling back to defaults' }, 'warn');
        }
        return { ...this.ENV_STATE, rt: 'ENV_DETECTION', subChannel: '[CSE_ENV]' };
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // APPARATION — the no-op factory, carried whole
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public apparation = {
        any: <T = A>(): T => ({}) as T,
        anyGeneric: <T = A>(): T => ({}) as T,
        anyProp: <T = A>(v: T): T => v,
        anyProp2: <T = A>(v: A): T => v as T,
        anyProp3: (v: A): A => v,
        anyAs: (v: S | undefined): A => v as A,
        promise: <T = A>(): Promise<T> => Promise.resolve({} as T),
        booleanPromise: (): Promise<B> => Promise.resolve(false),
        void: (): void => { },
        func: <T = A>(): () => T => () => ({} as T),
        funcVoid: (): () => void => () => { },
        universal: <T = A>(): T => new Proxy({} as A, {
            get: (target, prop) => {
                if (typeof prop === 'symbol') return undefined;
                const propName = prop as S;
                if (propName === 'then' || propName === 'catch' || propName === 'finally') return undefined;
                if ((propName as A) === (Symbol.iterator as A)) return function* () { };
                if (propName === 'toString' || propName === 'valueOf') return () => '';
                return (...args: A[]) => target;
            },
            set: () => true,
            deleteProperty: () => true,
            ownKeys: () => [],
            getOwnPropertyDescriptor: () => undefined,
            apply: (target) => target,
            construct: (target) => target,
        }) as T,
        universalNoop: <T = A>(): T => new Proxy({} as A, {
            get: (target, prop) => {
                if (typeof prop === 'symbol') return undefined;
                const propName = prop as S;
                if (propName === 'then' || propName === 'catch' || propName === 'finally') return undefined;
                if ((propName as A) === (Symbol.iterator as A)) return function* () { };
                if (propName === 'toString' || propName === 'valueOf') return () => '';
                return (...args: A[]) => target;
            },
            set: () => true,
            deleteProperty: () => true,
            ownKeys: () => [],
            getOwnPropertyDescriptor: () => undefined,
            apply: (target) => target,
            construct: (target) => target,
        }) as T,
        simpleNoop: <T = A>(): T => ({}) as T,
        noopPromise: <T = A>(): Promise<T> => Promise.resolve({} as T),
        noopBooleanPromise: (): Promise<B> => Promise.resolve(false),
        noopVoid: (): void => { },
        noopFunction: <T = A>(): () => T => () => ({} as T),
        thenable: <T = A>(): PromiseLike<T> => ({ then: (_onF?: A, _onR?: A) => new Promise<T>(() => { }) }) as A,
        asyncIterable: <T = A>(): AsyncIterable<T> => ({ [Symbol.asyncIterator]: async function* () { await new Promise(() => { }); } }) as A,
        null: (): null => null,
        primitive: <T extends S | N | B | bigint | symbol | undefined>(): T => undefined as T,
        string: (): S => '',
        number: (): N => 0,
        boolean: (): B => false,
        bigint: (): bigint => 0n,
        symbol: (): symbol => Symbol('apparation'),
        undefined: (): undefined => undefined,
        undefinedString: (): S => 'undefined',
        function_: (): (...args: A[]) => A => () => { },
        typedArray: <T extends ArrayBufferView = Uint8Array>(): T => new Uint8Array() as unknown as T,
        dataView: (): DataView => new DataView(new ArrayBuffer(0)),
        arrayBuffer: (): ArrayBuffer => new ArrayBuffer(0),
        map: <K = A, V = A>(): Map<K, V> => new Map<K, V>(),
        set: <T = A>(): Set<T> => new Set<T>(),
        date: (): D => new Date(0),
        regexp: (): RegExp => /(?:)/,
        error: (msg = '[Apparation]'): Error => new Error(msg),
        iterable: <T = A>(): Iterable<T> => [] as A,
        iterator: <T = A>(): Iterator<T> => [][Symbol.iterator](),
        object: <T extends object = object>(): T => ({} as T),
        plainObject: <T extends object = Record<S, A>>(): T => ({} as T),
        array: <T = A>(): T[] => [] as T[],
        weakMap: <K extends object = object, V = A>(): WeakMap<K, V> => new WeakMap<K, V>(),
        weakSet: <T extends object = object>(): WeakSet<T> => new WeakSet<T>(),
        promiseInstance: <T = A>(): Promise<T> => Promise.resolve({} as T),
        url: (): URL => new URL('about:blank'),
        urlSearchParams: (): URLSearchParams => new URLSearchParams(),
        buffer: (): A => (typeof Buffer !== 'undefined' ? Buffer.alloc(0) : new Uint8Array()) as A,
        _ArrayBuffer: (): Uint8Array => new Uint8Array(),
    } as A;
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    // what_is_this_item_exactly — the type interrogator, carried whole
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public what_is_this_item_exactly(v: U): S | undefined {
        // axis 2 first: is it async? — thenable (promise-like, not necessarily Promise)
        if (v != null && typeof (v as A).then === 'function') return 'thenable';
        // async iterable
        if (v != null && typeof (v as A)[Symbol.asyncIterator] === 'function') return 'async-iterable';
        // axis 1: what shape?
        if (v === null) return 'null';
        const ty = typeof v;
        if (ty !== 'object' && ty !== 'function') return ty;
        if (typeof v === 'function') return 'function';
        if (ArrayBuffer.isView(v)) return 'array';
        if (v instanceof Map) return 'map';
        if (v instanceof Set) return 'set';
        if (v instanceof Date) return 'date';
        if (v instanceof RegExp) return 'regexp';
        if (v instanceof Error) return 'error';
        if (typeof (v as A)[Symbol.iterator] === 'function') return 'iterable';
        if (v instanceof Array) return 'Array';
        if (v instanceof WeakMap) return 'WeakMap';
        if (v instanceof WeakSet) return 'WeakSet';
        if (v instanceof Promise) return 'Promise';
        if (v instanceof ArrayBuffer) return 'ArrayBuffer';
        // node-specific
        if (typeof Buffer !== 'undefined' && (v as A) instanceof Buffer) return 'Buffer';
        if (typeof URL !== 'undefined' && (v as A) instanceof URL) return 'URL';
        if (typeof URLSearchParams !== 'undefined' && (v as A) instanceof URLSearchParams) return 'URLSearchParams';
        // plain object / record
        if (typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof Map) && !(v instanceof Set)) return 'plainObject';
        return undefined;
    }// ═══════════════════════════════════════════════════════════════════════════════════════════
    // RESET — wipe everything: the one registry, waiters, subs, scheduler, env state
    // ═══════════════════════════════════════════════════════════════════════════════════════════
    public reset(): void {
        this.rifts.clear();
        this.multiWaiters.clear();
        this.deferTasks.clear();
        this.deferStats = { registered: 0, executed: 0, completed: 0, failed: 0, cancelled: 0 };
        this.autoCloud = undefined;
        this.ENV_STATE = { moduleLevel: null, platform: null, compilers: [], bundler: null, adapters: [], ready: false };
    }
}
export const maelstrom = new MAELSTROM();
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                           TYPE ALIASES (the shorthand table, carried forward)
// #region ─────────────────────────────────────────────────────────────────────────────────────
// THE EXPORT — exactly three classes leave this file, plus the live machine.
// MaybePromise stays outside the class, because that is how that one works.
// Nothing from any prior solution file is imported, referenced, or relied upon.
// ─────────────────────────────────────────────────────────────────────────────────────────────
interface RiftLike<T = A> {
    readonly isOpen: B;
    readonly isClaimed: B;
    readonly current: OPT<T>;
    readonly createdAt: N;
    readonly openedAt: OPT<N>;
    open(v?: T): V;
    reset(): V;
    claim(): B;
    wait(timeoutMs?: N): T | Promise<T>;
    onOpen(listener: (v: T) => V): () => V;
}
interface RiftRecord<T = A> {
    opened: B;
    claimed: B;
    value?: T;
    createdAt: N;
    openedAt?: N;
}

type StoreListener<T = A> = (v: RiftRecord<T>) => void;
type Solver =
    | 'get' | 'place' | 'smash_n_grab' | 'create_rift' | 'use_rift' | 'resolve_global_rift'
    | 'resolve_rift_initial' | 'has' | 'probe' | 'use_if_available' | 'conjure' | 'check'
    | 'load_module' | 'require_module' | 'export_of' | 'wait_for' | 'defer' | 'detect_environment' | 'manual';

type StoreKind = 'local' | 'cloud' | 'auto';

interface ResultRecord<T = A> {
    label: S;
    solver: Solver;
    store: StoreKind;
    value: T | undefined;
    ok: B;
    openedAt: N;
}

interface OPTIONAL_IMPORT_OPTIONS<T = A, F = A> {
    moduleName?: S;
    fallback?: F | (() => F);
    module?: B;             // true → external module lookup; false → internally sourced import
    cache?: B;              // default true. false = always re-run, skip the rift entirely.
    preferRequire?: B;      // default false. true = require() over import() when possible.
    importPromise?: A;      // a raw promise to await directly — for local files, no moduleName lookup.
    force?: F | (() => F);  // force a value over any import at all
    comp_name?: S;          // extract a single named export
    [key: S]: A;            // ...userEnvImports: candidates keyed by detected environment
}

interface EnvDetectionState {
    moduleLevel: S | null;
    platform: S | null;
    compilers: S[];   // full scan
    bundler: S | null;
    adapters: S[];    // full scan
    ready: B;
}
type Waiter = { id: N; deps: S[]; fn: (v?: A) => V; ready: SET<S> };
type DeferredState = 'pending' | 'running';
type DeferredTask = { id: N; deps: S[]; fn: (...v: A[]) => A; state: DeferredState };
type DeferredStats = { registered: N; executed: N; completed: N; failed: N; cancelled: N };
// #endregion ──────────────────────────────────────────────────────────────────────────────────
















