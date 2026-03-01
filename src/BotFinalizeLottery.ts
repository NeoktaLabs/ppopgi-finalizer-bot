// BotFinalizeLottery.ts
import {
  createPublicClient,
  createWalletClient,
  http,
  parseAbi,
  parseAbiItem,
  type Hex,
  type Address,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { etherlink } from "viem/chains";

// -------------------- TYPES & INTERFACES --------------------

export interface Env {
  BOT_PRIVATE_KEY: string;

  // Registry is used as a safety net scan (no indexer needed)
  REGISTRY_ADDRESS: string;

  // Deployer address for on-chain discovery (no indexer)
  DEPLOYER_ADDRESS: string;

  RPC_URL?: string;
  BOT_STATE: KVNamespace;

  // ✅ Durable Object mutex (global lock)
  BOT_LOCK: DurableObjectNamespace;

  HOT_SIZE?: string;
  COLD_SIZE?: string;

  MAX_TX?: string;
  TIME_BUDGET_MS?: string;
  ATTEMPT_TTL_SEC?: string;

  // cron interval (minutes), used only for the status endpoint countdown
  CRON_EVERY_MINUTES?: string;

  // ✅ Deployer log scan tuning (prevents "Block range is too large")
  LOG_CHUNK_BLOCKS?: string; // default 500 (smaller = safer)
  BOOTSTRAP_LOOKBACK_BLOCKS?: string; // default 50_000
  DEPLOYER_HEAD_LAG_BLOCKS?: string; // default 2

  // ✅ TicketsPurchased scan tuning for sold-out fast-path
  TICKETS_LOOKBACK_BLOCKS?: string; // default 2500
  TICKETS_ADDR_CHUNK?: string; // default 50
  TICKETS_HEAD_LAG_BLOCKS?: string; // default 2

  // ✅ extra safety buffer BEFORE forceCancelStuck()
  // Default 900 (15 minutes) if unset. (Hatch opens at +2h on-chain; bot waits +2h + buffer)
  HATCH_EXTRA_DELAY_SEC?: string; // default 900
}

// -------------------- DURABLE OBJECT: GLOBAL MUTEX --------------------

type LockState = {
  locked: boolean;
  runId: string | null;
  acquiredAtMs: number | null;
  leaseMs: number;
  ownerMeta?: Record<string, any> | null;
};

type LockAcquireResponse =
  | { ok: true; runId: string; acquiredAtMs: number; leaseMs: number }
  | { ok: false; runId: string | null; acquiredAtMs: number | null; leaseMs: number; reason: "locked" };

type LockReleaseResponse = { ok: true } | { ok: false; reason: "not-owner" | "not-locked" };

const DO_LOCK_STATE_KEY = "lock_state:v1";

// Global lock: single DO instance, serialized by the platform.
export class BotLockDO implements DurableObject {
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  private async readState(): Promise<LockState> {
    const s = (await this.state.storage.get(DO_LOCK_STATE_KEY)) as LockState | undefined;
    if (!s) {
      return { locked: false, runId: null, acquiredAtMs: null, leaseMs: 0, ownerMeta: null };
    }
    // If lease expired, auto-unlock on read.
    if (s.locked && s.acquiredAtMs != null && s.leaseMs > 0) {
      const now = Date.now();
      if (now - s.acquiredAtMs > s.leaseMs) {
        const cleared: LockState = { locked: false, runId: null, acquiredAtMs: null, leaseMs: 0, ownerMeta: null };
        await this.state.storage.put(DO_LOCK_STATE_KEY, cleared);
        return cleared;
      }
    }
    return s;
  }

  private async writeState(s: LockState): Promise<void> {
    await this.state.storage.put(DO_LOCK_STATE_KEY, s);
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method === "GET" && path === "/status") {
      const s = await this.readState();
      return new Response(JSON.stringify(s, null, 2), {
        headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
      });
    }

    if (req.method === "POST" && path === "/acquire") {
      const body = (await req.json().catch(() => ({}))) as any;
      const runId = String(body?.runId || "");
      const leaseMs = Number(body?.leaseMs || 0);
      const ownerMeta = body?.ownerMeta && typeof body.ownerMeta === "object" ? body.ownerMeta : null;

      if (!runId) return new Response("Missing runId", { status: 400 });

      const now = Date.now();
      const s = await this.readState();

      if (s.locked) {
        const resp: LockAcquireResponse = {
          ok: false,
          runId: s.runId,
          acquiredAtMs: s.acquiredAtMs,
          leaseMs: s.leaseMs,
          reason: "locked",
        };
        return new Response(JSON.stringify(resp), { headers: { "content-type": "application/json" } });
      }

      const next: LockState = {
        locked: true,
        runId,
        acquiredAtMs: now,
        leaseMs: Number.isFinite(leaseMs) && leaseMs > 0 ? leaseMs : 180_000,
        ownerMeta,
      };
      await this.writeState(next);

      const resp: LockAcquireResponse = { ok: true, runId, acquiredAtMs: now, leaseMs: next.leaseMs };
      return new Response(JSON.stringify(resp), { headers: { "content-type": "application/json" } });
    }

    if (req.method === "POST" && path === "/release") {
      const body = (await req.json().catch(() => ({}))) as any;
      const runId = String(body?.runId || "");
      if (!runId) return new Response("Missing runId", { status: 400 });

      const s = await this.readState();
      if (!s.locked) {
        const resp: LockReleaseResponse = { ok: false, reason: "not-locked" };
        return new Response(JSON.stringify(resp), { headers: { "content-type": "application/json" } });
      }
      if (s.runId !== runId) {
        const resp: LockReleaseResponse = { ok: false, reason: "not-owner" };
        return new Response(JSON.stringify(resp), { headers: { "content-type": "application/json" } });
      }

      await this.writeState({ locked: false, runId: null, acquiredAtMs: null, leaseMs: 0, ownerMeta: null });
      const resp: LockReleaseResponse = { ok: true };
      return new Response(JSON.stringify(resp), { headers: { "content-type": "application/json" } });
    }

    return new Response("Not Found", { status: 404 });
  }
}

function lockStub(env: Env) {
  // Single global lock instance.
  const id = env.BOT_LOCK.idFromName("global-finalizer-lock");
  return env.BOT_LOCK.get(id);
}

async function acquireDoLock(env: Env, runId: string, leaseMs: number): Promise<LockAcquireResponse> {
  const stub = lockStub(env);
  const res = await stub.fetch("https://lock/acquire", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      runId,
      leaseMs,
      ownerMeta: {
        // helpful for debugging; safe non-secret context
        ts: Date.now(),
      },
    }),
  });
  return (await res.json()) as LockAcquireResponse;
}

async function releaseDoLock(env: Env, runId: string): Promise<LockReleaseResponse> {
  const stub = lockStub(env);
  const res = await stub.fetch("https://lock/release", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ runId }),
  });
  return (await res.json()) as LockReleaseResponse;
}

async function readDoLockStatus(env: Env): Promise<LockState> {
  const stub = lockStub(env);
  const res = await stub.fetch("https://lock/status", { method: "GET" });
  return (await res.json()) as LockState;
}

// -------------------- ABIs --------------------

const registryAbi = parseAbi([
  "function getAllLotteriesCount() external view returns (uint256)",
  "function getAllLotteries(uint256 start, uint256 limit) external view returns (address[])",
  "function getAllLotteriesPageBounds(uint256 start, uint256 limit) external view returns (uint256 end, uint256 total)",
]);

const lotteryAbi = parseAbi([
  "function status() external view returns (uint8)",
  "function deadline() external view returns (uint64)",
  "function getSold() external view returns (uint256)",
  "function minTickets() external view returns (uint64)",
  "function maxTickets() external view returns (uint64)",
  "function entropy() external view returns (address)",
  "function entropyProvider() external view returns (address)",
  "function entropyRequestId() external view returns (uint64)",
  "function callbackGasLimit() external view returns (uint32)",
  "function isHatchOpen() external view returns (bool)",
  "function drawingRequestedAt() external view returns (uint64)",
  "function finalize() external payable",
  "function forceCancelStuck() external",
]);

const entropyAbi = parseAbi(["function getFeeV2(uint32 callbackGasLimit) external view returns (uint256)"]);

// Deployer event for discovery
const lotteryDeployedEvent = parseAbiItem(
  "event LotteryDeployed(address indexed lottery,address indexed creator,uint256 winningPot,uint256 ticketPrice,string name,address usdc,address entropy,address entropyProvider,uint32 callbackGasLimit,address feeRecipient,uint256 protocolFeePercent,uint64 deadline,uint64 minTickets,uint64 maxTickets)"
);

// TicketsPurchased event for “sold-out fast-path”
const ticketsPurchasedEvent = parseAbiItem(
  "event TicketsPurchased(address indexed buyer,uint256 count,uint256 totalCost,uint256 totalSold,uint256 rangeIndex,bool isNewRange)"
);

// -------------------- HELPERS --------------------

function chunkArray<T>(array: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < array.length; i += size) result.push(array.slice(i, i + size));
  return result;
}

function getSafeSize(val: string | undefined, defaultVal: bigint, maxVal: bigint): bigint {
  if (!val) return defaultVal;
  try {
    const parsed = BigInt(val);
    if (parsed < 0n) return defaultVal;
    return parsed > maxVal ? maxVal : parsed;
  } catch {
    return defaultVal;
  }
}

function getSafeInt(val: string | undefined, defaultVal: number, maxVal: number): number {
  if (!val) return defaultVal;
  const n = Number(val);
  if (!Number.isFinite(n)) return defaultVal;
  return Math.min(Math.max(0, Math.floor(n)), maxVal);
}

function parseBigIntOrNull(v: string | null): bigint | null {
  if (!v) return null;
  try {
    return BigInt(v);
  } catch {
    return null;
  }
}

function nowSec(): number {
  return Math.floor(Date.now() / 1000);
}

function lower(a: string): string {
  return a.toLowerCase();
}

function isTransientErrorMessage(msg: string): boolean {
  const m = msg.toLowerCase();
  return (
    m.includes("timeout") ||
    m.includes("timed out") ||
    m.includes("network") ||
    m.includes("gateway") ||
    m.includes("503") ||
    m.includes("429") ||
    m.includes("rate") ||
    m.includes("connection") ||
    m.includes("econn") ||
    m.includes("failed to fetch") ||
    m.includes("request took too long")
  );
}

function isWrongEntropyFeeMessage(msg: string): boolean {
  const m = msg.toLowerCase();
  return m.includes("wrongentropyfee") || (m.includes("wrong") && m.includes("entropy") && m.includes("fee"));
}

function priorityScore(isFull: boolean, isExpired: boolean, sold: bigint): number {
  if (isFull) return 3;
  if (isExpired && sold > 0n) return 2;
  if (isExpired && sold === 0n) return 1;
  return 0;
}

function statusLabel(s: bigint): string {
  switch (s) {
    case 0n:
      return "FundingPending(0)";
    case 1n:
      return "Open(1)";
    case 2n:
      return "Drawing(2)";
    case 3n:
      return "Completed(3)";
    case 4n:
      return "Canceled(4)";
    default:
      return `Unknown(${s.toString()})`;
  }
}

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function withRetry<T>(
  fn: () => Promise<T>,
  opts: { tries: number; baseDelayMs: number; label?: string; isRetryable?: (e: any) => boolean }
): Promise<T> {
  const tries = Math.max(1, opts.tries);
  const base = Math.max(0, opts.baseDelayMs);
  const isRetryable = opts.isRetryable ?? ((e: any) => isTransientErrorMessage(String(e?.message || e)));

  let lastErr: any = null;

  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch (e: any) {
      lastErr = e;
      const msg = (e?.shortMessage || e?.message || String(e)).toString();
      const retryable = isRetryable(e);

      if (!retryable || i === tries - 1) {
        if (opts.label) console.warn(`   ⛔ ${opts.label} failed: ${msg}`);
        throw e;
      }

      const delay = Math.min(4000, base * Math.pow(2, i));
      if (opts.label) console.warn(`   🌧️ ${opts.label} transient error: ${msg} (retry in ${delay}ms)`);
      await sleep(delay);
    }
  }

  throw lastErr;
}

/**
 * ✅ KV writes only — NO deletes (avoids KV delete free-tier limit)
 */
async function kvPutSafe(env: Env, key: string, value: string, ttlSec = 7 * 24 * 3600) {
  try {
    await env.BOT_STATE.put(key, value, { expirationTtl: ttlSec });
  } catch {
    // ignore
  }
}

/**
 * ✅ "Clear" a key without delete:
 * - write empty sentinel with short TTL so it disappears soon
 */
async function kvClearSafe(env: Env, key: string, ttlSec = 120) {
  try {
    await env.BOT_STATE.put(key, "", { expirationTtl: Math.max(60, ttlSec) });
  } catch {
    // ignore
  }
}

// ✅ NEW: nonce resync helper (prevents "broadcast succeeded but client errored" nonce drift)
async function resyncNonce(
  client: ReturnType<typeof createPublicClient>,
  address: Address,
  currentNonce: bigint
): Promise<bigint> {
  try {
    const pending = await withRetry(
      () => client.getTransactionCount({ address, blockTag: "pending" }),
      { tries: 2, baseDelayMs: 250, label: "getTransactionCount(pending) resync" }
    );
    return pending > currentNonce ? pending : currentNonce;
  } catch {
    return currentNonce;
  }
}

// -------------------- Watchlist State (single KV blob) --------------------

type WatchEntry = {
  addr: Address;
  addedAt: number; // unix sec
  source?: "deployer" | "registry_hot" | "registry_cold";
  lastStatus?: number; // 0..4
  lastCheckedAt?: number; // unix sec
};

type BotStateV1 = {
  v: 1;
  watch: Record<string, WatchEntry>;
  attempts: Record<string, number>;
  deployerCursor?: string; // bigint-as-string
  registryCursor?: string; // bigint-as-string
  ticketsCursor?: string; // bigint-as-string
  runCounter: number;
  updatedAt: number; // unix sec
};

const STATE_KEY = "state:v1";
const DEPLOYER_CURSOR_KEY_FALLBACK = "deployer_last_block"; // for migration/visibility only
const REGISTRY_CURSOR_KEY_FALLBACK = "cursor"; // for migration/visibility only
const TICKETS_CURSOR_KEY_FALLBACK = "tickets_last_block"; // for migration/visibility only

function emptyState(): BotStateV1 {
  return { v: 1, watch: {}, attempts: {}, runCounter: 0, updatedAt: nowSec() };
}

async function loadState(env: Env): Promise<BotStateV1> {
  const raw = await env.BOT_STATE.get(STATE_KEY);
  if (!raw) return emptyState();
  try {
    const parsed = JSON.parse(raw) as BotStateV1;
    if (!parsed || parsed.v !== 1) return emptyState();
    parsed.watch ||= {};
    parsed.attempts ||= {};
    parsed.runCounter = Number.isFinite(parsed.runCounter) ? parsed.runCounter : 0;
    parsed.updatedAt = Number.isFinite(parsed.updatedAt) ? parsed.updatedAt : nowSec();
    return parsed;
  } catch {
    return emptyState();
  }
}

async function saveState(env: Env, state: BotStateV1): Promise<void> {
  state.updatedAt = nowSec();
  await kvPutSafe(env, STATE_KEY, JSON.stringify(state), 365 * 24 * 3600);

  if (state.deployerCursor) await kvPutSafe(env, DEPLOYER_CURSOR_KEY_FALLBACK, state.deployerCursor, 365 * 24 * 3600);
  if (state.registryCursor) await kvPutSafe(env, REGISTRY_CURSOR_KEY_FALLBACK, state.registryCursor, 365 * 24 * 3600);
  if (state.ticketsCursor) await kvPutSafe(env, TICKETS_CURSOR_KEY_FALLBACK, state.ticketsCursor, 365 * 24 * 3600);
}

function watchKey(addr: Address): string {
  return lower(addr);
}

function addToWatch(state: BotStateV1, addr: Address, source: WatchEntry["source"], addedAt?: number): boolean {
  const k = watchKey(addr);
  if (state.watch[k]) return false;
  state.watch[k] = { addr, addedAt: addedAt ?? nowSec(), source };
  return true;
}

function removeFromWatch(state: BotStateV1, addr: Address): boolean {
  const k = watchKey(addr);
  if (!state.watch[k]) return false;
  delete state.watch[k];
  return true;
}

function pruneAttempts(state: BotStateV1, ttlSec: number) {
  const t = nowSec();
  for (const [k, ts] of Object.entries(state.attempts)) {
    if (!Number.isFinite(ts)) {
      delete state.attempts[k];
      continue;
    }
    if (t - ts > ttlSec * 2) delete state.attempts[k];
  }
}

function canAttempt(state: BotStateV1, attemptKey: string, ttlSec: number): boolean {
  const last = state.attempts[attemptKey];
  if (!last) return true;
  return nowSec() - last >= ttlSec;
}

function markAttempt(state: BotStateV1, attemptKey: string) {
  state.attempts[attemptKey] = nowSec();
}

// -------------------- Deployer log polling --------------------

async function pollDeployerForNewLotteries(
  env: Env,
  state: BotStateV1,
  client: ReturnType<typeof createPublicClient>,
  startTimeMs: number,
  timeBudgetMs: number
): Promise<{ added: Address[]; scannedTo?: bigint }> {
  const deployer = env.DEPLOYER_ADDRESS as Address;
  if (!deployer || !deployer.startsWith("0x")) return { added: [] };

  const chunkBlocks = BigInt(getSafeInt(env.LOG_CHUNK_BLOCKS, 500, 5000));
  const lookbackBlocks = BigInt(getSafeInt(env.BOOTSTRAP_LOOKBACK_BLOCKS, 50_000, 5_000_000));
  const headLagBlocks = BigInt(getSafeInt(env.DEPLOYER_HEAD_LAG_BLOCKS, 2, 50));

  const head = await withRetry(() => client.getBlockNumber(), { tries: 3, baseDelayMs: 250, label: "getBlockNumber" });
  const toBlock = head > headLagBlocks ? head - headLagBlocks : head;

  const lastScanned = parseBigIntOrNull(state.deployerCursor ?? (await env.BOT_STATE.get(DEPLOYER_CURSOR_KEY_FALLBACK)));

  const bootstrapFrom = toBlock > lookbackBlocks ? toBlock - lookbackBlocks : 0n;

  let fromBlock = lastScanned != null ? lastScanned + 1n : bootstrapFrom;
  if (fromBlock > toBlock) return { added: [] };

  const newlyAdded: Address[] = [];
  let scannedTo = lastScanned ?? fromBlock - 1n;

  while (fromBlock <= toBlock) {
    if (Date.now() - startTimeMs > timeBudgetMs - 2000) break;

    const end = fromBlock + chunkBlocks > toBlock ? toBlock : fromBlock + chunkBlocks;

    const logs = await withRetry(
      () =>
        client.getLogs({
          address: deployer,
          event: lotteryDeployedEvent,
          fromBlock,
          toBlock: end,
        }),
      {
        tries: 3,
        baseDelayMs: 350,
        label: `getLogs LotteryDeployed ${fromBlock}-${end}`,
        isRetryable: (e: any) => {
          const msg = String(e?.shortMessage || e?.message || e);
          if (msg.toLowerCase().includes("block range is too large")) return false;
          return isTransientErrorMessage(msg);
        },
      }
    );

    for (const log of logs) {
      const lottery = (log.args as any)?.lottery as Address | undefined;
      if (!lottery) continue;
      if (addToWatch(state, lottery, "deployer")) newlyAdded.push(lottery);
    }

    scannedTo = end;
    state.deployerCursor = scannedTo.toString();
    fromBlock = end + 1n;
  }

  if (newlyAdded.length > 0) {
    console.log(`🧾 Deployer discovery: added=${newlyAdded.length} (cursor=${scannedTo})`);
  }
  return { added: newlyAdded, scannedTo };
}

// -------------------- Registry rolling scan (every run) --------------------

async function registryRollingScan(
  env: Env,
  state: BotStateV1,
  client: ReturnType<typeof createPublicClient>
): Promise<{ hotAdded: Address[]; coldAdded: Address[]; total: bigint }> {
  const HOT_SIZE = getSafeSize(env.HOT_SIZE, 200n, 2000n);
  const COLD_SIZE = getSafeSize(env.COLD_SIZE, 100n, 2000n);

  const total = await withRetry(
    () =>
      client.readContract({
        address: env.REGISTRY_ADDRESS as Address,
        abi: registryAbi,
        functionName: "getAllLotteriesCount",
      }),
    { tries: 3, baseDelayMs: 250, label: "getAllLotteriesCount" }
  );

  const totalBig = BigInt(total as unknown as bigint);
  if (totalBig === 0n) return { hotAdded: [], coldAdded: [], total: 0n };

  const clampSize = async (start: bigint, limit: bigint): Promise<bigint> => {
    if (limit <= 0n) return 0n;
    const [end] = await withRetry(
      () =>
        client.readContract({
          address: env.REGISTRY_ADDRESS as Address,
          abi: registryAbi,
          functionName: "getAllLotteriesPageBounds",
          args: [start, limit],
        }),
      { tries: 3, baseDelayMs: 200, label: "getAllLotteriesPageBounds" }
    );
    const endBig = BigInt(end as unknown as bigint);
    if (endBig <= start) return 0n;
    return endBig - start;
  };

  const startHot = totalBig > HOT_SIZE ? totalBig - HOT_SIZE : 0n;

  const savedCursorStr = state.registryCursor ?? (await env.BOT_STATE.get(REGISTRY_CURSOR_KEY_FALLBACK));
  let cursor = savedCursorStr ? BigInt(savedCursorStr) : 0n;
  if (cursor >= totalBig) cursor = 0n;

  const startCold = cursor;

  const safeHotSize = await clampSize(startHot, HOT_SIZE);
  const safeColdSize = await clampSize(startCold, COLD_SIZE);

  const [hotBatch, coldBatch] = await withRetry(
    async () => {
      const [h, c] = await Promise.all([
        safeHotSize > 0n
          ? client.readContract({
              address: env.REGISTRY_ADDRESS as Address,
              abi: registryAbi,
              functionName: "getAllLotteries",
              args: [startHot, safeHotSize],
            })
          : Promise.resolve([] as Address[]),
        safeColdSize > 0n
          ? client.readContract({
              address: env.REGISTRY_ADDRESS as Address,
              abi: registryAbi,
              functionName: "getAllLotteries",
              args: [startCold, safeColdSize],
            })
          : Promise.resolve([] as Address[]),
      ]);
      return [h as Address[], c as Address[]] as const;
    },
    { tries: 3, baseDelayMs: 250, label: "getAllLotteries batches" }
  );

  let nextCursor = startCold + safeColdSize;
  if (nextCursor >= totalBig) nextCursor = 0n;
  state.registryCursor = nextCursor.toString();

  const hotAdded: Address[] = [];
  for (const a of hotBatch) if (addToWatch(state, a, "registry_hot")) hotAdded.push(a);

  const coldAdded: Address[] = [];
  for (const a of coldBatch) if (addToWatch(state, a, "registry_cold")) coldAdded.push(a);

  return { hotAdded, coldAdded, total: totalBig };
}

// -------------------- Sold-out fast-path via TicketsPurchased logs --------------------

async function pollSoldOutFromTicketsPurchased(
  env: Env,
  state: BotStateV1,
  client: ReturnType<typeof createPublicClient>,
  startTimeMs: number,
  timeBudgetMs: number,
  watchAddrs: Address[]
): Promise<Address[]> {
  if (watchAddrs.length === 0) return [];

  const lookback = BigInt(getSafeInt(env.TICKETS_LOOKBACK_BLOCKS, 2500, 250_000));
  const headLag = BigInt(getSafeInt(env.TICKETS_HEAD_LAG_BLOCKS, 2, 50));
  const addrChunk = getSafeInt(env.TICKETS_ADDR_CHUNK, 50, 250);

  const chunkBlocks = BigInt(getSafeInt(env.LOG_CHUNK_BLOCKS, 500, 5000));

  const head = await withRetry(() => client.getBlockNumber(), { tries: 3, baseDelayMs: 250, label: "getBlockNumber" });
  const toBlock = head > headLag ? head - headLag : head;

  const lastScanned = parseBigIntOrNull(state.ticketsCursor ?? (await env.BOT_STATE.get(TICKETS_CURSOR_KEY_FALLBACK)));

  const bootstrapFrom = toBlock > lookback ? toBlock - lookback : 0n;

  let fromBlock = lastScanned != null ? lastScanned + 1n : bootstrapFrom;
  if (fromBlock < bootstrapFrom) fromBlock = bootstrapFrom;
  if (fromBlock > toBlock) return [];

  const soldSeen = new Map<string, bigint>();
  const addrChunks = chunkArray(watchAddrs, addrChunk);
  let scannedTo = lastScanned ?? fromBlock - 1n;

  while (fromBlock <= toBlock) {
    if (Date.now() - startTimeMs > timeBudgetMs - 4000) break;
    const end = fromBlock + chunkBlocks > toBlock ? toBlock : fromBlock + chunkBlocks;

    for (const addrs of addrChunks) {
      if (Date.now() - startTimeMs > timeBudgetMs - 3500) break;

      const logs = await withRetry(
        () =>
          client.getLogs({
            address: addrs as Address[],
            event: ticketsPurchasedEvent,
            fromBlock,
            toBlock: end,
          }),
        {
          tries: 3,
          baseDelayMs: 350,
          label: `getLogs TicketsPurchased ${fromBlock}-${end} addrs=${addrs.length}`,
          isRetryable: (e: any) => {
            const msg = String(e?.shortMessage || e?.message || e);
            if (msg.toLowerCase().includes("block range is too large")) return false;
            return isTransientErrorMessage(msg);
          },
        }
      );

      for (const log of logs) {
        const lotAddr = (log.address as string) || "";
        if (!lotAddr || !lotAddr.startsWith("0x") || lotAddr.length !== 42) continue;

        const totalSold = BigInt(((log.args as any)?.totalSold ?? 0) as any);
        const k = lower(lotAddr);

        const prev = soldSeen.get(k);
        if (prev == null || totalSold > prev) soldSeen.set(k, totalSold);
      }
    }

    scannedTo = end;
    state.ticketsCursor = scannedTo.toString();
    fromBlock = end + 1n;
  }

  if (soldSeen.size === 0) return [];

  const lots = Array.from(soldSeen.keys()).map((k) => k as Address);
  const maxTicketsResults = await withRetry(
    () =>
      client.multicall({
        contracts: lots.map((addr) => ({
          address: addr,
          abi: lotteryAbi,
          functionName: "maxTickets",
        })),
      }),
    { tries: 3, baseDelayMs: 250, label: "multicall maxTickets (sold-out scan)" }
  );

  const urgent: Address[] = [];
  for (let i = 0; i < lots.length; i++) {
    const addr = lots[i];
    const r = maxTicketsResults[i];
    if (r.status !== "success") continue;

    const maxT = BigInt(r.result as any);
    if (maxT <= 0n) continue;

    const seen = soldSeen.get(lower(addr)) ?? 0n;
    if (seen >= maxT) urgent.push(addr);
  }

  if (urgent.length > 0) {
    console.log(`🚨 Sold-out fast-path: urgent=${urgent.length} (sample=${urgent.slice(0, 10).join(", ")})`);
  }

  return urgent;
}

// -------------------- Cron interval utilities (status endpoint only) --------------------

function getCronEveryMinutes(env: Env): number {
  const raw = env.CRON_EVERY_MINUTES;
  const n = Number(raw);
  return Number.isFinite(n) ? Math.max(1, Math.floor(n)) : 1;
}

function nextCronMs(nowMs = Date.now(), everyMinutes = 1): number {
  const m = Math.max(1, Math.floor(everyMinutes));
  const step = m * 60_000;
  return (Math.floor(nowMs / step) + 1) * step;
}

function parseKvNum(v: string | null): number | null {
  if (!v) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function corsHeadersFor(req: Request): Record<string, string> {
  const origin = req.headers.get("Origin") || "*";
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}

// -------------------- MAIN WORKER --------------------

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const cors = corsHeadersFor(req);

    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });
    if (req.method !== "GET") return new Response("Method Not Allowed", { status: 405, headers: cors });

    // --- Debug endpoint: watchlist ---
    if (url.pathname === "/watchlist") {
      const state = await loadState(env);

      const start = Math.max(0, Number(url.searchParams.get("start") || "0") | 0);
      const limit = Math.min(500, Math.max(1, Number(url.searchParams.get("limit") || "50") | 0));

      const entries = Object.values(state.watch);
      entries.sort((a, b) => (b.addedAt || 0) - (a.addedAt || 0));

      const page = entries.slice(start, start + limit);

      const counts: Record<string, number> = { unknown: 0, "0": 0, "1": 0, "2": 0, "3": 0, "4": 0 };
      for (const e of entries) {
        if (typeof e.lastStatus !== "number") counts.unknown++;
        else counts[String(e.lastStatus)] = (counts[String(e.lastStatus)] || 0) + 1;
      }

      return new Response(
        JSON.stringify(
          {
            total: entries.length,
            start,
            limit,
            countsByStatus: {
              unknown: counts.unknown,
              FundingPending: counts["0"],
              Open: counts["1"],
              Drawing: counts["2"],
              Completed: counts["3"],
              Canceled: counts["4"],
            },
            cursors: {
              deployer: state.deployerCursor ?? null,
              registry: state.registryCursor ?? null,
              tickets: state.ticketsCursor ?? null,
            },
            items: page.map((e) => ({
              addr: e.addr,
              addedAt: e.addedAt,
              source: e.source || "unknown",
              lastStatus: typeof e.lastStatus === "number" ? statusLabel(BigInt(e.lastStatus)) : null,
              lastCheckedAt: e.lastCheckedAt ?? null,
            })),
          },
          null,
          2
        ),
        {
          headers: {
            ...cors,
            "content-type": "application/json; charset=utf-8",
            "cache-control": "public, max-age=2",
          },
        }
      );
    }

    // --- Existing status endpoint ---
    if (url.pathname !== "/bot-status") return new Response("Not Found", { status: 404, headers: cors });

    const [
      lastRunTsRaw,
      lastFinishedTsRaw,
      lastOkTsRaw,
      lastStatus,
      lastError,
      lastRunId,
      lastDurationRaw,
      lastTxCountRaw,
      deployerBlockRaw,
      ticketsBlockRaw,
      stateRaw,
    ] = await Promise.all([
      env.BOT_STATE.get("last_run_ts"),
      env.BOT_STATE.get("last_run_finished_ts"),
      env.BOT_STATE.get("last_ok_ts"),
      env.BOT_STATE.get("last_run_status"),
      env.BOT_STATE.get("last_run_error"),
      env.BOT_STATE.get("last_run_id"),
      env.BOT_STATE.get("last_run_duration_ms"),
      env.BOT_STATE.get("last_run_tx_count"),
      env.BOT_STATE.get(DEPLOYER_CURSOR_KEY_FALLBACK),
      env.BOT_STATE.get(TICKETS_CURSOR_KEY_FALLBACK),
      env.BOT_STATE.get(STATE_KEY),
    ]);

    const now = Date.now();
    const lastRun = parseKvNum(lastRunTsRaw);
    const lastFinished = parseKvNum(lastFinishedTsRaw);
    const lastOk = parseKvNum(lastOkTsRaw);
    const durationMs = parseKvNum(lastDurationRaw);
    const lastTxCount = parseKvNum(lastTxCountRaw);

    const cronEveryMinutes = getCronEveryMinutes(env);
    const nextRun = nextCronMs(now, cronEveryMinutes);

    // ✅ Read lock status from DO (strong consistency)
    let lockInfo: LockState | null = null;
    try {
      lockInfo = await readDoLockStatus(env);
    } catch {
      lockInfo = null;
    }

    let watchCount: number | null = null;
    let deployerCursorFromState: string | null = null;
    let registryCursorFromState: string | null = null;
    let ticketsCursorFromState: string | null = null;
    try {
      if (stateRaw) {
        const s = JSON.parse(stateRaw) as BotStateV1;
        watchCount = s?.watch ? Object.keys(s.watch).length : null;
        deployerCursorFromState = s?.deployerCursor ?? null;
        registryCursorFromState = s?.registryCursor ?? null;
        ticketsCursorFromState = s?.ticketsCursor ?? null;
      }
    } catch {
      // ignore
    }

    return new Response(
      JSON.stringify(
        {
          status: lastStatus || "unknown",
          running: lockInfo?.locked ?? false,
          lockRunId: lockInfo?.locked ? lockInfo.runId : null,
          lockAcquiredAtMs: lockInfo?.acquiredAtMs ?? null,
          lockLeaseMs: lockInfo?.leaseMs ?? null,

          lastRunId: lastRunId || null,
          lastRun,
          lastFinished,
          durationMs,
          txCount: lastTxCount,

          secondsSinceLastRun: lastRun ? Math.floor((now - lastRun) / 1000) : null,
          lastOk,
          secondsSinceLastOk: lastOk ? Math.floor((now - lastOk) / 1000) : null,

          cronEveryMinutes,
          nextRun,
          secondsToNextRun: Math.max(0, Math.floor((nextRun - now) / 1000)),
          lastError: lastError && lastError.trim() ? lastError : null,

          deployerCursorBlock: deployerBlockRaw ? Number(deployerBlockRaw) : null,
          ticketsCursorBlock: ticketsBlockRaw ? Number(ticketsBlockRaw) : null,

          watchlistCount: watchCount,
          deployerCursorFromState,
          registryCursorFromState,
          ticketsCursorFromState,
        },
        null,
        2
      ),
      {
        headers: {
          ...cors,
          "content-type": "application/json; charset=utf-8",
          "cache-control": "public, max-age=2",
        },
      }
    );
  },

  async scheduled(_event: ScheduledEvent, env: Env, _ctx: ExecutionContext): Promise<void> {
    const START_TIME = Date.now();

    // Use lease slightly longer than your time budget, with a floor.
    const TIME_BUDGET_MS = getSafeInt(env.TIME_BUDGET_MS, 25_000, 45_000);
    const LEASE_MS = Math.max(90_000, Math.min(240_000, TIME_BUDGET_MS + 60_000));

    const runId = crypto.randomUUID();
    console.log(`🤖 Run ${runId} started`);

    await kvPutSafe(env, "last_run_ts", START_TIME.toString());
    await kvPutSafe(env, "last_run_status", "running");
    await kvPutSafe(env, "last_run_id", runId);

    await kvClearSafe(env, "last_run_error", 120);
    await kvClearSafe(env, "last_run_duration_ms", 120);
    await kvClearSafe(env, "last_run_tx_count", 120);
    await kvClearSafe(env, "last_run_finished_ts", 120);

    // ✅ Strongly consistent lock via Durable Object
    let acquired: LockAcquireResponse;
    try {
      acquired = await acquireDoLock(env, runId, LEASE_MS);
    } catch (e: any) {
      const msg = (e?.message || String(e)).toString();
      console.warn(`⚠️ Lock DO acquire error: ${msg}`);
      await kvPutSafe(env, "last_run_status", "skipped_lock_error");
      await kvPutSafe(env, "last_run_finished_ts", Date.now().toString());
      await kvPutSafe(env, "last_run_duration_ms", String(Date.now() - START_TIME));
      return;
    }

    if (!acquired.ok) {
      console.warn(`⚠️ Locked by run ${acquired.runId ?? "unknown"}. Skipping.`);
      await kvPutSafe(env, "last_run_status", "skipped_locked");
      await kvPutSafe(env, "last_run_finished_ts", Date.now().toString());
      await kvPutSafe(env, "last_run_duration_ms", String(Date.now() - START_TIME));
      return;
    }

    let txCount = 0;

    try {
      txCount = await runLogic(env, START_TIME);

      await kvPutSafe(env, "last_run_status", "ok");
      await kvPutSafe(env, "last_ok_ts", Date.now().toString());
      await kvPutSafe(env, "last_run_tx_count", String(txCount));
    } catch (e: any) {
      const msg = (e?.message || String(e)).toString();
      console.error("❌ Critical Error:", msg);

      await kvPutSafe(env, "last_run_status", "error");
      await kvPutSafe(env, "last_run_error", msg.slice(0, 500));
      await kvPutSafe(env, "last_run_tx_count", String(txCount));
    } finally {
      const finished = Date.now();
      await kvPutSafe(env, "last_run_finished_ts", finished.toString());
      await kvPutSafe(env, "last_run_duration_ms", String(finished - START_TIME));

      // ✅ Release DO lock (best-effort)
      try {
        const rel = await releaseDoLock(env, runId);
        if (rel.ok) console.log(`🔓 Lock released (DO)`);
        else console.warn(`⚠️ Lock release refused: ${rel.reason}`);
      } catch (e: any) {
        const msg = (e?.message || String(e)).toString();
        console.warn(`⚠️ Lock DO release error: ${msg}`);
      }
    }
  },
};

// -------------------- Core logic --------------------

async function runLogic(env: Env, startTimeMs: number): Promise<number> {
  if (!env.BOT_PRIVATE_KEY || !env.REGISTRY_ADDRESS || !env.DEPLOYER_ADDRESS) {
    throw new Error("Missing Env: BOT_PRIVATE_KEY/REGISTRY_ADDRESS/DEPLOYER_ADDRESS");
  }

  const rpcUrl = env.RPC_URL || "https://node.mainnet.etherlink.com";
  const account = privateKeyToAccount(env.BOT_PRIVATE_KEY as Hex);

  const transport = http(rpcUrl, { timeout: 12_000, retryCount: 2, retryDelay: 400 });
  const client = createPublicClient({ chain: etherlink, transport });
  const wallet = createWalletClient({ account, chain: etherlink, transport });

  console.log(`👛 Bot address: ${account.address}`);
  console.log(`🌐 RPC: ${rpcUrl}`);

  const MAX_TX = getSafeInt(env.MAX_TX, 5, 25);
  const TIME_BUDGET_MS = getSafeInt(env.TIME_BUDGET_MS, 25_000, 45_000);
  const ATTEMPT_TTL_SEC = getSafeInt(env.ATTEMPT_TTL_SEC, 600, 3600);

  const HATCH_EXTRA_DELAY_SEC = getSafeInt(env.HATCH_EXTRA_DELAY_SEC, 900, 24 * 3600);

  const state = await loadState(env);
  state.runCounter += 1;

  pruneAttempts(state, ATTEMPT_TTL_SEC);

  const beforeWatchCount = Object.keys(state.watch).length;

  const dep = await pollDeployerForNewLotteries(env, state, client, startTimeMs, TIME_BUDGET_MS);
  const reg = await registryRollingScan(env, state, client);

  const afterDiscoveryCount = Object.keys(state.watch).length;

  const addedAll = [...dep.added, ...reg.hotAdded, ...reg.coldAdded];
  if (addedAll.length > 0) {
    const sample = addedAll.slice(0, 10);
    console.log(
      `📌 Watchlist discovery: added=${addedAll.length} (sample=${sample.join(", ")}) watchlist=${afterDiscoveryCount}`
    );
  } else {
    console.log(`📌 Watchlist discovery: added=0 watchlist=${afterDiscoveryCount}`);
  }

  const watchEntries = Object.values(state.watch);

  const MAX_WATCH_CHECK = 3000;
  const candidates: Address[] = watchEntries
    .sort((a, b) => (b.addedAt || 0) - (a.addedAt || 0))
    .slice(0, MAX_WATCH_CHECK)
    .map((e) => e.addr);

  if (candidates.length === 0) {
    console.log("ℹ️ Watchlist is empty. Done.");
    await saveState(env, state);
    return 0;
  }

  const urgentSoldOut = await pollSoldOutFromTicketsPurchased(env, state, client, startTimeMs, TIME_BUDGET_MS, candidates);

  const statusResults = await withRetry(
    () =>
      client.multicall({
        contracts: candidates.map((addr) => ({
          address: addr,
          abi: lotteryAbi,
          functionName: "status",
        })),
      }),
    { tries: 3, baseDelayMs: 250, label: "status multicall" }
  );

  const openLotteries: Address[] = [];
  const drawingLotteries: Address[] = [];
  const doneLotteries: Address[] = [];

  const tNow = nowSec();

  for (let i = 0; i < candidates.length; i++) {
    const addr = candidates[i];
    const r = statusResults[i];
    if (r.status !== "success") continue;

    const s = Number(BigInt(r.result as any));
    const wk = watchKey(addr);
    if (state.watch[wk]) {
      state.watch[wk].lastStatus = s;
      state.watch[wk].lastCheckedAt = tNow;
    }

    if (s === 1) openLotteries.push(addr);
    else if (s === 2) drawingLotteries.push(addr);
    else if (s === 3 || s === 4) doneLotteries.push(addr);
  }

  const removed: Address[] = [];
  for (const d of doneLotteries) {
    if (removeFromWatch(state, d)) removed.push(d);
  }

  if (removed.length > 0) {
    console.log(
      `🧹 Watchlist prune: removed=${removed.length} (sample=${removed.slice(0, 10).join(", ")}) watchlist=${
        Object.keys(state.watch).length
      }`
    );
  }

  console.log(
    `📊 Watchlist summary: total=${Object.keys(state.watch).length} checked=${candidates.length} open=${openLotteries.length} drawing=${drawingLotteries.length} done=${doneLotteries.length} (before=${beforeWatchCount}, afterDiscovery=${afterDiscoveryCount})`
  );

  const currentNonceStart = await withRetry(
    () => client.getTransactionCount({ address: account.address, blockTag: "pending" }),
    { tries: 3, baseDelayMs: 300, label: "getTransactionCount(pending)" }
  );

  let currentNonce = currentNonceStart;
  let txCount = 0;

  if (openLotteries.length > 0) {
    const urgentSet = new Set(urgentSoldOut.map((a) => lower(a)));
    const urgentOpen = openLotteries.filter((a) => urgentSet.has(lower(a)));
    const normalOpen = openLotteries.filter((a) => !urgentSet.has(lower(a)));

    const orderedOpen = [...urgentOpen, ...normalOpen];

    console.log(
      `⚡ Open lotteries: total=${openLotteries.length} urgentSoldOutOpen=${urgentOpen.length} normalOpen=${normalOpen.length}`
    );

    const chunks = chunkArray(orderedOpen, 25);

    for (const chunk of chunks) {
      if (txCount >= MAX_TX) break;
      if (Date.now() - startTimeMs > TIME_BUDGET_MS) break;

      const tNowSec = BigInt(nowSec());

      const detailCalls = chunk.flatMap((addr) => [
        { address: addr, abi: lotteryAbi, functionName: "status" },
        { address: addr, abi: lotteryAbi, functionName: "deadline" },
        { address: addr, abi: lotteryAbi, functionName: "getSold" },
        { address: addr, abi: lotteryAbi, functionName: "minTickets" },
        { address: addr, abi: lotteryAbi, functionName: "maxTickets" },
        { address: addr, abi: lotteryAbi, functionName: "entropy" },
        { address: addr, abi: lotteryAbi, functionName: "entropyProvider" },
        { address: addr, abi: lotteryAbi, functionName: "entropyRequestId" },
        { address: addr, abi: lotteryAbi, functionName: "callbackGasLimit" },
      ]);

      const detailResults = await withRetry(
        () => client.multicall({ contracts: detailCalls }),
        { tries: 3, baseDelayMs: 250, label: "details multicall" }
      );

      type Cand = {
        addr: Address;
        deadline: bigint;
        sold: bigint;
        minTickets: bigint;
        maxTickets: bigint;
        entropyAddr: Address;
        callbackGasLimit: number;
        isExpired: boolean;
        isFull: boolean;
        cancelPath: boolean;
        score: number;
      };

      const actionable: Cand[] = [];

      for (let i = 0; i < chunk.length; i++) {
        const lottery = chunk[i];
        const baseIdx = i * 9;

        const rStatus = detailResults[baseIdx];
        const rDeadline = detailResults[baseIdx + 1];
        const rSold = detailResults[baseIdx + 2];
        const rMin = detailResults[baseIdx + 3];
        const rMax = detailResults[baseIdx + 4];
        const rEntropy = detailResults[baseIdx + 5];
        const rReq = detailResults[baseIdx + 7];
        const rGas = detailResults[baseIdx + 8];

        if (
          rStatus.status !== "success" ||
          rDeadline.status !== "success" ||
          rSold.status !== "success" ||
          rMin.status !== "success" ||
          rMax.status !== "success" ||
          rEntropy.status !== "success" ||
          rReq.status !== "success" ||
          rGas.status !== "success"
        )
          continue;

        const statusNow = BigInt(rStatus.result as any);
        if (statusNow !== 1n) continue;

        const reqId = BigInt(rReq.result as any);
        if (reqId !== 0n) continue;

        const deadline = BigInt(rDeadline.result as any);
        const sold = BigInt(rSold.result as any);
        const minTickets = BigInt(rMin.result as any);
        const maxTickets = BigInt(rMax.result as any);

        const entropyAddr = rEntropy.result as Address;
        const callbackGasLimit = Number(BigInt(rGas.result as any));

        const isExpired = tNowSec >= deadline;
        const isFull = maxTickets > 0n && sold >= maxTickets;
        if (!isExpired && !isFull) continue;

        const cancelPath = isExpired && sold < minTickets;
        if (!cancelPath && sold === 0n) continue;

        actionable.push({
          addr: lottery,
          deadline,
          sold,
          minTickets,
          maxTickets,
          entropyAddr,
          callbackGasLimit,
          isExpired,
          isFull,
          cancelPath,
          score: priorityScore(isFull, isExpired, sold),
        });
      }

      if (actionable.length === 0) continue;

      actionable.sort((a, b) => {
        const au = urgentSet.has(lower(a.addr)) ? 1 : 0;
        const bu = urgentSet.has(lower(b.addr)) ? 1 : 0;
        if (au !== bu) return bu - au;
        return b.score - a.score;
      });

      for (const c of actionable) {
        if (txCount >= MAX_TX) break;
        if (Date.now() - startTimeMs > TIME_BUDGET_MS) break;

        const attemptKey = `finalize:${lower(c.addr)}`;
        if (!canAttempt(state, attemptKey, ATTEMPT_TTL_SEC)) continue;

        console.log(
          `🚀 Finalize candidate: ${c.addr} (urgent=${urgentSet.has(lower(c.addr))} expired=${c.isExpired} full=${
            c.isFull
          } sold=${c.sold} min=${c.minTickets} max=${c.maxTickets} cancelPath=${c.cancelPath} gas=${c.callbackGasLimit})`
        );

        const fetchFeeV2 = async (useCache: boolean): Promise<bigint> => {
          const feeKey = `feev2:${lower(c.entropyAddr)}:${c.callbackGasLimit}`;
          if (useCache) {
            const cachedFee = await env.BOT_STATE.get(feeKey);
            if (cachedFee) {
              try {
                return BigInt(cachedFee);
              } catch {
                // ignore
              }
            }
          }

          const fee = await withRetry(
            () =>
              client.readContract({
                address: c.entropyAddr,
                abi: entropyAbi,
                functionName: "getFeeV2",
                args: [c.callbackGasLimit],
              }),
            { tries: 3, baseDelayMs: 250, label: "entropy.getFeeV2" }
          );

          const v = BigInt(fee as any);
          await kvPutSafe(env, feeKey, v.toString(), 60);
          return v;
        };

        let value = 0n;
        if (!c.cancelPath) {
          try {
            value = await fetchFeeV2(true);
          } catch (e: any) {
            const msg = (e?.shortMessage || e?.message || "").toString();
            console.warn(`   ⚠️ FeeV2 lookup failed; skipping draw for now: ${msg}`);
            markAttempt(state, attemptKey);
            continue;
          }
        }

        let simulated = false;
        let simWasTransient = false;

        for (let simTry = 0; simTry < 2; simTry++) {
          try {
            await withRetry(
              () =>
                client.simulateContract({
                  account,
                  address: c.addr,
                  abi: lotteryAbi,
                  functionName: "finalize",
                  value,
                }),
              { tries: 2, baseDelayMs: 200, label: "simulate finalize" }
            );
            simulated = true;
            break;
          } catch (e: any) {
            const msg = (e?.shortMessage || e?.message || "").toString();

            if (!c.cancelPath && simTry === 0 && isWrongEntropyFeeMessage(msg)) {
              try {
                value = await fetchFeeV2(false);
                console.warn(`   ♻️ WrongEntropyFee detected; refreshed fee and retrying simulation.`);
                continue;
              } catch {
                console.warn(`   ⚠️ Fee refresh failed after WrongEntropyFee; skipping.`);
                break;
              }
            }

            if (isTransientErrorMessage(msg)) {
              console.warn(`   🌧️ Transient sim error (will retry next run): ${msg}`);
              simWasTransient = true;
              break;
            }

            console.warn(`   ⏭️ Simulation revert: ${msg}`);
            break;
          }
        }

        if (!simWasTransient) {
          markAttempt(state, attemptKey);
        }

        if (!simulated) continue;

        try {
          const nonceToUse = currentNonce;
          const hash = await withRetry(
            () =>
              wallet.writeContract({
                account,
                address: c.addr,
                abi: lotteryAbi,
                functionName: "finalize",
                value,
                nonce: nonceToUse,
              }),
            { tries: 2, baseDelayMs: 250, label: "send finalize tx" }
          );

          currentNonce++;
          console.log(`   ✅ finalize Tx Sent: ${hash}`);
          txCount++;
        } catch (e: any) {
          const msg = (e?.shortMessage || e?.message || "").toString();
          console.warn(`   ⏭️ finalize Tx failed: ${msg}`);
          currentNonce = await resyncNonce(client, account.address, currentNonce);
        }

        if (txCount > 0 && txCount % 2 === 0) {
          currentNonce = await resyncNonce(client, account.address, currentNonce);
        }
      }
    }
  } else {
    console.log("ℹ️ No Open lotteries found.");
  }

  if (drawingLotteries.length > 0 && txCount < MAX_TX && Date.now() - startTimeMs <= TIME_BUDGET_MS) {
    console.log(`🧯 Found ${drawingLotteries.length} Drawing lotteries to check for hatch.`);

    const hatchResults = await withRetry(
      () =>
        client.multicall({
          contracts: drawingLotteries.flatMap((addr) => [
            { address: addr, abi: lotteryAbi, functionName: "isHatchOpen" },
            { address: addr, abi: lotteryAbi, functionName: "drawingRequestedAt" },
          ]),
        }),
      { tries: 3, baseDelayMs: 250, label: "hatch multicall (isHatchOpen + drawingRequestedAt)" }
    );

    const nowS = nowSec();

    for (let i = 0; i < drawingLotteries.length; i++) {
      if (txCount >= MAX_TX) break;
      if (Date.now() - startTimeMs > TIME_BUDGET_MS) break;

      const addr = drawingLotteries[i];
      const rHatch = hatchResults[i * 2];
      const rDrawAt = hatchResults[i * 2 + 1];

      if (rHatch.status !== "success" || rDrawAt.status !== "success") continue;
      if (rHatch.result !== true) continue;

      const drawingRequestedAt = Number(BigInt(rDrawAt.result as any));
      if (!Number.isFinite(drawingRequestedAt) || drawingRequestedAt <= 0) continue;

      const ok = nowS > drawingRequestedAt + 2 * 3600 + HATCH_EXTRA_DELAY_SEC;
      if (!ok) {
        const wait = drawingRequestedAt + 2 * 3600 + HATCH_EXTRA_DELAY_SEC - nowS;
        console.log(`   ⏳ Hatch open but waiting buffer for ${addr}: wait ~${wait}s`);
        continue;
      }

      const attemptKey = `hatch:${lower(addr)}`;
      if (!canAttempt(state, attemptKey, ATTEMPT_TTL_SEC)) continue;

      console.log(`🧯 Hatch open (+buffer): ${addr} -> forceCancelStuck()`);

      let simWasTransient = false;
      try {
        await withRetry(
          () =>
            client.simulateContract({
              account,
              address: addr,
              abi: lotteryAbi,
              functionName: "forceCancelStuck",
            }),
          { tries: 2, baseDelayMs: 200, label: "simulate forceCancelStuck" }
        );
      } catch (e: any) {
        const msg = (e?.shortMessage || e?.message || "").toString();
        console.warn(`   ⏭️ Hatch simulation revert: ${msg}`);
        if (isTransientErrorMessage(msg)) simWasTransient = true;
        if (!simWasTransient) markAttempt(state, attemptKey);
        continue;
      }

      markAttempt(state, attemptKey);

      try {
        const nonceToUse = currentNonce;
        const hash = await withRetry(
          () =>
            wallet.writeContract({
              account,
              address: addr,
              abi: lotteryAbi,
              functionName: "forceCancelStuck",
              nonce: nonceToUse,
            }),
          { tries: 2, baseDelayMs: 250, label: "send forceCancelStuck tx" }
        );

        currentNonce++;
        console.log(`   ✅ forceCancelStuck Tx Sent: ${hash}`);
        txCount++;
      } catch (e: any) {
        const msg = (e?.shortMessage || e?.message || "").toString();
        console.warn(`   ⏭️ forceCancelStuck Tx failed: ${msg}`);
        currentNonce = await resyncNonce(client, account.address, currentNonce);
      }
    }
  }

  await saveState(env, state);

  console.log(
    `🏁 Run complete. txCount=${txCount} watchlist=${Object.keys(state.watch).length} checked=${candidates.length} urgentSoldOut=${urgentSoldOut.length}`
  );
  return txCount;
}