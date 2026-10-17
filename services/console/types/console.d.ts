// What the console's page receives: the shapes of /api/topology and
// /api/state (from tinytrader-fleet), and the widget contract (decision
// 0010). Checked against the page's JavaScript by `tsc -p services/console`.

/** A schedule entry a service describes (`/describe` → `schedule`). */
interface ScheduleEntry {
  name: string;
  kind: 'window' | 'pause' | 'yield' | 'always' | 'interval' | 'event';
  when?: string;
  /** `HH:MM-HH:MM`, New York time, set relative to the regular session. */
  hours?: string;
  every_seconds?: number;
  feed?: string;
  symbols?: string[];
  class?: string;
  yields_to?: string[];
}

interface TopoNode {
  id: string;
  label?: string;
  sub?: string;
  role?: string;
  host?: string;
}

interface TopoService extends TopoNode {
  schedule?: ScheduleEntry[];
  /** One symbol's one-minute bars, on demand (`?symbol=SPY&days=2`). */
  bars?: string;
  /** On-demand stock snapshots (`/api/stream/{id}/snapshots?symbols=`). */
  snapshots?: string;
  /** On-demand option chains (`/api/stream/{id}/chain?underlying=`). */
  chain?: string;
  /** What a chain lists, without prices (`/api/stream/{id}/chain_index?underlying=`). */
  chain_index?: string;
  /** A relay's time and sales (`/api/stream/{id}/tape?symbol=&after=`). */
  tape?: string;
  /** An account service's daily P/L (`/api/stream/{id}/performance?account=`). */
  performance?: string;
  /** An account service's trades on a day (`/api/stream/{id}/trades?account=&date=`). */
  trades?: string;
  /** The options screener (`/api/stream/{id}/screener`). */
  screener?: string;
  /** Every ticker with its company's name (`/api/stream/{id}/tickers`). */
  tickers?: string;
  /** On-demand news for given symbols (`/api/stream/{id}/news_search?symbols=`). */
  news_search?: string;
  /** On-demand past earnings for a company (`/api/stream/{id}/earnings_history?symbol=`). */
  earnings_history?: string;
  /** A relay's description: protocol, listen address, symbols. */
  relay?: { protocol: string; listen: string; symbols: string[] };
  /** A market calendar it serves. */
  calendar?: string;
  events?: string;
  accounts?: unknown;
  observe?: string;
}

interface TopoEdge {
  from: string;
  to: string;
  kind: 'flow' | 'read' | string;
  label?: string;
}

/** A bundle (decision 0011): services sharing hours, with a work class. */
interface Bundle {
  name: string;
  class: 'capture' | 'interactive' | 'background';
  members: string[];
  note?: string;
}

/** Price cards a topology declares: one per key of a service's series stream. */
interface Panel {
  service: string;
  source?: string;
  heading?: string;
  note?: string;
  title?: string;
  decimals?: number;
  prefix?: string;
  suffix?: string;
  format?: string;
  footer?: { metric: string; match?: string; format?: string; decimals?: number }[];
}

/** A brokerage account as the accounts service serves it. */
interface Account {
  /** Its name in the deployment, such as `paper-1`. */
  name?: string;
  label?: string;
  environment?: 'live' | 'paper' | string;
  updated_at?: number;
  account?: Record<string, any>;
  positions?: Record<string, any>[];
  /** Open orders as the broker has them, however placed; null when they couldn't be read. */
  orders?: Record<string, any>[] | null;
}

interface Topology {
  name?: string;
  focus?: string;
  hosts?: { id: string; label?: string; sub?: string }[];
  services: TopoService[];
  external?: TopoNode[];
  stores?: (TopoNode & { metric?: { service: string; name: string; label?: string } })[];
  edges?: TopoEdge[];
  panels?: Panel[];
  bundles?: Bundle[];
  /** Tools the deployment runs beside tinytrader, by name, such as `notebook`. */
  links?: Record<string, string>;
}

/** A part of the deployment graph: an outside source, a service or a store. */
/** A client connected to a relay, from its `/clients` roster. */
interface RelayClient {
  client?: string | null;
  account?: string | null;
  peer: string;
  since: number;
  replay: boolean;
  conditions: boolean;
}

type GraphNode = TopoService & {
  kind: 'ext' | 'svc' | 'store' | 'client' | 'acct';
  /** A client's connections, one per relay. */
  conns?: (RelayClient & { relay: string })[];
  unnamed?: boolean;
  /** An account node's account, when the deployment reads it. */
  account?: Account | null;
  metric?: { service: string; name: string; label?: string; format?: string; decimals?: number };
};

/** A service's health (`/health`), as the fleet polled it. */
interface Health {
  state: 'starting' | 'ready' | 'degraded' | 'idle' | string;
  detail?: string;
  build?: string;
  uptime_seconds?: number;
  last_error?: string;
  last_error_at?: number;
}

interface Metric {
  labels: Record<string, string>;
  value: number;
}

interface Container {
  state: string;
  status: string;
  image?: string;
  started_at?: string;
  restart_count?: number;
  cpu_percent?: number;
  memory?: { usage_bytes?: number };
}

/** A schema's files under one dataset on one drive. */
interface SchemaFiles {
  bytes: number;
  /** Bytes per day, by `YYYY-MM-DD`. */
  days: Record<string, number>;
  encodings?: string[];
  files?: number;
}

interface CatalogDataset {
  id: string;
  symbol?: string;
  sources?: string[];
  schemas: Record<string, SchemaFiles>;
}

interface Volume {
  total_bytes?: number;
  available_bytes?: number;
  used_fraction?: number;
  status: 'ok' | 'warning' | 'critical' | 'floor' | 'unknown';
  reason?: string;
}

interface CatalogRoot {
  name: string;
  path: string;
  present: boolean;
  writable?: boolean;
  volume?: Volume;
  datasets?: CatalogDataset[];
  dataset_bytes?: number;
  other?: { name: string; bytes: number; files?: number; modified?: number }[];
  other_bytes?: number;
  unrecognised?: string[];
  thresholds?: Thresholds;
  service?: string;
}

interface Thresholds {
  warning: number;
  critical: number;
  floor: number;
}

/** A drive the store found but doesn't use (decision 0007). */
interface Candidate {
  path: string;
  label: string;
  total_bytes?: number;
  available_bytes?: number;
}

interface Catalog {
  generated_at: number | null;
  thresholds?: Thresholds;
  roots: CatalogRoot[];
  candidates?: Candidate[];
}

/** Everything the fleet knows about one service. */
interface ServiceState {
  health?: { body?: Health; error?: string };
  container?: Container;
  metrics?: Record<string, Metric[]>;
  catalog?: Catalog;
  calendar?: { clock?: { is_open: boolean; next_open?: string; next_close?: string }; days: { date: string; open: string; close: string }[] };
  clients?: { clients: RelayClient[] };
  earnings?: { reports: { symbol: string; name: string | null; date: string; when: 'before-open' | 'after-close' | 'unknown'; eps_estimate: number | null; eps_actual: number | null; market_cap: number | null; dates: Record<string, string>; disputed: boolean; importance: 'low' | 'medium' | 'high' }[]; sources?: Record<string, { rows: number; error: string | null }> };
  econ?: { events: { at: number; date: string; time: string; title: string; source: string; importance: 'low' | 'medium' | 'high'; category?: 'econ' | 'oil' }[]; sources?: Record<string, { events: number; error: string | null }> };
  news?: { items: { id: string; headline: string; summary?: string; url?: string; source?: string; author?: string; symbols?: string[]; created_at?: string }[] };
  accounts?: { accounts: Account[] };
  /** Series streams (such as `recent`): key → [[unix_ms, value], ...]. */
  [stream: string]: unknown;
}

interface Snapshot {
  services: Record<string, ServiceState>;
  docker?: { configured: boolean; error?: string };
  error?: string;
}

/** TradingView's Lightweight Charts (standalone build), loaded by the Candles widget. */
declare var LightweightCharts: any;

/** A mounted widget (decision 0010). */
interface WidgetInstance {
  /** After each poll of the fleet. */
  update?(): void;
  /** When its panel closes or the workspace changes. */
  dispose?(): void;
}

interface WidgetDef {
  title: string;
  mount(el: HTMLElement): WidgetInstance;
}

/** The state channels (lib/channels.js): each holds its latest value. */
interface Channels {
  /** The deployment, as /api/topology last served it (lib/fleet.js sets it); null until read. */
  topology: Topology | null;
  /** The symbol picked last, which charts and the builders follow (lib/theme.js). */
  focus: string | null;
  /** The account picked for trading, kept in this browser. */
  account: string;
  /** The New York day picked in the Performance view, for other widgets to follow. */
  day: string | null;
}

/** A trading journal entry (services/console/src/journal.rs). */
interface JournalEntry {
  id: string;
  /** The New York day it is about, `YYYY-MM-DD`. */
  day: string;
  /** When it was written, in milliseconds since the epoch. */
  at: number;
  /** The account it refers to; null for all accounts. */
  account: string | null;
  text: string;
  tags: string[];
  /** Discipline, 1 to 5. */
  rating: number | null;
  updated: number;
}
