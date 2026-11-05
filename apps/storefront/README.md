# D&K Storefront Application

Next.js 15 / React 19 application for shopping, POS, administration and role dashboards. TypeScript, Tailwind CSS and Apache ECharts implement the UI. Routes use the App Router; Docker runs the standalone production output.

## Routes

Public route visibility does not grant access to authenticated cart/checkout/order APIs. API roles and store scope remain authoritative. Sources: [pages](src/app), [role rules](src/lib/role-routes.ts), [store layout](src/app/store/layout.tsx).

| Path | UI access / behavior |
|---|---|
| `/`, `/products`, `/products/[slug]` | Public browsing, filters, variants and approved reviews |
| `/login`, `/register` | Authentication |
| `/cart`, `/wishlist`, `/checkout` | Authenticated data/actions; unauthenticated UI prompts login |
| `/checkout/result/[orderNumber]` | Checkout receipt/result |
| `/orders`, `/orders/[orderNumber]` | Owned order history/detail, receipt confirmation, review/return actions |
| `/orders/returns/[returnCode]` | Owned return request detail |
| `/admin` | Admin overview |
| `/admin/analytics` | All staff; server restricts requested view and store scope |
| `/admin/products` | Inventory manager / admin |
| `/admin/inbound`, `/admin/inbound/[receiptCode]` | Inventory manager / admin |
| `/admin/coupons`, `/admin/reviews` | Marketing manager / admin |
| `/admin/orders`, `/admin/orders/[orderNumber]` | Admin / operations manager in UI; API currently requires admin |
| `/admin/logistics`, `/admin/returns`, `/admin/returns/[returnCode]` | Admin / operations manager in UI; API currently requires admin |
| `/admin/customers` | Admin |
| `/admin/pos` | Admin entry point that redirects to `/store/pos` |
| `/store`, `/store/pos` | Store manager / admin; store assignment rules apply at API |
| `/store/orders`, `/store/orders/[orderNumber]` | Store manager / admin, scoped store orders |
| `/store/inventory`, `/store/staff` | Store manager / admin, scoped inventory/staff |

The POS API also accepts other staff assigned to a store, with admin/system-admin exceptions, but the `/store` layout is narrower. There is no separate delivery-staff login or customer profile-edit route.

## Dashboard behavior

BI has seven role views, with compact KPI summaries, charts and collapsed detail tables. Sales separates store/trend analysis from products/categories; Operations separates logistics from returns; Marketing separates log interactions from coupons/reviews. System separates reconciliation, published HTTP Metrics and provisional HTTP Realtime from Landing. Admin and Store overview pages use the same compact components; duplicated banners, generic next-action lists and always-visible definition tables have been removed.

All 15 OLTP marts and 3 log marts are represented by a KPI, chart or detail view. The [design rationale and mart mapping](../../docs/design-analysis/DASHBOARD_DESIGN.md) records official Power BI/Tableau/W3C sources, audience decisions and exact coverage. Published-mart contracts, KPI formulas, source data and RBAC are preserved; HTTP Realtime has a separate Landing contract.

Filters retain their real scope: 7/30/90 days only change sales trend; store selection changes the authorized store view; 1/7/30 UTC dates only change HTTP reporting. Marketing has independent log and commerce observation/publication/cache states. Published HTTP percentiles stay at hour/route/method grain; zero denominators remain undefined. Missing values render as dashes, observed zero remains zero, and missing time buckets remain gaps. Search/sort/CSV changes only the opened detail table. Top subsets are labeled.

The UI retains last-successful data during refresh, marks failures/cache and uses a shared in-flight request for trend/HTTP reads of the same window. Hidden HTTP does not fetch. HTTP Realtime alone has a 5/15/60-minute sliding window, independent source/snapshot timestamps and 10-second visible-only polling with in-flight reuse; it stops when leaving the view. Its minute trends, error rates, direct approximate p95 and Top 20 routes are provisional. Invalid/replayed/conflicting/late/future observations are in collapsed quality details; raw payloads are never returned. An unavailable Landing read retains the previous timestamp/result with an error label; it does not borrow a published mart or MySQL aggregate. Other analytics refresh every 15 seconds while visible; this does not run Spark or advance the underlying published data. Publication times use UTC+7; HTTP window labels are explicitly UTC. No unsupported targets/deltas or inferred pipeline health are added.

## Run with Docker

Configure the repository `.env` first, then from the repository root:

```bash
docker compose --profile core up -d --build storefront
```

Open `http://localhost:3000`. Compose profiles add to the selection in COMPOSE_PROFILES. To rebuild only the running frontend after a UI edit:

```bash
docker compose up -d --no-deps --build storefront
```

The image does not bind-mount frontend source. Restarting an old image does not pick up source changes.

## Local development

Use Node.js 22 as in CI, and run a reachable API separately:

```bash
cd apps/storefront
npm ci
npm run dev
```

| Variable | Default | Use |
|---|---|---|
| `NEXT_PUBLIC_API_BASE_URL` | `http://localhost:8000` | Browser-reachable FastAPI URL |
| `NEXT_PUBLIC_CSRF_COOKIE_NAME` | `web_csrf` | Cookie copied to the CSRF request header |

Set public variables before `npm run dev` or `npm run build` (for example in `apps/storefront/.env.local`). Compose supplies them as build arguments. Existing production images require rebuilding when these values change. Configure API CORS to match the browser origin; see [setup](../../docs/runbook/SETUP.md).

## Static checks and build

From the repository root:

```bash
npm --prefix apps/storefront run lint
npm --prefix apps/storefront run typecheck
npm --prefix apps/storefront run build
```

These check the frontend source/build. They do not demonstrate that every backend permission, transaction or data pipeline works at runtime. Current API limits are documented in the [API guide](../../services/ecommerce-api/README.md).
