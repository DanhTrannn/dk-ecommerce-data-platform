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

Seven views cover executive, sales, marketing, store, inventory, operations and system. Executive/sales/store/inventory/operations read published marts through the API; admin overview uses the admin/order marts, and system compares the published order mart with one current MySQL observation. Marketing combines published log funnel/product-demand marts with published OLTP coupon/review marts. Its source panel shows independent observation/publication times, availability and cache age; a missing source leaves that section empty without hiding the other. KPI request events are not unique people or orders, and undefined ratios remain blank.

The 7/30/90-day control changes sales trend, not every KPI. Store selection applies to the store view subject to API scope. Search, sorting and CSV export operate within individual tables. Analytics refresh every 15 seconds while visible; this does not run Spark/Airflow or advance frozen mart windows. Marketing's log marts update through the independent hourly DAG; its commerce windows use a separate publication. No OLTP proxy is shown.

Admin overview retains null as a dash, includes all order states and displays its observation/publication timestamps; low-stock counts mean ≤5 units, and returned orders are not return requests. System uses matched/mismatch/unavailable/cached states with a source observation time rather than CDC heartbeat or inferred pipeline health. Source zero leaves relative variance blank while absolute variance remains meaningful. System reconciliation uses a separate bar chart for each metric: orders use an order-count axis, money uses a VND axis. Monetary tooltips preserve full values; variance badges use API percentages. Fallback shows missing comparison data instead of treating placeholder Gold values as measurements. Targets and change deltas without source data remain hidden. See [metric definitions](../../docs/architecture/METRICS_CATALOG.md).

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
