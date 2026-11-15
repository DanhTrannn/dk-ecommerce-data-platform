# D&K Legacy Storefront

This retained Next.js 15 / React 19 application contains shopping, POS, administration and role-dashboard screens implemented with TypeScript, Tailwind CSS and Apache ECharts. It remains in source and CI, but **there is no `storefront` service in the current Docker Compose runtime**. The current interface is the Streamlit Platform Console in [platform-app](../platform-app/README.md); business sales BI uses the separate Superset PoC described in [architecture](../../docs/architecture.md).

The backend has retired several reads still used here. A successful frontend build does not make these screens a supported current serving path. Shopping and some operational calls still have corresponding API routes; this is source compatibility, not an end-to-end runtime guarantee. See the [active API inventory](../platform-app/src/platform_app/api/README.md).

## Retained screens and backend compatibility

Routes use the App Router. [Page source](src/app), [client role rules](src/lib/role-routes.ts), [store layout](src/app/store/layout.tsx), [API wrappers](src/lib/api.ts) and [commerce wrappers](src/lib/commerce.ts) describe the legacy client behavior. UI visibility does not grant API permission.

| UI paths | Present behavior / backend limitations |
|---|---|
| `/`, `/products`, `/products/[slug]` | Public product browsing, filters, variants and approved reviews; corresponding catalog/location/review GET routes remain |
| `/login`, `/register` | Cookie authentication routes remain |
| `/cart`, `/wishlist`, `/checkout`, `/checkout/result/[orderNumber]` | Authenticated shopping and checkout calls remain; public page visibility still requires login for private data/actions |
| `/orders`, `/orders/[orderNumber]`, `/orders/returns/[returnCode]` | Owned order/return reads and permitted cancel, complete, review and return actions remain |
| `/admin` | Calls retired GET `/api/v1/admin/overview`; overview data is unavailable through that route |
| `/admin/analytics` | Seven legacy role views remain in source. Business role metrics, store selection and sales-trend reads are retired; System HTTP Metrics/Realtime have retained, independent endpoints |
| `/admin/products` | UI allows inventory manager/admin; product list and create/update/archive/variant calls remain |
| `/admin/inbound`, `/admin/inbound/[receiptCode]` | List GET is retired; receipt creation/detail remain for inventory manager/admin |
| `/admin/coupons`, `/admin/reviews` | List GETs are retired; marketing manager/admin mutations remain |
| `/admin/orders`, `/admin/orders/[orderNumber]` | Admin list GET is retired; detail/transitions remain for admin. The UI also admits operations managers, whom these APIs reject |
| `/admin/logistics` | Shipment list/detail GETs are retired; delivery-staff management and order logistics transitions remain admin-only |
| `/admin/returns`, `/admin/returns/[returnCode]` | Admin return list GET is retired; detail/review/receive/inspection routes remain admin-only. Operations-manager UI access does not grant backend access |
| `/admin/customers` | Client requests an unrestricted list; retained API now requires exact `email`, so this call returns 422 |
| `/admin/pos` | Redirects to `/store/pos` |
| `/store`, `/store/orders`, `/store/inventory`, `/store/staff` | Store dashboard, list, inventory and staff GETs used by these screens are retired |
| `/store/orders/[orderNumber]` | Detail and confirm/cancel APIs remain for assigned store manager/admin; confirm/cancel require CSRF and an idempotency key |
| `/store/pos` | POS search/transaction APIs remain. UI permits store manager/admin; API additionally admits other assigned staff and exempts admin/system_admin from assignment checks |

Absent GET routes may yield 404 or 405 depending on other registered methods/paths. Retained mutation/detail APIs do not restore the legacy list or dashboard calls. Some screens load several dependencies together; one retained call does not imply the entire screen works. There is no separate delivery-staff login or customer profile-edit route.

## Historical dashboard behavior

Dashboard components and frontend response types retain the former 15-OLTP-mart/three-log-mart design. They are historical UI contracts, not evidence that retired backend aggregates are still served. Executive, sales, store, inventory, operations, marketing and system views remain in the client; the current Superset sales PoC is independent of these pages and uses two publication-pinned sales marts.

The two supported HTTP reads are GET `/api/v1/admin/analytics/http-metrics?days=7` and GET `/api/v1/admin/analytics/http-realtime?minutes=15`, both restricted to `admin`/`system_admin`. Published HTTP Metrics accepts 1–30 UTC dates and reads the independently published hourly route mart with bounded last-good server fallback. HTTP Realtime accepts 5–60 minutes and reads one Landing snapshot without server last-good cache or publication requirement. Both return aggregate observations; they do not run pipelines or provide business-event realtime. Their detailed data, empty-result and failure contracts live in the [API guide](../platform-app/src/platform_app/api/README.md).

The legacy System UI separates reconciliation, published HTTP and realtime tabs. HTTP polling/cache behavior is implemented in the retained components, but the default reconciliation request still targets retired `role-metrics`. Realtime polling runs every 10 seconds only while visible and retains the previous observation/timestamp after a failed refresh; this client behavior does not change the backend's no-store contract. Published percentile groups retain hour/route/method grain; the client must not interpret them as a global percentile. Missing values and observed zeros remain distinct.

## Local development

Use Node.js 22 and npm 10+ as declared in [package.json](package.json), and run a reachable API separately. From the repository root:

```bash
cd services/storefront
npm ci
npm run dev
```

Open `http://localhost:3000`. Configure browser-facing values before development/build, for example in `services/storefront/.env.local`:

| Variable | Default | Use |
|---|---|---|
| `NEXT_PUBLIC_API_BASE_URL` | `http://localhost:8000` | Browser-reachable FastAPI URL |
| `NEXT_PUBLIC_CSRF_COOKIE_NAME` | `web_csrf` | Readable cookie copied into `X-CSRF-Token` |

Public variables are compiled into the browser build. Changing them requires a rebuild for production output. Configure API CORS to match the exact browser origin, and use matching hosts for local cookie sessions (for example `localhost` for both browser app and API). The API's default local cookies are not Secure. See [setup](../../docs/setup.md) for backend initialization/settings.

There is no supported `docker compose ... storefront` command in the current Compose file. The retained [Dockerfile](Dockerfile) and [Next configuration](next.config.mjs) describe a standalone-output image; their presence does not register a runtime service. For a local production build check, use `npm run build`. The retained image runs the generated standalone `server.js`.

## Static checks and build

From the repository root, after `npm ci` in this directory:

```bash
npm --prefix services/storefront run lint
npm --prefix services/storefront run typecheck
npm --prefix services/storefront run build
```

The current CI retains these frontend checks. They validate source/build compatibility, not the existence of backend routes, user permissions, transaction persistence, publications or pipeline execution. No frontend behavior or retired API is restored by this documentation update.
