// app/routes/app.fulfillment.tsx
import enTranslations from "@shopify/polaris/locales/en.json";
import "@shopify/polaris/build/esm/styles.css";
import type { ActionFunctionArgs, LoaderFunctionArgs } from "react-router";
import { useFetcher, useLoaderData } from "react-router";
import React, { useEffect, useMemo, useState } from "react";
import {
  AppProvider as PolarisAppProvider,
  Page,
  Layout,
  Card,
  Text,
  BlockStack,
  InlineStack,
  Button,
  IndexTable,
  Box,
  Spinner,
  TextField,
  Checkbox,
  Banner,
  Badge,
  Divider,
} from "@shopify/polaris";
import { authenticate } from "../shopify.server";

/**
 * =========================
 * Types
 * =========================
 */

type OrderListItem = {
  id: string;
  name: string;
  createdAt?: string | null;
  financialStatus: string | null;
  fulfillmentStatus: string | null;
};

type LoaderData =
  | {
      ok: true;
      orders: OrderListItem[];
    }
  | {
      ok: false;
      error: string;
      details?: string;
    };

type OrderDetailsOk = {
  ok: true;
  intent: "order_details";
  order: {
    id: string;
    name: string;
    financialStatus: string | null;
    fulfillmentStatus: string | null;
  };
  fulfillmentOrders: Array<{
    id: string;
    status: string | null;
    requestStatus: string | null;
    assignedLocationName: string | null;
    lineItems: Array<{
      id: string; // fulfillmentOrderLineItem id
      remainingQuantity: number;
      totalQuantity: number;
      title: string;
      sku: string | null;
      variantTitle: string | null;
      lineItemId?: string;
    }>;
  }>;
};

type CsvSyncOk = {
  ok: true;
  intent: "csv_sync";
  filename: string;
  totalRows: number;
  processed: number;
  createdFulfillments: number;
  failed: number;
  errorsSample: Array<{ row: number; order_name?: string; error: string }>;
};

type CreateGroupedOk = {
  ok: true;
  intent: "create_fulfillments_grouped";
  orderId: string;
  created: number;
  results: Array<{ fulfillmentId: string | null; status: string | null; key: string }>;
};

type ActionErr = {
  ok: false;
  intent: string;
  error: string;
  details?: string;
};

type AnyFetcherData = OrderDetailsOk | CsvSyncOk | CreateGroupedOk | ActionErr | undefined;

/**
 * =========================
 * Helpers
 * =========================
 */

function jsonResponse(body: any, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function normalizeOrderName(raw: string): string {
  const s = String(raw || "").trim();
  if (!s) return "";
  if (/^\d+$/.test(s)) return `#${s}`;
  if (s.startsWith("#")) return s;
  return s;
}

function parseCsvText(csvText: string): { headers: string[]; rows: string[][] } {
  const text = csvText.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines = text.split("\n").filter((l) => l.trim().length > 0);
  const rows: string[][] = [];
  for (const line of lines) rows.push(parseCsvLine(line));
  if (rows.length === 0) return { headers: [], rows: [] };
  const headers = rows[0].map((h) => h.trim());
  const data = rows.slice(1);
  return { headers, rows: data };
}

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      const next = line[i + 1];
      if (inQuotes && next === '"') {
        cur += '"';
        i++;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out.map((v) => v.trim());
}

function csvRowToObj(headers: string[], row: string[]): Record<string, string> {
  const obj: Record<string, string> = {};
  for (let c = 0; c < headers.length; c++) obj[headers[c] || `col_${c + 1}`] = String(row[c] ?? "");
  return obj;
}

function pickHeader(obj: Record<string, string>, names: string[]) {
  const keys = Object.keys(obj);
  const map: Record<string, string> = {};
  for (const k of keys) map[k.toLowerCase()] = k;
  for (const n of names) {
    const hit = map[n.toLowerCase()];
    if (hit) return obj[hit];
  }
  return "";
}

function statusTone(fulfillmentStatus?: string | null): "success" | "attention" | "warning" | "info" {
  const s = String(fulfillmentStatus || "").toUpperCase();
  if (s.includes("FULFILLED")) return "success";
  if (s.includes("PARTIAL")) return "attention";
  if (s.includes("UNFULFILLED")) return "warning";
  return "info";
}

function isFulfillableFOStatus(s?: string | null) {
  const st = String(s || "").toUpperCase();
  if (!st) return true;
  if (st === "CLOSED") return false;
  if (st === "ON_HOLD") return false; // optional
  return true; // OPEN / IN_PROGRESS / SCHEDULED...
}

/**
 * =========================
 * loader(): list unfulfilled/partial orders
 * =========================
 */
export async function loader({ request }: LoaderFunctionArgs) {
  try {
    const { admin } = await authenticate.admin(request);

    const q = `fulfillment_status:unfulfilled OR fulfillment_status:partial`;

    const res = await admin.graphql(
      `#graphql
      query ListOrders($q: String!) {
        orders(first: 50, query: $q, sortKey: CREATED_AT, reverse: true) {
          nodes {
            id
            name
            createdAt
            displayFinancialStatus
            displayFulfillmentStatus
          }
        }
      }`,
      { variables: { q } }
    );

    const json: any = await res.json();

    const orders: OrderListItem[] = (json?.data?.orders?.nodes || []).map((o: any) => ({
      id: String(o.id),
      name: String(o.name || ""),
      createdAt: o.createdAt ?? null,
      financialStatus: o.displayFinancialStatus ?? null,
      fulfillmentStatus: o.displayFulfillmentStatus ?? null,
    }));

    return jsonResponse({ ok: true, orders } satisfies LoaderData);
  } catch (e: any) {
    if (e instanceof Response) return e;
    return jsonResponse({ ok: false, error: "Loader error.", details: e?.message ? String(e.message) : String(e) }, 500);
  }
}

/**
 * =========================
 * action()
 * =========================
 */
export async function action({ request }: ActionFunctionArgs) {
  const form = await request.formData();
  const intent = String(form.get("intent") || "").trim();

  try {
    const { admin } = await authenticate.admin(request);

    // =========================
    // ORDER DETAILS
    // =========================
    if (intent === "order_details") {
      const orderId = String(form.get("order_id") || "");
      if (!orderId) return jsonResponse({ ok: false, intent, error: "Missing order_id" } satisfies ActionErr, 400);

      const res = await admin.graphql(
        `#graphql
        query OrderDetails($id: ID!) {
          order(id: $id) {
            id
            name
            displayFinancialStatus
            displayFulfillmentStatus
            fulfillmentOrders(first: 50) {
              nodes {
                id
                status
                requestStatus
                lineItems(first: 100) {
                  edges {
                    node {
                      id
                      remainingQuantity
                      totalQuantity
                      lineItem {
                        id
                        title
                        sku
                        variantTitle
                      }
                    }
                  }
                }
              }
            }
          }
        }`,
        { variables: { id: orderId } }
      );

      const json: any = await res.json();
      if (json?.errors?.length) {
        return jsonResponse(
          { ok: false, intent, error: "Shopify GraphQL error while loading order details.", details: JSON.stringify(json.errors, null, 2) } satisfies ActionErr,
          500
        );
      }

      const order = json?.data?.order;
      if (!order?.id) return jsonResponse({ ok: false, intent, error: "Order not found (or access denied)." } satisfies ActionErr, 404);

      const fulfillmentOrders = (order.fulfillmentOrders?.nodes || []).map((fo: any) => ({
        id: String(fo.id),
        status: fo.status ?? null,
        requestStatus: fo.requestStatus ?? null,
        lineItems: (fo.lineItems?.edges || [])
        .map((e: any) => e?.node)
        .filter(Boolean)
        .map((li: any) => {
          const vt = li?.lineItem?.variantTitle ?? null;
          return {
            id: String(li.id),
            remainingQuantity: Number(li.remainingQuantity ?? 0),
            totalQuantity: Number(li.totalQuantity ?? 0),
            title: String(li?.lineItem?.title || ""),
            sku: li?.lineItem?.sku ?? null,
            variantTitle: vt ? vt : null, // hoặc vt?.trim() ? vt : null
            lineItemId: String(li?.lineItem?.id || ""),
          };
        }),
      }));

      return jsonResponse({
        ok: true,
        intent,
        order: {
          id: String(order.id),
          name: String(order.name),
          financialStatus: order.displayFinancialStatus ?? null,
          fulfillmentStatus: order.displayFulfillmentStatus ?? null,
        },
        fulfillmentOrders,
      } satisfies OrderDetailsOk);
    }

    // =========================
    // CREATE FULFILLMENTS GROUPED BY TRACKING
    // =========================
    if (intent === "create_fulfillments_grouped") {
      const orderId = String(form.get("order_id") || "");
      const notifyCustomer = String(form.get("notify_customer") || "") === "1";

      if (!orderId) return jsonResponse({ ok: false, intent, error: "Missing order_id" } satisfies ActionErr, 400);

      // picked foLineItemIds are those with pick_<id> === "1"
      const pickedIds = Array.from(form.keys())
        .filter((k) => k.startsWith("pick_") && String(form.get(k) || "") === "1")
        .map((k) => k.replace("pick_", ""));

      if (pickedIds.length === 0) {
        return jsonResponse({ ok: false, intent, error: "Please pick at least 1 item." } satisfies ActionErr, 400);
      }

      type PickedItem = {
        foLineItemId: string;
        fulfillmentOrderId: string;
        quantity: number;
        trackingNumber: string;
        carrier: string;
      };

      const items: PickedItem[] = pickedIds
        .map((foLineItemId) => {
          const fulfillmentOrderId = String(form.get(`fo_${foLineItemId}`) || "");
          const qty = Math.max(0, Number(String(form.get(`qty_${foLineItemId}`) || "0")));
          const trackingNumber = String(form.get(`tn_${foLineItemId}`) || "").trim();
          const carrier = String(form.get(`cr_${foLineItemId}`) || "").trim();
          return { foLineItemId, fulfillmentOrderId, quantity: qty, trackingNumber, carrier };
        })
        .filter((x) => x.fulfillmentOrderId && x.quantity > 0);

      if (items.length === 0) {
        return jsonResponse({ ok: false, intent, error: "Quantity must be > 0 for picked items." } satisfies ActionErr, 400);
      }

      // Group by tracking key
      const groupMap = new Map<string, PickedItem[]>();
      for (const it of items) {
        const key = it.trackingNumber ? `${it.trackingNumber}|||${it.carrier || ""}` : "NO_TRACK";
        const arr = groupMap.get(key) || [];
        arr.push(it);
        groupMap.set(key, arr);
      }

      const results: Array<{ fulfillmentId: string | null; status: string | null; key: string }> = [];

      for (const [key, groupItems] of groupMap.entries()) {
        // Sub-group by fulfillmentOrderId (required by API)
        const byFO = new Map<string, { id: string; quantity: number }[]>();
        for (const it of groupItems) {
          const arr = byFO.get(it.fulfillmentOrderId) || [];
          arr.push({ id: it.foLineItemId, quantity: it.quantity });
          byFO.set(it.fulfillmentOrderId, arr);
        }

        const [tn, cr] = key === "NO_TRACK" ? ["", ""] : key.split("|||");

        const trackingInfo =
          tn && tn.trim().length > 0
            ? {
                number: tn.trim(),
                company: cr && cr.trim().length > 0 ? cr.trim() : undefined,
              }
            : undefined;

        const lineItemsByFulfillmentOrder = Array.from(byFO.entries()).map(([fulfillmentOrderId, foLineItems]) => ({
          fulfillmentOrderId,
          fulfillmentOrderLineItems: foLineItems.map((x) => ({ id: x.id, quantity: x.quantity })),
        }));

        const res = await admin.graphql(
          `#graphql
          mutation CreateFulfillment($fulfillment: FulfillmentV2Input!) {
            fulfillmentCreateV2(fulfillment: $fulfillment) {
              fulfillment { id status }
              userErrors { field message }
            }
          }`,
          {
            variables: {
              fulfillment: {
                notifyCustomer,
                trackingInfo,
                lineItemsByFulfillmentOrder,
              },
            },
          }
        );

        const json: any = await res.json();
        if (json?.errors?.length) {
          return jsonResponse({ ok:false, error:"GraphQL error", details: JSON.stringify(json.errors,null,2)}, 500);
        }

        const out = json?.data?.fulfillmentCreateV2;
        const errs = out?.userErrors || [];
        if (errs.length) {
          return jsonResponse(
            { ok: false, intent, error: errs[0]?.message || "Fulfillment failed", details: JSON.stringify(errs, null, 2) } satisfies ActionErr,
            400
          );
        }

        results.push({
          fulfillmentId: out?.fulfillment?.id ?? null,
          status: out?.fulfillment?.status ?? null,
          key,
        });
      }

      return jsonResponse({
        ok: true,
        intent,
        orderId,
        created: results.length,
        results,
      } satisfies CreateGroupedOk);
    }

    // =========================
    // CSV SYNC (upload + sync immediately)
    // columns:
    // order_name (required), tracking_number (optional), carrier(optional), notify_customer(optional 1/0/true/false)
    // =========================
    if (intent === "csv_sync") {
      const file = form.get("csv_file");
      if (!(file instanceof File)) return jsonResponse({ ok: false, intent, error: "Please upload a CSV file." } satisfies ActionErr, 400);

      const filename = file.name || "upload.csv";
      const text = await file.text();
      const { headers, rows } = parseCsvText(text);
      if (!headers.length) return jsonResponse({ ok: false, intent, error: "CSV seems empty or invalid (no header row found)." } satisfies ActionErr, 400);

      const errorsSample: Array<{ row: number; order_name?: string; error: string }> = [];
      let processed = 0;
      let createdFulfillments = 0;

      for (let i = 0; i < rows.length; i++) {
        const rowNum = i + 2;
        const obj = csvRowToObj(headers, rows[i]);

        const orderNameRaw = pickHeader(obj, ["order_name", "order", "name"]);
        const trackingNumber = pickHeader(obj, ["tracking_number", "tracking", "tn"]);
        const carrier = pickHeader(obj, ["carrier", "company", "shipping_company"]);
        const notifyRaw = pickHeader(obj, ["notify_customer", "notify"]);

        const normalized = normalizeOrderName(orderNameRaw);
        if (!normalized) {
          if (errorsSample.length < 20) errorsSample.push({ row: rowNum, error: "Missing order_name" });
          continue;
        }

        const notifyCustomer =
          String(notifyRaw || "").toLowerCase() === "1" ||
          String(notifyRaw || "").toLowerCase() === "true" ||
          String(notifyRaw || "").toLowerCase() === "yes";

        try {
          // find order
          const res1 = await admin.graphql(
            `#graphql
            query FindOrder($q: String!) {
              orders(first: 5, query: $q) { nodes { id name } }
            }`,
            { variables: { q: `name:${normalized}` } }
          );
          const json1: any = await res1.json();
          const order = json1?.data?.orders?.nodes?.[0];
          if (!order?.id) {
            if (errorsSample.length < 20) errorsSample.push({ row: rowNum, order_name: normalized, error: "Order not found" });
            continue;
          }

          // load FOs
          const res2 = await admin.graphql(
            `#graphql
            query FOs($id: ID!) {
              order(id: $id) {
                fulfillmentOrders(first: 50) {
                  nodes {
                    id
                    lineItems(first: 100) {
                      nodes { id remainingQuantity }
                    }
                  }
                }
              }
            }`,
            { variables: { id: order.id } }
          );
          const json2: any = await res2.json();
          const fos = json2?.data?.order?.fulfillmentOrders?.nodes || [];
          if (!fos.length) {
            if (errorsSample.length < 20) errorsSample.push({ row: rowNum, order_name: normalized, error: "No fulfillmentOrders (not shippable / no location)" });
            continue;
          }

          const trackingInfo =
            String(trackingNumber || "").trim().length > 0
              ? { number: String(trackingNumber).trim(), company: String(carrier || "").trim() || undefined }
              : undefined;

          for (const fo of fos) {
            const liNodes = fo?.lineItems?.nodes || [];
            const lineItems = liNodes
              .map((li: any) => ({ id: String(li.id), quantity: Number(li.remainingQuantity ?? 0) }))
              .filter((x: any) => x.quantity > 0);
            if (!lineItems.length) continue;

            const res3 = await admin.graphql(
              `#graphql
              mutation CreateFulfillment($fulfillment: FulfillmentV2Input!) {
                fulfillmentCreateV2(fulfillment: $fulfillment) {
                  fulfillment { id status }
                  userErrors { field message }
                }
              }`,
              {
                variables: {
                  fulfillment: {
                    notifyCustomer,
                    trackingInfo,
                    lineItemsByFulfillmentOrder: [
                      {
                        fulfillmentOrderId: String(fo.id),
                        fulfillmentOrderLineItems: lineItems.map((x: any) => ({ id: x.id, quantity: x.quantity })),
                      },
                    ],
                  },
                },
              }
            );

            const json3: any = await res3.json();
            const out = json3?.data?.fulfillmentCreateV2;
            const errs = out?.userErrors || [];
            if (json3?.errors?.length || errs.length) {
              const msg = json3?.errors?.length ? "GraphQL error" : errs[0]?.message || "Fulfillment failed";
              if (errorsSample.length < 20) errorsSample.push({ row: rowNum, order_name: normalized, error: msg });
              continue;
            }

            createdFulfillments += 1;
          }

          processed += 1;
        } catch (e: any) {
          if (errorsSample.length < 20) errorsSample.push({ row: rowNum, order_name: normalized, error: e?.message ? String(e.message) : String(e) });
        }
      }

      return jsonResponse({
        ok: true,
        intent,
        filename,
        totalRows: rows.length,
        processed,
        createdFulfillments,
        failed: rows.length - processed,
        errorsSample,
      } satisfies CsvSyncOk);
    }

    return jsonResponse({ ok: false, intent, error: "Unknown action intent." } satisfies ActionErr, 400);
  } catch (e: any) {
    if (e instanceof Response) return e;
    return jsonResponse(
      { ok: false, intent, error: "Server error while processing request.", details: e?.message ? String(e.message) : String(e) } satisfies ActionErr,
      500
    );
  }
}

/**
 * =========================
 * UI state for per-item tracking
 * =========================
 */
type ItemState = {
  picked: boolean;
  qty: string; // keep string to avoid Polaris TextField issues
  tn: string;
  cr: string;
  foId: string; // fulfillmentOrderId
  max: number;
};

function buildInitialItemState(details: OrderDetailsOk): Record<string, ItemState> {
  const map: Record<string, ItemState> = {};
  for (const fo of details.fulfillmentOrders) {
    for (const li of fo.lineItems) {
      map[li.id] = {
        picked: false,
        qty: "0",
        tn: "",
        cr: "",
        foId: fo.id,
        max: li.remainingQuantity,
      };
    }
  }
  return map;
}

/**
 * =========================
 * Page
 * =========================
 */
export default function FulfillmentCenterPage() {
  const loader = useLoaderData() as LoaderData;

  const orders: OrderListItem[] = loader && (loader as any).ok ? (loader as any).orders : [];

  // Bulk CSV sync fetcher
  const bulkFetcher = useFetcher<AnyFetcherData>();
  const bulkOk =
    bulkFetcher.data && (bulkFetcher.data as any).ok === true && (bulkFetcher.data as any).intent === "csv_sync"
      ? (bulkFetcher.data as CsvSyncOk)
      : null;
  const bulkErr =
    bulkFetcher.data && (bulkFetcher.data as any).ok === false && (bulkFetcher.data as any).intent === "csv_sync"
      ? (bulkFetcher.data as ActionErr)
      : null;

  // Details fetcher (lazy load when expand)
  const detailsFetcher = useFetcher<AnyFetcherData>();
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [detailsById, setDetailsById] = useState<Record<string, OrderDetailsOk>>({});
  const [itemsByOrderId, setItemsByOrderId] = useState<Record<string, Record<string, ItemState>>>({});

  // Create fulfillment(s) fetcher
  const createFetcher = useFetcher<AnyFetcherData>();
  const createOk =
    createFetcher.data && (createFetcher.data as any).ok === true && (createFetcher.data as any).intent === "create_fulfillments_grouped"
      ? (createFetcher.data as CreateGroupedOk)
      : null;
  const createErr =
    createFetcher.data && (createFetcher.data as any).ok === false && (createFetcher.data as any).intent === "create_fulfillments_grouped"
      ? (createFetcher.data as ActionErr)
      : null;

  // Apply-to-all tracking controls (per expanded order)
  const [applyAllTN, setApplyAllTN] = useState("");
  const [applyAllCR, setApplyAllCR] = useState("");

  // detailsFetcher -> store
  useEffect(() => {
    const d: any = detailsFetcher.data;
    if (d?.ok === true && d.intent === "order_details") {
      const details = d as OrderDetailsOk;
      setDetailsById((prev) => ({ ...prev, [details.order.id]: details }));
      setItemsByOrderId((prev) => {
        const cur = prev[details.order.id] || {};
        const init = buildInitialItemState(details);
        // init = default, cur = user typed (override)
        return { ...prev, [details.order.id]: { ...init, ...cur } };
      });
    }
  }, [detailsFetcher.data]);

  const expandedDetails = expandedId ? detailsById[expandedId] : null;
  const expandedItems = expandedId ? itemsByOrderId[expandedId] : null;

  // open/close row
  const toggleOrder = (orderId: string) => {
    setExpandedId((cur) => (cur === orderId ? null : orderId));
    // reset apply-to-all inputs when switching
    setApplyAllTN("");
    setApplyAllCR("");
    if (!detailsById[orderId]) {
      const fd = new FormData();
      fd.set("intent", "order_details");
      fd.set("order_id", orderId);
      detailsFetcher.submit(fd, { method: "post" });
    }
  };

  const setItem = (orderId: string, itemId: string, patch: Partial<ItemState>) => {
    setItemsByOrderId((prev) => {
      const curOrder = prev[orderId] || {};
      const cur = curOrder[itemId];
      if (!cur) return prev;
      return {
        ...prev,
        [orderId]: {
          ...curOrder,
          [itemId]: { ...cur, ...patch },
        },
      };
    });
  };

  const applyToAll = () => {
    if (!expandedId || !expandedItems || !mergedItems.length) return;

    setItemsByOrderId((prev) => {
      const curOrder = prev[expandedId] || {};
      const next: Record<string, ItemState> = { ...curOrder };

      for (const li of mergedItems) {
        const canFulfill = isFulfillableFOStatus(li._foStatus);
        if (!canFulfill) continue;
        const it = next[li.id];
        if (!it) continue;
        next[li.id] = { ...it, tn: applyAllTN, cr: applyAllCR, foId: li._foId };
      }
      return { ...prev, [expandedId]: next };
    });
  };

  const mergedItems = useMemo(() => {
    if (!expandedDetails) return [];

    type M = OrderDetailsOk["fulfillmentOrders"][number]["lineItems"][number] & {
      _foId: string;
      _foStatus: string;
      _key: string;
    };

    // lấy tất cả lineItems còn remaining > 0 từ mọi FO (kể cả IN_PROGRESS)
    const all: M[] = expandedDetails.fulfillmentOrders.flatMap((fo) => {
      const foStatus = String(fo.status || "").toUpperCase();
      return fo.lineItems
        .filter((li) => Number(li.remainingQuantity || 0) > 0)
        .map((li) => ({
          ...li,
          _foId: fo.id,
          _foStatus: foStatus,
          _key: li.lineItemId || li.id, // dedupe theo lineItemId
        }));
    });

    // dedupe theo _key, ưu tiên FO status OPEN, rồi đến non-CLOSED
    const bestByKey = new Map<string, M>();

    const score = (s: string) => {
      if (s === "OPEN") return 3;
      if (s && s !== "CLOSED") return 2; // IN_PROGRESS/ON_HOLD/SCHEDULED...
      return 1;
    };

    for (const x of all) {
      const cur = bestByKey.get(x._key);
      if (!cur || score(x._foStatus) > score(cur._foStatus)) {
        bestByKey.set(x._key, x);
      }
    }

    return Array.from(bestByKey.values());
  }, [expandedDetails]);

  const anyPickedCount = useMemo(() => {
    if (!expandedId || !expandedItems) return 0;

    return mergedItems.filter((li) => {
      const canFulfill = isFulfillableFOStatus(li._foStatus);
      if (!canFulfill) return false;
      const st = expandedItems[li.id];
      return st?.picked && Number(st.qty || 0) > 0;
    }).length;
  }, [expandedId, expandedItems, mergedItems]);
  return (
    <PolarisAppProvider i18n={enTranslations}>
      <Page title="Fulfillment Center">
        <Layout>
          {/* =========================
              BULK CSV
            ========================= */}
          <Layout.Section>
            <Card>
              <BlockStack gap="300">
                <Text as="h2" variant="headingMd">
                  Bulk Fulfill (CSV Sync)
                </Text>

                <Text as="p" tone="subdued">
                  Upload CSV and click Sync. Suggested columns: <code>order_name</code>, <code>tracking_number</code>, <code>carrier</code>,{" "}
                  <code>notify_customer</code>.
                </Text>

                <bulkFetcher.Form method="post" encType="multipart/form-data">
                  <input type="hidden" name="intent" value="csv_sync" />
                  <BlockStack gap="300">
                    <div>
                      <Text as="p" fontWeight="semibold">
                        CSV file
                      </Text>
                      <input name="csv_file" type="file" accept=".csv,text/csv" />
                    </div>

                    <InlineStack gap="200" align="start">
                      <Button submit variant="primary" loading={bulkFetcher.state !== "idle"}>
                        Sync CSV
                      </Button>
                    </InlineStack>
                  </BlockStack>
                </bulkFetcher.Form>

                {bulkErr ? (
                  <Banner tone="critical" title="CSV Sync Error">
                    <p>{bulkErr.error}</p>
                    {bulkErr.details ? <pre style={{ whiteSpace: "pre-wrap" }}>{bulkErr.details}</pre> : null}
                  </Banner>
                ) : null}

                {bulkOk ? (
                  <Banner tone="success" title="CSV Synced">
                    <p>
                      Processed <b>{bulkOk.processed}</b> / {bulkOk.totalRows} rows · created fulfillments: <b>{bulkOk.createdFulfillments}</b> ·
                      failed: <b>{bulkOk.failed}</b>
                    </p>

                    {bulkOk.errorsSample?.length ? (
                      <>
                        <Divider />
                        <p style={{ marginTop: 8 }}>
                          <b>Error sample</b> (first {bulkOk.errorsSample.length})
                        </p>
                        <ul>
                          {bulkOk.errorsSample.map((e, idx) => (
                            <li key={idx}>
                              Row {e.row} {e.order_name ? `(${e.order_name})` : ""}: {e.error}
                            </li>
                          ))}
                        </ul>
                      </>
                    ) : null}
                  </Banner>
                ) : null}
              </BlockStack>
            </Card>
          </Layout.Section>

          {/* =========================
              MANUAL (inline expand)
            ========================= */}
          <Layout.Section>
            <Card>
              <BlockStack gap="300">
                <InlineStack align="space-between">
                  <Text as="h2" variant="headingMd">
                    Manual Fulfill (Orders not fulfilled)
                  </Text>
                  {loader && (loader as any).ok === false ? (
                    <Badge tone="critical">Loader error</Badge>
                  ) : (
                    <Badge tone="info">{`${orders.length} orders`}</Badge>
                  )}
                </InlineStack>

                <Text as="p" tone="subdued">
                  Danh sách bên dưới là các order <b>Unfulfilled / Partial</b>. Bấm Open để xổ ngay dưới dòng đó và nhập tracking theo từng item.
                  (Nếu tracking khác nhau, Shopify sẽ tạo nhiều fulfillment theo nhóm tracking.)
                </Text>

                {createErr ? (
                  <Banner tone="critical" title="Create fulfillment error">
                    <p>{createErr.error}</p>
                    {createErr.details ? <pre style={{ whiteSpace: "pre-wrap" }}>{createErr.details}</pre> : null}
                  </Banner>
                ) : null}

                {createOk ? (
                  <Banner tone="success" title="Fulfillment(s) created">
                    <p>
                      Created <b>{createOk.created}</b> fulfillment(s) for <code>{createOk.orderId}</code>
                    </p>
                    <ul>
                      {createOk.results.map((r, i) => (
                        <li key={i}>
                          [{r.key}] id: <code>{r.fulfillmentId || ""}</code> · status: <b>{r.status || ""}</b>
                        </li>
                      ))}
                    </ul>
                  </Banner>
                ) : null}

                <IndexTable
                  resourceName={{ singular: "order", plural: "orders" }}
                  itemCount={orders.length}
                  headings={[
                    { title: "" },
                    { title: "Order" },
                    { title: "Financial" },
                    { title: "Fulfillment" },
                    { title: "Created" },
                  ]}
                  selectable={false}
                >
                  {orders.map((o, idx) => {
                    const isOpen = expandedId === o.id;
                    const details = detailsById[o.id];

                    const detailsErr =
                      isOpen &&
                      detailsFetcher.state === "idle" &&
                      (detailsFetcher.data as any)?.ok === false &&
                      (detailsFetcher.data as any)?.intent === "order_details"
                        ? (detailsFetcher.data as ActionErr)
                        : null;

                    // chỉ loading khi fetcher đang chạy
                    const isLoading = isOpen && detailsFetcher.state !== "idle" && !details;

                    return (
                      <React.Fragment key={o.id}>
                        <IndexTable.Row id={o.id} position={idx}>
                          <IndexTable.Cell>
                            <Button size="slim" onClick={() => toggleOrder(o.id)}>
                              {isOpen ? "Close" : "Open"}
                            </Button>
                          </IndexTable.Cell>

                          <IndexTable.Cell>
                            <InlineStack gap="200" align="start">
                              <Text as="span" fontWeight="semibold">
                                {o.name}
                              </Text>
                              <Badge tone={statusTone(o.fulfillmentStatus)}>{String(o.fulfillmentStatus || "")}</Badge>
                            </InlineStack>
                          </IndexTable.Cell>

                          <IndexTable.Cell>{o.financialStatus || ""}</IndexTable.Cell>
                          <IndexTable.Cell>{o.fulfillmentStatus || ""}</IndexTable.Cell>
                          <IndexTable.Cell>{o.createdAt ? String(o.createdAt).slice(0, 10) : ""}</IndexTable.Cell>
                        </IndexTable.Row>

                        {/* ✅ Inline expanded row under the order row */}
                        {isOpen ? (
                          <IndexTable.Row id={`${o.id}-details`} position={idx + 0.1}>
                            <IndexTable.Cell colSpan={5}>
                              <Box padding="300" background="bg-surface-secondary" borderRadius="200">
                                {isLoading ? (
                                  <InlineStack gap="200" align="center">
                                    <Spinner size="small" />
                                    <Text as="span">loading...</Text>
                                  </InlineStack>
                                ) : detailsErr ? (
                                  <Banner tone="critical" title="Load order details failed">
                                    <p>{detailsErr.error}</p>
                                    {detailsErr.details ? <pre style={{ whiteSpace: "pre-wrap" }}>{detailsErr.details}</pre> : null}
                                  </Banner>
                                ) : details ? (
                                  // render details như cũ
                                  <BlockStack gap="300">
                                    <Card>
                                      <BlockStack gap="200">
                                        {details.fulfillmentOrders.length === 0 ? (
                                          <Banner tone="warning" title="No fulfillment orders">
                                            <p>
                                              Order này không có fulfillmentOrders. Thường là: item không requires shipping,
                                              hoặc không có location fulfill/assigned.
                                            </p>
                                          </Banner>
                                        ) : null}
                                        <InlineStack align="space-between">
                                          <Text as="p" tone="subdued">
                                            FOs: {details.fulfillmentOrders.length} — Items:{" "}
                                            {details.fulfillmentOrders.reduce((s, fo) => s + fo.lineItems.length, 0)}
                                          </Text>
                                          <InlineStack gap="200">
                                            <Badge tone="info">{details.order.financialStatus || "—"}</Badge>
                                            <Badge tone={statusTone(details.order.fulfillmentStatus)}>{details.order.fulfillmentStatus || "—"}</Badge>
                                          </InlineStack>
                                        </InlineStack>

                                        {/* Apply-to-all tracking */}
                                        <Divider />

                                        <InlineStack gap="300" align="start">
                                          <div style={{ minWidth: 260 }}>
                                            <TextField
                                              label="Tracking number (apply to all items)"
                                              value={applyAllTN}
                                              onChange={setApplyAllTN}
                                              autoComplete="off"
                                            />
                                          </div>
                                          <div style={{ minWidth: 220 }}>
                                            <TextField
                                              label="Carrier (apply to all items)"
                                              value={applyAllCR}
                                              onChange={setApplyAllCR}
                                              autoComplete="off"
                                            />
                                          </div>
                                          <div style={{ paddingTop: 22 }}>
                                            <Button onClick={applyToAll} disabled={!expandedId || !expandedItems}>
                                              Apply
                                            </Button>
                                          </div>
                                        </InlineStack>

                                        <Text as="p" tone="subdued">
                                          Tip: Tick item + set Qty &gt; 0. Tracking có thể khác nhau theo item (app sẽ tự group).
                                        </Text>
                                      </BlockStack>
                                    </Card>

                                    {/* Create form */}
                                    <createFetcher.Form method="post">
                                      <input type="hidden" name="intent" value="create_fulfillments_grouped" />
                                      <input type="hidden" name="order_id" value={details.order.id} />

                                      <Card>
                                        <BlockStack gap="300">
                                          <InlineStack align="space-between" gap="200">
                                            <Checkbox
                                              label="Notify customer"
                                              checked={true}
                                              onChange={() => {
                                                /* we keep default on for UX; hidden input controls actual submit */
                                              }}
                                            />
                                            {/* Real submit value */}
                                            <input type="hidden" name="notify_customer" value="1" />

                                            <InlineStack gap="200" align="end">
                                              <Badge tone="info">{`Picked: ${anyPickedCount}`}</Badge>
                                              <Button
                                                submit
                                                variant="primary"
                                                loading={createFetcher.state !== "idle"}
                                                disabled={createFetcher.state !== "idle" || anyPickedCount === 0}
                                              >
                                                Create fulfillment(s)
                                              </Button>
                                            </InlineStack>
                                          </InlineStack>

                                          {/* Hidden inputs mirror current state so submit works */}
                                          {expandedId && expandedItems
                                            ? mergedItems.map((li) => {
                                                const st = expandedItems[li.id];
                                                if (!st) return null;
                                                const liId = li.id;
                                                return (
                                                  <React.Fragment key={liId}>
                                                    <input type="hidden" name={`fo_${liId}`} value={li._foId} />
                                                    <input type="hidden" name={`pick_${liId}`} value={st.picked ? "1" : "0"} />
                                                    <input type="hidden" name={`qty_${liId}`} value={st.qty} />
                                                    <input type="hidden" name={`tn_${liId}`} value={st.tn} />
                                                    <input type="hidden" name={`cr_${liId}`} value={st.cr} />
                                                  </React.Fragment>
                                                );
                                              })
                                            : null}

                                          {/* Items table */}
                                          <div style={{ overflowX: "auto" }}>
                                            <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 980 }}>
                                              <thead>
                                                <tr>
                                                  {["Pick", "Title", "SKU", "Variant", "Remaining", "Qty", "Tracking #", "Carrier"].map((h) => (
                                                    <th
                                                      key={h}
                                                      style={{
                                                        textAlign: "left",
                                                        padding: "10px 10px",
                                                        borderBottom: "1px solid rgba(0,0,0,.12)",
                                                        whiteSpace: "nowrap",
                                                      }}
                                                    >
                                                      <Text as="span" fontWeight="semibold">
                                                        {h}
                                                      </Text>
                                                    </th>
                                                  ))}
                                                </tr>
                                              </thead>

                                              <tbody>
                                                {mergedItems.map((li) => {
                                                  // ⚠️ st đang key theo fulfillmentOrderLineItemId (li.id)
                                                  // mà mergedItems đã dedupe => st có thể undefined nếu bạn chọn "bản khác"
                                                  // nên mình lấy st theo li.id hiện tại (bản đầu tiên sau dedupe)
                                                  const st0 = expandedId && expandedItems ? expandedItems[li.id] : undefined;
                                                  if (!st0) return null;

                                                  // ép state dùng FO đang render (để create_fulfillments_grouped submit đúng fulfillmentOrderId)
                                                  const st = st0.foId === li._foId ? st0 : { ...st0, foId: li._foId };

                                                  const canFulfill = isFulfillableFOStatus(li._foStatus);

                                                  const max = st.max;

                                                  return (
                                                    <tr key={li._key}>
                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)" }}>
                                                        <Checkbox
                                                          label=""
                                                          checked={st.picked}
                                                          disabled={!canFulfill}
                                                          onChange={(val) => setItem(expandedId!, li.id, { picked: val })}
                                                        />
                                                      </td>

                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)" }}>
                                                        <BlockStack gap="050">
                                                          <Text as="p" fontWeight="semibold">
                                                            {li.title}
                                                          </Text>
                                                        </BlockStack>
                                                      </td>

                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)" }}>
                                                        <Text as="span">{li.sku || ""}</Text>
                                                      </td>

                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)" }}>
                                                        <Text as="span">{li.variantTitle || "—"}</Text>
                                                      </td>

                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)" }}>
                                                        <Badge tone={max > 0 ? "info" : "critical"}>{String(max)}</Badge>
                                                      </td>

                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)", width: 120 }}>
                                                        <TextField
                                                          label=""
                                                          type="number"
                                                          value={st.qty}
                                                          disabled={!canFulfill}
                                                          onChange={(val) => {
                                                            const n = Math.max(0, Number(val || 0));
                                                            const clamped = isFinite(n) ? Math.min(n, max) : 0;
                                                            setItem(expandedId!, li.id, { qty: String(clamped) });
                                                          }}
                                                          autoComplete="off"
                                                        />
                                                      </td>

                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)", width: 220 }}>
                                                        <TextField
                                                          label=""
                                                          value={st.tn}
                                                          disabled={!canFulfill}
                                                          onChange={(val) => setItem(expandedId!, li.id, { tn: val })}
                                                          placeholder="1Z... / YT..."
                                                          autoComplete="off"
                                                        />
                                                      </td>

                                                      <td style={{ padding: "10px 10px", borderBottom: "1px solid rgba(0,0,0,.08)", width: 190 }}>
                                                        <TextField
                                                          label=""
                                                          value={st.cr}
                                                          disabled={!canFulfill}
                                                          onChange={(val) => setItem(expandedId!, li.id, { cr: val })}
                                                          placeholder="USPS / UPS..."
                                                          autoComplete="off"
                                                        />
                                                      </td>
                                                    </tr>
                                                  );
                                                })}
                                              </tbody>
                                            </table>
                                          </div>

                                          <Text as="p" tone="subdued">
                                            Nếu bạn nhập tracking khác nhau cho từng item, app sẽ tự group theo tracking và tạo nhiều fulfillments.
                                          </Text>
                                        </BlockStack>
                                      </Card>
                                    </createFetcher.Form>
                                  </BlockStack>
                                ) : null}
                              </Box>
                            </IndexTable.Cell>
                          </IndexTable.Row>
                        ) : null}
                      </React.Fragment>
                    );
                  })}
                </IndexTable>

                {!orders.length ? (
                  <Banner tone="info" title="No orders">
                    <p>Không có order nào đang Unfulfilled/Partial.</p>
                  </Banner>
                ) : null}
              </BlockStack>
            </Card>
          </Layout.Section>
        </Layout>
      </Page>
    </PolarisAppProvider>
  );
}