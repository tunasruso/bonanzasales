import { createClient } from '@supabase/supabase-js';

const supabaseUrl = 'https://lyfznzntclgitarujlab.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0';

export const supabase = createClient(supabaseUrl, supabaseKey);

export interface SalesRecord {
  id: number;
  sale_date: string;
  day_of_month: number;
  week_number: number;
  month: number;
  quarter: number;
  year: number;
  weekday: string;
  warehouse: string;
  store: string;
  product: string;
  product_group: string;
  unit: string;
  unit_type: 'kg' | 'pcs';
  quantity: number;
  quantity_pcs: number;
  quantity_kg: number;
  revenue: number;
}

export interface AggregatedData {
  key: string;
  revenue: number;
  quantity_kg: number;
  quantity_pcs: number;
  transactions: number;
}

// Helper to fetch all rows
async function fetchAll(query: any, batchSize = 1000) {
  let allData: any[] = [];
  let from = 0;
  let hasMore = true;

  while (hasMore) {
    const { data, error } = await query.range(from, from + batchSize - 1);

    if (error) {
      console.error('Error fetching batch:', error);
      throw error;
    }

    if (data && data.length > 0) {
      allData = [...allData, ...data];
      from += batchSize;

      // If we got less than requested, it's the last page
      if (data.length < batchSize) {
        hasMore = false;
      }
    } else {
      hasMore = false;
    }
  }
  return allData;
}

export async function fetchSalesData(
  startDate: string,
  endDate: string,
  stores?: string[],
  productGroups?: string[],
  products?: string[]
): Promise<SalesRecord[]> {
  let query = supabase
    .from('sales_analytics')
    .select('*')
    .gte('sale_date', startDate)
    .lte('sale_date', endDate)
    .order('sale_date', { ascending: true });

  if (stores && stores.length > 0) {
    query = query.in('store', stores);
  }

  if (productGroups && productGroups.length > 0) {
    query = query.in('product_group', productGroups);
  }

  if (products && products.length > 0) {
    query = query.in('product', products);
  }

  try {
    const data = await fetchAll(query);
    return data;
  } catch (error) {
    console.error('Error fetching full sales data:', error);
    return [];
  }
}

export async function fetchDistinctValues(column: string): Promise<string[]> {
  const { data, error } = await supabase
    .rpc('get_distinct_values', { col_name: column });

  if (error) {
    console.error(`Error fetching ${column}:`, error);
    return [];
  }

  // RPC returns array of objects: [{ value: 'Store1' }, { value: 'Store2' }]
  // Or if we defined returns setof text -> returns array of strings?
  // Let's check return type. "returns table (value text)" -> [{ value: "..." }]
  return (data || []).map((item: any) => item.value).filter(Boolean);
}

export interface ProductWeight {
  id: number;
  product_group: string;
  product_name_pattern: string | null;
  avg_weight_kg: number;
  category: 'new' | 'second'; // Added category
  created_at: string;
}

// Helper: Get weight AND category
export function getProductCategoryAndWeight(record: any, weights: ProductWeight[]): { weight: number, category: 'new' | 'second' } {
  const pGroup = record.product_group || '';
  const pName = record.product || '';
  const qty = Number(record.quantity_pcs || record.quantity);

  // Default to 'second' if no rule found (as per instructions: "Everything else is Second")
  let category: 'new' | 'second' = 'second';
  let avgWeight = 0;

  // Find matching rule
  let match = weights.find(w =>
    w.product_group === pGroup &&
    w.product_name_pattern &&
    pName.includes(w.product_name_pattern)
  );

  if (!match) {
    match = weights.find(w =>
      w.product_group === pGroup &&
      !w.product_name_pattern
    );
  }

  // Global pattern match (e.g. АКЦИЯ)
  if (!match) {
    match = weights.find(w =>
      w.product_name_pattern &&
      (w.product_group === '%' || w.product_group === 'АКЦИЯ') &&
      pName.includes(w.product_name_pattern)
    );
  }

  if (match) {
    category = match.category || 'second';
    avgWeight = Number(match.avg_weight_kg);
  }

  // Calculate total weight for this line
  // If New -> Weight is 0 (User requirement: "БЕЗ килограммов")
  if (category === 'new') {
    return { weight: 0, category: 'new' };
  }

  // If Second -> Use calculated if rule exists, else 0 (caller handles fallback to 1C)
  // Wait, if match found -> return calculated.
  // If NO match -> return 0 (caller handles fallback).
  let calculated = 0;
  if (match && qty) {
    calculated = qty * avgWeight;
  }

  return { weight: calculated, category };
}

// Kept for backward compatibility if needed, but wrapper around above
export function calculateEstimatedWeight(record: any, weights: ProductWeight[]): number {
  return getProductCategoryAndWeight(record, weights).weight;
}

export async function fetchKPIs(
  startDate: string,
  endDate: string,
  stores?: string[],
  productGroups?: string[],
  products?: string[]
) {
  // ... (Query logic same as before)
  let query = supabase
    .from('sales_analytics')
    .select('revenue, quantity_kg, quantity_pcs, recorder_id, store, product_group, product') // Added product for matching
    .gte('sale_date', startDate)
    .lte('sale_date', endDate);

  if (stores && stores.length > 0) query = query.in('store', stores);
  if (productGroups && productGroups.length > 0) query = query.in('product_group', productGroups);
  if (products && products.length > 0) query = query.in('product', products);

  try {
    const data = await fetchAll(query); // Use fetchAll
    if (!data) return null;

    // Fetch weights
    const wRes = await supabase.from('product_weights').select('*');
    const weights = (wRes.data || []) as ProductWeight[];

    // Initialize accumulators
    const total = { revenue: 0, kg: 0, pcs: 0, checks: new Set<string>(), positions: 0 };
    const second = { revenue: 0, kg: 0, pcs: 0, checks: new Set<string>(), positions: 0 }; // Only checks containing second?
    const newGoods = { revenue: 0, kg: 0, pcs: 0, checks: new Set<string>(), positions: 0 };

    data.forEach((r: any) => {
      const { weight: calculatedWeight, category } = getProductCategoryAndWeight(r, weights);

      // Determine effective KG
      // If New -> 0
      // If Second -> calculated OR 1C
      let rowKg = 0;
      if (category === 'second') {
        rowKg = calculatedWeight > 0 ? calculatedWeight : Number(r.quantity_kg);
      }

      const rowRev = Number(r.revenue);
      const rowPcs = Number(r.quantity_pcs);
      const checkId = r.recorder_id ? (r.recorder_id.includes('_') ? r.recorder_id.split('_')[0] : r.recorder_id) : null;

      // Update Totals
      total.revenue += rowRev;
      total.kg += rowKg;
      total.pcs += rowPcs;
      total.positions++;
      if (checkId) total.checks.add(checkId);

      // Update Categories
      if (category === 'second') {
        second.revenue += rowRev;
        second.kg += rowKg;
        second.pcs += rowPcs;
        second.positions++; // "Total purchases position (pure second)"
        if (checkId) second.checks.add(checkId);
      } else {
        newGoods.revenue += rowRev;
        // Kg is 0
        newGoods.pcs += rowPcs;
        newGoods.positions++;
        if (checkId) newGoods.checks.add(checkId);
      }
    });

    return {
      total: {
        revenue: total.revenue,
        kg: total.kg,
        pcs: total.pcs,
        checks: total.checks.size,
        avgCheck: total.checks.size > 0 ? total.revenue / total.checks.size : 0,
        pricePerKg: total.kg > 0 ? total.revenue / total.kg : 0,
        positions: total.positions
      },
      second: {
        revenue: second.revenue,
        kg: second.kg,
        pcs: second.pcs,
        checks: second.checks.size, // Checks containing at least one second item (approx)
        avgCheck: second.checks.size > 0 ? second.revenue / second.checks.size : 0, // Revenue of Second / Count of checks with Second
        pricePerKg: second.kg > 0 ? second.revenue / second.kg : 0,
        positions: second.positions
      },
      newGoods: {
        revenue: newGoods.revenue,
        kg: 0,
        pcs: newGoods.pcs,
        checks: newGoods.checks.size,
        avgCheck: newGoods.checks.size > 0 ? newGoods.revenue / newGoods.checks.size : 0,
        pricePerKg: 0,
        positions: newGoods.positions
      }
    };
  } catch (error) {
    console.error('Error fetching KPIs:', error);
    return null;
  }
}

export interface InventoryRecord {
  store: string;
  product: string;
  quantity: number;
  product_group?: string;
}

export async function fetchInventory(): Promise<InventoryRecord[]> {
  // Inventory is not large (a few thousand rows), but using fetchAll is safer
  const query = supabase
    .from('inventory_analytics')
    .select('store, product, quantity, product_group');

  try {
    const data = await fetchAll(query);
    return data;
  } catch (error) {
    console.error('Error fetching inventory:', error);
    return [];
  }
}

export async function fetchProductWeights(): Promise<ProductWeight[]> {
  const { data, error } = await supabase
    .from('product_weights')
    .select('*');

  if (error) {
    console.error('Error fetching weights:', error);
    return [];
  }
  return data as ProductWeight[];
}

export async function checkUser(username: string, password: string): Promise<boolean> {
  const { data, error } = await supabase
    .from('app_users')
    .select('id')
    .eq('username', username)
    .eq('password', password)
    .single();

  if (error || !data) {
    return false;
  }
  return true;
}

