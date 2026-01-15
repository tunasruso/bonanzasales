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
  const query = supabase
    .from('sales_analytics')
    .select(column)
    .order(column);

  try {
    const data = await fetchAll(query); // Use fetchAll to get all rows
    const uniqueValues = [...new Set(data?.map(item => (item as any)[column]).filter(Boolean))];
    return uniqueValues as string[];
  } catch (error) {
    console.error(`Error fetching ${column}:`, error);
    return [];
  }
}

export async function fetchKPIs(
  startDate: string,
  endDate: string,
  stores?: string[],
  productGroups?: string[],
  products?: string[]
) {
  // Use a simpler query for KPIs to save bandwidth if possible, but for now reuse fetchAll logic
  // Ideally, we should use Database Functions (RPC) for aggregation to avoid downloading MBs of data.
  // But given the constraints, we will download all necessary columns.

  let query = supabase
    .from('sales_analytics')
    .select('revenue, quantity_kg, quantity_pcs, recorder_id, store, product_group')
    .gte('sale_date', startDate)
    .lte('sale_date', endDate);

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
    const data = await fetchAll(query); // Reuse fetchAll helper defined above
    if (!data) return null;

    const totalRevenue = data.reduce((sum: number, r: any) => sum + Number(r.revenue), 0);
    const totalKg = data.reduce((sum: number, r: any) => sum + Number(r.quantity_kg), 0);
    const totalPcs = data.reduce((sum: number, r: any) => sum + Number(r.quantity_pcs), 0);
    const uniqueChecks = new Set(data.map((r: any) => r.recorder_id)).size;
    const avgCheck = uniqueChecks > 0 ? totalRevenue / uniqueChecks : 0;

    return {
      totalRevenue,
      totalKg,
      totalPcs,
      uniqueChecks,
      avgCheck,
      totalTransactions: data.length
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
