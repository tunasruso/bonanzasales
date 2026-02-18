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
export function getProductCategoryAndWeight(record: any, weights: ProductWeight[]): {
  weight: number,
  category: 'new' | 'second',
  isAPlus: boolean,
  isBedding: boolean
} {
  const pGroup = record.product_group || '';
  const pName = record.product || '';
  const qty = Number(record.quantity_pcs || record.quantity);

  // Default to 'second' if no rule found
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

  // Detection of specific subcategories for detailed report
  const isAPlus = (pName.includes('A+') || pName.includes('А+'));
  const isBedding = (pGroup === 'КПБ' || pGroup.includes('КПБ') || pName.includes('КПБ'));

  // Calculate total weight for this line
  if (category === 'new') {
    return { weight: 0, category: 'new', isAPlus, isBedding };
  }

  let calculated = 0;
  if (match && qty) {
    calculated = qty * avgWeight;
  }

  return { weight: calculated, category, isAPlus, isBedding };
}

// Kept for backward compatibility
export function calculateEstimatedWeight(record: any, weights: ProductWeight[]): number {
  return getProductCategoryAndWeight(record, weights).weight;
}

// In-process accumulation interface
interface AccumulatorDetailedKPI {
  revenue: number;
  kg: number;
  pcs: number;
  checks: Set<string>;
}

export interface DetailedKPI {
  revenue: number;
  kg: number;
  pcs: number;
  checks: number; // Final result is size
}

export interface ShopDetailedKPI {
  store: string;
  total: DetailedKPI;
  second: DetailedKPI;
  aPlus: DetailedKPI;
  bedding: DetailedKPI;
  revenueGrowth: number;
  totalPastRevenue: number;
  revenueGrowthWeek?: number;
  totalPastWeekRevenue?: number;
}

export async function fetchShopDetailedKPIs(
  startDate: string,
  endDate: string,
  stores?: string[]
) {
  // Calculate Comparable Period (one month before)
  const start = new Date(startDate);
  const end = new Date(endDate);
  const prevStart = new Date(start);
  prevStart.setMonth(prevStart.getMonth() - 1);
  const prevEnd = new Date(end);
  prevEnd.setMonth(prevEnd.getMonth() - 1);

  const formattedPrevStart = prevStart.toISOString().split('T')[0];
  const formattedPrevEnd = prevEnd.toISOString().split('T')[0];

  // Calculate Weekly Info if period is <= 7 days
  const timeDiff = end.getTime() - start.getTime();
  const daysDiff = Math.ceil(timeDiff / (1000 * 3600 * 24)) + 1; // +1 to include start date
  const isShortPeriod = daysDiff <= 7;

  let formattedPrevWeekStart = '';
  let formattedPrevWeekEnd = '';

  if (isShortPeriod) {
    const prevWeekStart = new Date(start);
    prevWeekStart.setDate(prevWeekStart.getDate() - 7);
    const prevWeekEnd = new Date(end);
    prevWeekEnd.setDate(prevWeekEnd.getDate() - 7);

    formattedPrevWeekStart = prevWeekStart.toISOString().split('T')[0];
    formattedPrevWeekEnd = prevWeekEnd.toISOString().split('T')[0];
  }

  let query = supabase
    .from('sales_analytics')
    .select('revenue, quantity_kg, quantity_pcs, recorder_id, store, product_group, product')
    .gte('sale_date', startDate)
    .lte('sale_date', endDate);

  if (stores && stores.length > 0) query = query.in('store', stores);

  let prevQuery = supabase
    .from('sales_analytics')
    .select('revenue, store')
    .gte('sale_date', formattedPrevStart)
    .lte('sale_date', formattedPrevEnd);

  if (stores && stores.length > 0) prevQuery = prevQuery.in('store', stores);

  let prevWeekQuery = null;
  if (isShortPeriod) {
    prevWeekQuery = supabase
      .from('sales_analytics')
      .select('revenue, store')
      .gte('sale_date', formattedPrevWeekStart)
      .lte('sale_date', formattedPrevWeekEnd);

    if (stores && stores.length > 0) prevWeekQuery = prevWeekQuery.in('store', stores);
  }

  try {
    const [data, prevData, prevWeekData] = await Promise.all([
      fetchAll(query),
      fetchAll(prevQuery),
      isShortPeriod && prevWeekQuery ? fetchAll(prevWeekQuery) : Promise.resolve([])
    ]);

    if (!data) return [];

    const weights = await fetchProductWeights();
    const shopsMap = new Map<string, { store: string, total: AccumulatorDetailedKPI, second: AccumulatorDetailedKPI, aPlus: AccumulatorDetailedKPI, bedding: AccumulatorDetailedKPI }>();
    const prevShopsRevenue = new Map<string, number>();
    const prevWeekShopsRevenue = new Map<string, number>();

    // Sum up previous period revenue
    prevData.forEach((r: any) => {
      const sName = r.store || 'Unknown';
      prevShopsRevenue.set(sName, (prevShopsRevenue.get(sName) || 0) + Number(r.revenue));
    });

    // Sum up prev week revenue
    if (prevWeekData) {
      prevWeekData.forEach((r: any) => {
        const sName = r.store || 'Unknown';
        prevWeekShopsRevenue.set(sName, (prevWeekShopsRevenue.get(sName) || 0) + Number(r.revenue));
      });
    }

    const getEmptyDetailed = (): AccumulatorDetailedKPI => ({ revenue: 0, kg: 0, pcs: 0, checks: new Set<string>() });

    data.forEach((r: any) => {
      const storeName = r.store || 'Unknown';
      if (!shopsMap.has(storeName)) {
        shopsMap.set(storeName, {
          store: storeName,
          total: getEmptyDetailed(),
          second: getEmptyDetailed(),
          aPlus: getEmptyDetailed(),
          bedding: getEmptyDetailed()
        });
      }

      const shop = shopsMap.get(storeName)!;
      const { weight: calculatedWeight, category, isAPlus, isBedding } = getProductCategoryAndWeight(r, weights);

      let rowKg = 0;
      if (category === 'second') {
        rowKg = calculatedWeight > 0 ? calculatedWeight : Number(r.quantity_kg);
      }

      const rowRev = Number(r.revenue);
      const rowPcs = Number(r.quantity_pcs);
      const checkId = r.recorder_id ? (r.recorder_id.includes('_') ? r.recorder_id.split('_')[0] : r.recorder_id) : null;

      // Update TOTAL
      shop.total.revenue += rowRev;
      shop.total.kg += rowKg;
      shop.total.pcs += rowPcs;
      if (checkId) shop.total.checks.add(checkId);

      // Update SECOND
      if (category === 'second') {
        shop.second.revenue += rowRev;
        shop.second.kg += rowKg;
        shop.second.pcs += rowPcs;
        if (checkId) shop.second.checks.add(checkId);
      }

      // Update A+ (Subcategory of Second)
      if (category === 'second' && isAPlus) {
        shop.aPlus.revenue += rowRev;
        shop.aPlus.kg += rowKg;
        shop.aPlus.pcs += rowPcs;
        if (checkId) shop.aPlus.checks.add(checkId);
      }

      // Update NEW GOODS (КПБ column — includes ALL 'new' category items: КПБ, Наволочка, Простыня, Полотенце, etc.)
      if (category === 'new') {
        shop.bedding.revenue += rowRev;
        shop.bedding.kg += 0; // New goods have no weight
        shop.bedding.pcs += rowPcs;
        if (checkId) shop.bedding.checks.add(checkId);
      }
    });

    // Finalize
    const result: ShopDetailedKPI[] = Array.from(shopsMap.values()).map(s => {
      const pastRevenue = prevShopsRevenue.get(s.store) || 0;
      let revenueGrowth = 0;
      if (pastRevenue > 0) {
        revenueGrowth = ((s.total.revenue - pastRevenue) / pastRevenue) * 100;
      }

      const pastWeekRevenue = prevWeekShopsRevenue.get(s.store) || 0;
      let revenueGrowthWeek: number | undefined = undefined;
      if (isShortPeriod) {
        if (pastWeekRevenue > 0) {
          revenueGrowthWeek = ((s.total.revenue - pastWeekRevenue) / pastWeekRevenue) * 100;
        } else {
          revenueGrowthWeek = s.total.revenue > 0 ? 100 : 0;
        }
      }

      return {
        store: s.store,
        total: { ...s.total, checks: s.total.checks.size },
        second: { ...s.second, checks: s.second.checks.size },
        aPlus: { ...s.aPlus, checks: s.aPlus.checks.size },
        bedding: { ...s.bedding, checks: s.bedding.checks.size },
        revenueGrowth,
        totalPastRevenue: pastRevenue,
        revenueGrowthWeek,
        totalPastWeekRevenue: isShortPeriod ? pastWeekRevenue : undefined
      };
    });
    return result;
  } catch (error) {
    console.error('Error fetching shop detailed KPIs:', error);
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
  let query = supabase
    .from('sales_analytics')
    .select('revenue, quantity_kg, quantity_pcs, recorder_id, store, product_group, product')
    .gte('sale_date', startDate)
    .lte('sale_date', endDate);

  if (stores && stores.length > 0) query = query.in('store', stores);
  if (productGroups && productGroups.length > 0) query = query.in('product_group', productGroups);
  if (products && products.length > 0) query = query.in('product', products);

  try {
    const data = await fetchAll(query);
    if (!data) return null;

    const weights = await fetchProductWeights();

    const total = { revenue: 0, kg: 0, pcs: 0, checks: new Set<string>(), positions: 0 };
    const second = { revenue: 0, kg: 0, pcs: 0, checks: new Set<string>(), positions: 0 };
    const newGoods = { revenue: 0, kg: 0, pcs: 0, checks: new Set<string>(), positions: 0 };

    data.forEach((r: any) => {
      const { weight: calculatedWeight, category } = getProductCategoryAndWeight(r, weights);

      let rowKg = 0;
      if (category === 'second') {
        rowKg = calculatedWeight > 0 ? calculatedWeight : Number(r.quantity_kg);
      }

      const rowRev = Number(r.revenue);
      const rowPcs = Number(r.quantity_pcs);
      const checkId = r.recorder_id ? (r.recorder_id.includes('_') ? r.recorder_id.split('_')[0] : r.recorder_id) : null;

      total.revenue += rowRev;
      total.kg += rowKg;
      total.pcs += rowPcs;
      total.positions++;
      if (checkId) total.checks.add(checkId);

      if (category === 'second') {
        second.revenue += rowRev;
        second.kg += rowKg;
        second.pcs += rowPcs;
        second.positions++;
        if (checkId) second.checks.add(checkId);
      } else {
        newGoods.revenue += rowRev;
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
        checks: second.checks.size,
        avgCheck: second.checks.size > 0 ? second.revenue / second.checks.size : 0,
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

