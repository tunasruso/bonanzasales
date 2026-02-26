import { useState, useEffect, useMemo } from 'react';
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  Area, AreaChart, LineChart, Line
} from 'recharts';
import {
  TrendingUp, Package, Weight, ShoppingCart, Receipt,
  Calendar, Store, Filter, ArrowUpDown, RefreshCw, ChevronDown, ChevronUp
} from 'lucide-react';
import { fetchSalesData, fetchDistinctValues, fetchKPIs, fetchInventory, calculateEstimatedWeight, getProductCategoryAndWeight, supabase, fetchShopDetailedKPIs, fetchVisitors, type SalesRecord, type InventoryRecord, type ShopDetailedKPI, type VisitorRecord } from './lib/supabase';
import Login from './components/Login';
import './index.css';

// Colors for charts
const COLORS = ['#00d4ff', '#a855f7', '#ec4899', '#22c55e', '#f97316', '#eab308', '#06b6d4', '#8b5cf6'];

function formatNumber(num: number | undefined | null, decimals = 0): string {
  if (num === undefined || num === null) return '0';
  return new Intl.NumberFormat('ru-RU', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  }).format(num);
}

function formatCurrency(num: number | undefined | null): string {
  if (num === undefined || num === null) return '0 ₽';
  return new Intl.NumberFormat('ru-RU', {
    style: 'currency',
    currency: 'RUB',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(num);
}

// Weekday localization
const WEEKDAY_MAP: Record<string, string> = {
  'Monday': 'Понедельник',
  'Tuesday': 'Вторник',
  'Wednesday': 'Среда',
  'Thursday': 'Четверг',
  'Friday': 'Пятница',
  'Saturday': 'Суббота',
  'Sunday': 'Воскресенье'
};

// Month localization
const MONTH_MAP: Record<string, string> = {
  '01': 'Январь',
  '02': 'Февраль',
  '03': 'Март',
  '04': 'Апрель',
  '05': 'Май',
  '06': 'Июнь',
  '07': 'Июль',
  '08': 'Август',
  '09': 'Сентябрь',
  '10': 'Октябрь',
  '11': 'Ноябрь',
  '12': 'Декабрь'
};

const WEEKDAY_ORDER = [
  'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'
];

// Helper to get today's date in YYYY-MM-DD format
function getTodayDate(): string {
  const today = new Date();
  return today.toISOString().split('T')[0];
}

export default function App() {
  // Date state - default to today
  const [startDate, setStartDate] = useState(getTodayDate());
  const [endDate, setEndDate] = useState(getTodayDate());

  // Filter state
  const [selectedStores, setSelectedStores] = useState<string[]>([]);
  const [selectedGroups, setSelectedGroups] = useState<string[]>([]);
  const [selectedProducts, setSelectedProducts] = useState<string[]>([]);
  const [stores, setStores] = useState<string[]>([]);
  const [productGroups, setProductGroups] = useState<string[]>([]);
  const [productsList, setProductsList] = useState<string[]>([]);

  // Data state
  const [salesData, setSalesData] = useState<SalesRecord[]>([]);
  const [inventoryData, setInventoryData] = useState<InventoryRecord[]>([]);
  const [kpis, setKpis] = useState<any>(null);
  const [shopKPIs, setShopKPIs] = useState<ShopDetailedKPI[]>([]);
  const [visitorsData, setVisitorsData] = useState<VisitorRecord[]>([]);
  const [productWeights, setProductWeights] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [showFilters, setShowFilters] = useState(window.innerWidth > 768);

  // Pivot state
  const [rowDimension, setRowDimension] = useState('store');
  const [sortColumn, setSortColumn] = useState('revenue');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');

  // Load initial data
  useEffect(() => {
    async function loadDataAndFilters() {
      const [storeList, groupList, prodList, weightsList] = await Promise.all([
        fetchDistinctValues('store'),
        fetchDistinctValues('product_group'),
        fetchDistinctValues('product'),
        supabase.from('product_weights').select('*')
      ]);
      setStores(storeList);
      setProductGroups(groupList);
      setProductsList(prodList);
      if (weightsList.data) setProductWeights(weightsList.data);
    }
    loadDataAndFilters();
  }, []);

  // Load sales and inventory data
  const loadData = async () => {
    setLoading(true);
    const [data, kpiData, inventory, shopData, visitors] = await Promise.all([
      fetchSalesData(startDate, endDate,
        selectedStores.length > 0 ? selectedStores : undefined,
        selectedGroups.length > 0 ? selectedGroups : undefined,
        selectedProducts.length > 0 ? selectedProducts : undefined
      ),
      fetchKPIs(startDate, endDate,
        selectedStores.length > 0 ? selectedStores : undefined,
        selectedGroups.length > 0 ? selectedGroups : undefined,
        selectedProducts.length > 0 ? selectedProducts : undefined
      ),
      fetchInventory(endDate),
      fetchShopDetailedKPIs(startDate, endDate, selectedStores.length > 0 ? selectedStores : undefined),
      fetchVisitors(startDate, endDate, selectedStores.length > 0 ? selectedStores : undefined)
    ]);
    setSalesData(data);
    setKpis(kpiData);
    setInventoryData(inventory);
    setShopKPIs(shopData as ShopDetailedKPI[]);
    setVisitorsData(visitors);
    setLoading(false);
  };

  useEffect(() => {
    loadData();
  }, []);

  // Tab state
  const [activeTab, setActiveTab] = useState<'dashboard' | 'inventory'>('dashboard');

  // Filtered Inventory Data
  const filteredInventory = useMemo(() => {
    return inventoryData.filter(item => {
      if (selectedStores.length > 0 && !selectedStores.includes(item.store)) return false;
      if (selectedGroups.length > 0 && item.product_group && !selectedGroups.includes(item.product_group)) return false;
      if (selectedProducts.length > 0 && !selectedProducts.includes(item.product)) return false;
      return true;
    }).sort((a, b) => b.quantity - a.quantity);
  }, [inventoryData, selectedStores, selectedGroups, selectedProducts]);

  // Aggregate data by dimension
  const aggregatedData = useMemo(() => {
    const grouped = new Map<string, { revenue: number; kg: number; pcs: number; count: number; stock: number }>();

    // Process Sales
    salesData.forEach(record => {
      // Determine key based on dimension
      let key = '';
      if (rowDimension === 'store') key = record.store;
      else if (rowDimension === 'product_group') key = record.product_group;
      else if (rowDimension === 'product') key = record.product;
      else if (rowDimension === 'weekday') key = WEEKDAY_MAP[record.weekday] || record.weekday;
      else if (rowDimension === 'month') {
        const monthNum = String(record.month).padStart(2, '0');
        const monthName = MONTH_MAP[monthNum];
        key = `${monthName} ${record.year}`;
      }

      // Calculate weight (Priority: Calculated > 1C Data)
      const estimatedKg = calculateEstimatedWeight(record, productWeights);
      const rowKg = estimatedKg > 0 ? estimatedKg : Number(record.quantity_kg);

      const existing = grouped.get(key) || { revenue: 0, kg: 0, pcs: 0, count: 0, stock: 0 };
      grouped.set(key, {
        ...existing,
        revenue: existing.revenue + Number(record.revenue),
        kg: existing.kg + rowKg,
        pcs: existing.pcs + Number(record.quantity_pcs),
        count: existing.count + 1
      });
    });

    // Process Inventory
    if (inventoryData.length > 0) {
      // ... (inventory logic same as before, no changes needed for weekday/month grouping as stock is mostly for store/product)
      if (rowDimension === 'store') {
        inventoryData.forEach(item => {
          // Filter by selected stores
          if (selectedStores.length > 0 && !selectedStores.includes(item.store)) return;

          // Filter by selected product groups
          if (selectedGroups.length > 0) {
            if (item.product_group && !selectedGroups.includes(item.product_group)) return;
            if (!item.product_group) return;
          }

          const key = item.store;
          const existing = grouped.get(key) || { revenue: 0, kg: 0, pcs: 0, count: 0, stock: 0 };
          grouped.set(key, {
            ...existing,
            stock: existing.stock + Number(item.quantity)
          });
        });
      } else if (rowDimension === 'product') {
        inventoryData.forEach(item => {
          if (selectedStores.length > 0 && !selectedStores.includes(item.store)) return;

          if (selectedGroups.length > 0) {
            if (item.product_group && !selectedGroups.includes(item.product_group)) return;
            if (!item.product_group) return;
          }

          if (selectedProducts.length > 0 && !selectedProducts.includes(item.product)) return;

          const key = item.product;

          if (key) {
            const existing = grouped.get(key) || { revenue: 0, kg: 0, pcs: 0, count: 0, stock: 0 };
            grouped.set(key, {
              ...existing,
              stock: existing.stock + Number(item.quantity)
            });
          }
        });
      }
    }

    let result = Array.from(grouped.entries()).map(([key, values]) => ({
      name: key || 'Не указано',
      revenue: values.revenue,
      kg: values.kg,
      pcs: values.pcs,
      transactions: values.count,
      stock: values.stock
    }));

    // Sort
    result.sort((a, b) => {
      // Custom sorting for Weekdays
      if (rowDimension === 'weekday') {
        const idxA = WEEKDAY_ORDER.indexOf(a.name);
        const idxB = WEEKDAY_ORDER.indexOf(b.name);
        // If user explicitly sorted by a metric, respect that?
        // The user request says "when grouping by weeks, sort Mon-Sun".
        // Let's assume natural sort order overrides default metric sort for the MAIN view,
        // but if user clicks a column header (e.g. Revenue), they might expect revenue sort?
        // However, `sortColumn` defaults to 'revenue'.
        // Let's implement: If sortColumn is 'revenue' (default) AND dimension is time, force chronological.
        // If user clicks other columns, `sortColumn` changes, and we might want to respect that.
        // But usually time series are best viewed chronically.

        // For now, let's force chronological sort if sortColumn is NOT explicitly set to something else by user click?
        // Actually `sortColumn` is state.

        // User requirement: "sort days from Mon to Sun".
        // Implementation: If sortColumn is 'revenue' (the default) OR 'name' (if we had one), prefer chronological for time dimensions. 
        // But table has sortable headers. 
        // Let's prioritize the specific request: When grouping by weekday, sort Mon -> Sun. 
        // We can do this by checking if the sort column is 'revenue' (default) - or we can just enforce it as a secondary sort or primary?
        // Let's make it the primary sort logic if we are in 'weekday' mode, UNLESS user explicitly clicks something else?
        // Actually, simpler: Just sort by index if we are in weekday mode.
        if (idxA !== -1 && idxB !== -1) return idxA - idxB;
      }

      // Custom sorting for Months
      if (rowDimension === 'month') {
        // Parse "Month YYYY" back to date or just compare?
        // "Январь 2025" vs "Январь 2026" vs "Февраль 2025".
        // Robust way: map month name back to index 0-11, then compare year + month.
        const parseDate = (str: string) => {
          const parts = str.split(' ');
          if (parts.length !== 2) return 0;
          const mName = parts[0];
          const year = parseInt(parts[1]);
          const mIdx = Object.entries(MONTH_MAP).find(([_k, v]) => v === mName)?.[0];
          if (!mIdx) return 0;
          return year * 100 + parseInt(mIdx);
        };
        const valA = parseDate(a.name);
        const valB = parseDate(b.name);
        return valA - valB;
      }

      const aVal = a[sortColumn as keyof typeof a] as number;
      const bVal = b[sortColumn as keyof typeof b] as number;
      return sortDirection === 'desc' ? bVal - aVal : aVal - bVal;
    });

    return result;
  }, [salesData, inventoryData, rowDimension, sortColumn, sortDirection, selectedStores, selectedGroups, selectedProducts]);

  // ... (keep chart data memos)
  // Chart data by month
  const monthlyData = useMemo(() => {
    const grouped = new Map<string, { revenue: number; kg: number; pcs: number }>();

    salesData.forEach(record => {
      const monthKey = `${record.year}-${String(record.month).padStart(2, '0')}`;
      const existing = grouped.get(monthKey) || { revenue: 0, kg: 0, pcs: 0 };

      const estimatedKg = calculateEstimatedWeight(record, productWeights);
      const rowKg = estimatedKg > 0 ? estimatedKg : Number(record.quantity_kg);

      grouped.set(monthKey, {
        revenue: existing.revenue + Number(record.revenue),
        kg: existing.kg + rowKg,
        pcs: existing.pcs + Number(record.quantity_pcs)
      });
    });

    return Array.from(grouped.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([month, values]) => ({
        month,
        revenue: values.revenue,
        kg: values.kg,
        pcs: values.pcs
      }));
  }, [salesData, productWeights]);

  // Chart data by day for daily dynamics
  const dailyData = useMemo(() => {
    const grouped = new Map<string, { revenue: number; secondRevenue: number; secondKg: number }>();

    salesData.forEach(record => {
      const date = record.sale_date;
      const existing = grouped.get(date) || { revenue: 0, secondRevenue: 0, secondKg: 0 };

      const { weight, category } = getProductCategoryAndWeight(record, productWeights);

      grouped.set(date, {
        revenue: existing.revenue + Number(record.revenue),
        secondRevenue: existing.secondRevenue + (category === 'second' ? Number(record.revenue) : 0),
        secondKg: existing.secondKg + (category === 'second' ? weight : 0)
      });
    });

    return Array.from(grouped.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, values]) => ({
        date: date.split('-').slice(1).reverse().join('.'), // DD.MM
        fullDate: date,
        revenue: values.revenue,
        avgPriceSecond: values.secondKg > 0 ? Math.round(values.secondRevenue / values.secondKg) : 0
      }));
  }, [salesData, productWeights]);

  // Pie chart data
  const pieData = useMemo(() => {
    // Re-calculate basic aggregation for pie chart without stock interference
    const simpleAgg = salesData.reduce((acc, curr) => {
      const key = curr.store;
      if (!acc[key]) acc[key] = 0;
      acc[key] += Number(curr.revenue);
      return acc;
    }, {} as Record<string, number>);

    const sorted = Object.entries(simpleAgg)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);

    return sorted.slice(0, 8).map(item => ({
      name: item.name.length > 15 ? item.name.substring(0, 15) + '...' : item.name,
      value: item.value
    }));
  }, [salesData]);

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(prev => prev === 'desc' ? 'asc' : 'desc');
    } else {
      setSortColumn(column);
      setSortDirection('desc');
    }
  };

  if (!isAuthenticated) {
    return <Login onLogin={() => setIsAuthenticated(true)} />;
  }

  return (
    <div className="dashboard">
      {/* ... (keep header and filters) */}
      {/* Header */}
      <header className="dashboard-header">
        <div className="dashboard-title">
          <span className="logo">📊</span>
          <h1>Бонанза продажи</h1>
        </div>
        <div className="tab-controls" style={{ display: 'flex', gap: '10px', marginLeft: '20px' }}>
          <button
            onClick={() => setActiveTab('dashboard')}
            style={{
              background: activeTab === 'dashboard' ? '#3b82f6' : '#1f2937',
              color: 'white',
              border: 'none',
              padding: '8px 16px',
              borderRadius: '6px',
              cursor: 'pointer'
            }}
          >
            Продажи
          </button>
          <button
            onClick={() => setActiveTab('inventory')}
            style={{
              background: activeTab === 'inventory' ? '#3b82f6' : '#1f2937',
              color: 'white',
              border: 'none',
              padding: '8px 16px',
              borderRadius: '6px',
              cursor: 'pointer'
            }}
          >
            Остатки
          </button>
        </div>
        <button className="apply-btn" onClick={loadData} style={{ marginLeft: 'auto' }}>
          <RefreshCw size={16} style={{ marginRight: 8 }} />
          Обновить
        </button>
      </header>

      {/* Filters */}
      {/* Collapsible Filters */}
      <div className="filters-container" style={{ marginBottom: 32 }}>
        <button
          className="filter-toggle-btn"
          onClick={() => setShowFilters(!showFilters)}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            padding: '10px 16px',
            background: 'var(--bg-card)',
            border: '1px solid rgba(255,255,255,0.1)',
            borderRadius: '12px',
            color: 'var(--text-primary)',
            cursor: 'pointer',
            marginBottom: showFilters ? 16 : 0,
            width: '100%',
            justifyContent: 'space-between'
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Filter size={18} />
            <span>Фильтры</span>
          </div>
          {showFilters ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
        </button>

        {showFilters && (
          <div className="filters-bar" style={{ marginBottom: 0 }}>
            {activeTab === 'dashboard' && (
              <div className="filter-group">
                <label>Период</label>
                <div style={{ display: 'flex', gap: 8 }}>
                  <input
                    type="date"
                    value={startDate}
                    onChange={e => setStartDate(e.target.value)}
                  />
                  <input
                    type="date"
                    value={endDate}
                    onChange={e => setEndDate(e.target.value)}
                  />
                </div>
              </div>
            )}

            <div className="filter-group">
              <label>Магазин</label>
              <select
                value={selectedStores[0] || ''}
                onChange={e => setSelectedStores(e.target.value ? [e.target.value] : [])}
              >
                <option value="">Все магазины</option>
                {stores.map(store => (
                  <option key={store} value={store}>{store}</option>
                ))}
              </select>
            </div>

            <div className="filter-group">
              <label>Товарная группа</label>
              <select
                value={selectedGroups[0] || ''}
                onChange={e => setSelectedGroups(e.target.value ? [e.target.value] : [])}
              >
                <option value="">Все группы</option>
                {productGroups.map(group => (
                  <option key={group} value={group}>{group}</option>
                ))}
              </select>
            </div>

            <div className="filter-group">
              <label>Номенклатура</label>
              <select
                value={selectedProducts[0] || ''}
                onChange={e => setSelectedProducts(e.target.value ? [e.target.value] : [])}
                style={{ maxWidth: '200px' }}
              >
                <option value="">Вся номенклатура</option>
                {productsList.map(prod => (
                  <option key={prod} value={prod}>{prod.length > 30 ? prod.substring(0, 30) + '...' : prod}</option>
                ))}
              </select>
            </div>
            <button className="apply-btn" onClick={loadData}>
              <RefreshCw size={16} style={{ marginRight: 8 }} />
              Применить
            </button>
          </div>
        )}
      </div>

      {loading ? (
        <div className="loading">
          <div className="loading-spinner"></div>
          Загрузка данных...
        </div>
      ) : (
        <>
          {activeTab === 'dashboard' ? (
            <>
              {/* KPI Cards */}
              <div className="kpi-table-container">
                <div className="kpi-table">
                  {/* Header */}
                  <div className="kpi-row header">
                    <div className="kpi-cell label">Категория</div>
                    <div className="kpi-cell"><TrendingUp size={16} style={{ marginRight: 4 }} /> Выручка</div>
                    <div className="kpi-cell"><Weight size={16} style={{ marginRight: 4 }} /> Вес (кг)</div>
                    <div className="kpi-cell"><Package size={16} style={{ marginRight: 4 }} /> Шт</div>
                    <div className="kpi-cell"><ShoppingCart size={16} style={{ marginRight: 4 }} /> Чеков</div>
                    <div className="kpi-cell"><Package size={16} style={{ marginRight: 4 }} /> Позиций</div>
                    <div className="kpi-cell"><Receipt size={16} style={{ marginRight: 4 }} /> Ср. чек</div>
                    <div className="kpi-cell"><TrendingUp size={16} style={{ marginRight: 4 }} /> Цена/кг</div>
                  </div>

                  {/* Row 1: TOTAL */}
                  <div className="kpi-row total">
                    <div className="kpi-cell label">ИТОГО</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.total?.revenue || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.total?.kg || 0, 2)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.total?.pcs || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.total?.checks || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.total?.positions || 0)}</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.total?.avgCheck || 0)}</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.total?.pricePerKg || 0)}</div>
                  </div>

                  {/* Row 2: SECOND */}
                  <div className="kpi-row second">
                    <div className="kpi-cell label">СЕКОНД</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.second?.revenue || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.second?.kg || 0, 2)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.second?.pcs || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.second?.checks || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.second?.positions || 0)}</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.second?.avgCheck || 0)}</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.second?.pricePerKg || 0)}</div>
                  </div>

                  {/* Row 3: NEW */}
                  <div className="kpi-row new">
                    <div className="kpi-cell label">НОВЫЙ</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.newGoods?.revenue || 0)}</div>
                    <div className="kpi-cell value dimmed">—</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.newGoods?.pcs || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.newGoods?.checks || 0)}</div>
                    <div className="kpi-cell value">{formatNumber(kpis?.newGoods?.positions || 0)}</div>
                    <div className="kpi-cell value">{formatCurrency(kpis?.newGoods?.avgCheck || 0)}</div>
                    <div className="kpi-cell value dimmed">—</div>
                  </div>
                </div>
              </div>

              {/* Detailed Dashboard Table (Excel-based) */}
              <div className="table-section" style={{ marginTop: 40 }}>
                <div className="table-header">
                  <h3>📊 Детальный отчет по магазинам (21 показатель)</h3>
                </div>
                <div className="detailed-table-wrapper" style={{ overflowX: 'auto', borderRadius: 12, border: '1px solid rgba(255,255,255,0.1)' }}>
                  <table className="detailed-kpi-table">
                    <thead>
                      <tr>
                        <th rowSpan={2} style={{ minWidth: '120px', background: '#1f2937' }}>Магазин</th>
                        <th colSpan={3} style={{ background: 'rgba(59, 130, 246, 0.3)', borderBottom: '2px solid #3b82f6' }}>ИТОГО</th>
                        <th colSpan={3} style={{ background: 'rgba(168, 85, 247, 0.3)', borderBottom: '2px solid #a855f7' }}>СЕКОНД</th>
                        <th colSpan={4} style={{ background: 'rgba(236, 72, 153, 0.3)', borderBottom: '2px solid #ec4899' }}>Категория "А+"</th>
                        <th colSpan={2} style={{ background: 'rgba(6, 182, 212, 0.3)', borderBottom: '2px solid #06b6d4' }}>Новый (КПБ)</th>
                        <th colSpan={5} style={{ background: 'rgba(245, 158, 11, 0.3)', borderBottom: '2px solid #f59e0b' }}>Средний чек</th>
                        <th colSpan={4} style={{ background: 'rgba(16, 185, 129, 0.3)', borderBottom: '2px solid #10b981' }}>Трафик (счетчики)</th>
                      </tr>
                      <tr className="sub-header">
                        {/* ИТОГО sub-columns */}
                        <th style={{ background: 'rgba(59, 130, 246, 0.1)' }}>Выручка</th>
                        <th style={{ background: 'rgba(59, 130, 246, 0.1)' }}>Прирост,<br />% месяц<br />назад</th>
                        <th style={{ background: 'rgba(59, 130, 246, 0.1)' }}>Прирост,<br />% неделю<br />назад</th>

                        {/* СЕКОНД sub-columns */}
                        <th style={{ background: 'rgba(168, 85, 247, 0.1)' }}>Выручка, ₽</th>
                        <th style={{ background: 'rgba(168, 85, 247, 0.1)' }}>Вес, Кг</th>
                        <th style={{ background: 'rgba(168, 85, 247, 0.1)' }}>Цена/Кг, ₽</th>
                        {/* A+ */}
                        <th style={{ background: 'rgba(236, 72, 153, 0.1)' }}>Выручка, ₽</th>
                        <th style={{ background: 'rgba(236, 72, 153, 0.1)' }}>Доля, %</th>
                        <th style={{ background: 'rgba(236, 72, 153, 0.1)' }}>Вес, Кг</th>
                        <th style={{ background: 'rgba(236, 72, 153, 0.1)' }}>Цена/Кг, ₽</th>
                        {/* КПБ */}
                        <th style={{ background: 'rgba(6, 182, 212, 0.1)' }}>Выручка, ₽</th>
                        <th style={{ background: 'rgba(6, 182, 212, 0.1)' }}>Доля, %</th>
                        {/* Ср. чеки */}
                        <th style={{ background: 'rgba(245, 158, 11, 0.1)' }}>Итого, ₽</th>
                        <th style={{ background: 'rgba(245, 158, 11, 0.1)' }}>Секонд, ₽</th>
                        <th style={{ background: 'rgba(245, 158, 11, 0.1)' }}>"А+", ₽</th>
                        <th style={{ background: 'rgba(245, 158, 11, 0.1)' }}>КПБ, ₽</th>
                        <th className="highlight-red" style={{ background: 'rgba(245, 158, 11, 0.1)' }}>Ср. кол-во тов. в чеке</th>
                        {/* Трафик */}
                        <th style={{ background: 'rgba(16, 185, 129, 0.1)' }}>Трафик, Чел</th>
                        <th style={{ background: 'rgba(16, 185, 129, 0.1)' }}>Конв, %</th>
                        <th style={{ background: 'rgba(16, 185, 129, 0.1)' }}>Новые, Шт</th>
                        <th style={{ background: 'rgba(16, 185, 129, 0.1)' }}>Доля нов, %</th>
                      </tr>
                    </thead>
                    <tbody>
                      {shopKPIs.map((row, i) => (
                        <tr key={i}>
                          <td className="sticky-col font-bold">{row.store}</td>
                          {/* Итого */}
                          <td className="number">{formatCurrency(row.total.revenue)}</td>
                          <td className={`number ${row.revenueGrowth > 0 ? 'growth-up' : row.revenueGrowth < 0 ? 'growth-down' : 'dimmed'}`}>
                            {row.revenueGrowth > 0 ? '+' : ''}{formatNumber(row.revenueGrowth, 1)}%
                          </td>
                          <td className={`number ${row.revenueGrowthWeek !== undefined ? (row.revenueGrowthWeek > 0 ? 'growth-up' : row.revenueGrowthWeek < 0 ? 'growth-down' : 'dimmed') : 'dimmed'}`}>
                            {row.revenueGrowthWeek !== undefined ? (
                              <>
                                {row.revenueGrowthWeek > 0 ? '+' : ''}{formatNumber(row.revenueGrowthWeek, 1)}%
                              </>
                            ) : (
                              '—'
                            )}
                          </td>
                          {/* Секонд */}
                          <td className="number">{formatCurrency(row.second.revenue)}</td>
                          <td className="number">{formatNumber(row.second.kg, 1)}</td>
                          <td className="number">{formatCurrency(row.second.kg > 0 ? row.second.revenue / row.second.kg : 0)}</td>
                          {/* A+ */}
                          <td className="number">{formatCurrency(row.aPlus.revenue)}</td>
                          <td className="number">
                            {row.second.revenue > 0 ? formatNumber((row.aPlus.revenue / row.second.revenue) * 100, 1) : 0}%
                          </td>
                          <td className="number">{formatNumber(row.aPlus.kg, 1)}</td>
                          <td className="number">{formatCurrency(row.aPlus.kg > 0 ? row.aPlus.revenue / row.aPlus.kg : 0)}</td>
                          {/* КПБ */}
                          <td className="number">{formatCurrency(row.bedding.revenue)}</td>
                          <td className="number">
                            {row.total.revenue > 0 ? formatNumber((row.bedding.revenue / row.total.revenue) * 100, 1) : 0}%
                          </td>
                          {/* Ср. чек */}
                          <td className="number">{formatCurrency(row.total.checks > 0 ? row.total.revenue / row.total.checks : 0)}</td>
                          <td className="number">{formatCurrency(row.second.checks > 0 ? row.second.revenue / row.second.checks : 0)}</td>
                          <td className="number">{formatCurrency(row.aPlus.checks > 0 ? row.aPlus.revenue / row.aPlus.checks : 0)}</td>
                          <td className="number">{formatCurrency(row.bedding.checks > 0 ? row.bedding.revenue / row.bedding.checks : 0)}</td>
                          <td className="number highlight-red">{formatNumber(row.total.checks > 0 ? row.total.pcs / row.total.checks : 0, 1)}</td>
                          {/* Трафик - from visitors_analytics */}
                          {(() => {
                            const storeVisitors = visitorsData.filter(v => v.store === row.store).reduce((sum, v) => sum + Number(v.visitor_count), 0);
                            const checks = row.total.checks as unknown as number;
                            const conv = storeVisitors > 0 ? (checks / storeVisitors) * 100 : 0;
                            const hasData = storeVisitors > 0;
                            return (
                              <>
                                <td className={`number ${hasData ? '' : 'dimmed'}`}>{hasData ? storeVisitors.toLocaleString('ru-RU') : '—'}</td>
                                <td className={`number ${hasData ? '' : 'dimmed'}`}>{hasData ? formatNumber(conv, 1) + '%' : '—'}</td>
                                <td className="number dimmed">—</td>
                                <td className="number dimmed">—</td>
                              </>
                            );
                          })()}
                        </tr>
                      ))}
                      {/* Summary Row */}
                      <tr className="summary-row">
                        <td className="sticky-col">ИТОГО ВСЕГО</td>
                        <td className="number">{formatCurrency(shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0))}</td>
                        <td className={`number ${(() => {
                          const totalCurr = shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0);
                          const totalPast = shopKPIs.reduce((acc, r) => acc + r.totalPastRevenue, 0);
                          const totalGrowth = totalPast > 0 ? ((totalCurr / totalPast) - 1) * 100 : 0;
                          return totalGrowth > 0 ? 'growth-up' : totalGrowth < 0 ? 'growth-down' : 'dimmed';
                        })()
                          }`}>
                          {(() => {
                            const totalCurr = shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0);
                            const totalPast = shopKPIs.reduce((acc, r) => acc + r.totalPastRevenue, 0);
                            const totalGrowth = totalPast > 0 ? ((totalCurr / totalPast) - 1) * 100 : 0;
                            return (totalGrowth > 0 ? '+' : '') + formatNumber(totalGrowth, 1) + '%';
                          })()}
                        </td>
                        <td className={`number ${(() => {
                          // Calculate global weekly growth
                          const totalCurr = shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0);
                          const totalPastWeek = shopKPIs.reduce((acc, r) => acc + (r.totalPastWeekRevenue || 0), 0);

                          // Check if any shop has undefined weekly growth (meaning long period)
                          // Ideally, we check one record or the passed prop, but here checking if totalPastWeek > 0 is a proxy
                          // If period > 7 days, totalPastWeekRevenue is undefined/0 for all.
                          if (totalPastWeek === 0) return 'dimmed';

                          const growth = ((totalCurr - totalPastWeek) / totalPastWeek) * 100;
                          return growth > 0 ? 'growth-up' : growth < 0 ? 'growth-down' : 'dimmed';
                        })()
                          }`}>
                          {(() => {
                            const totalCurr = shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0);
                            const totalPastWeek = shopKPIs.reduce((acc, r) => acc + (r.totalPastWeekRevenue || 0), 0);

                            if (totalPastWeek === 0 && shopKPIs.every(r => r.revenueGrowthWeek === undefined)) return '—';

                            const growth = totalPastWeek > 0 ? ((totalCurr - totalPastWeek) / totalPastWeek) * 100 : 0;
                            return (growth > 0 ? '+' : '') + formatNumber(growth, 1) + '%';
                          })()}
                        </td>
                        <td className="number">{formatCurrency(shopKPIs.reduce((acc, r) => acc + r.second.revenue, 0))}</td>
                        <td className="number">{formatNumber(shopKPIs.reduce((acc, r) => acc + r.second.kg, 0), 1)}</td>
                        <td className="number">
                          {formatCurrency(
                            shopKPIs.reduce((acc, r) => acc + r.second.kg, 0) > 0
                              ? shopKPIs.reduce((acc, r) => acc + r.second.revenue, 0) / shopKPIs.reduce((acc, r) => acc + r.second.kg, 0)
                              : 0
                          )}
                        </td>
                        <td className="number">{formatCurrency(shopKPIs.reduce((acc, r) => acc + r.aPlus.revenue, 0))}</td>
                        <td className="number">
                          {shopKPIs.reduce((acc, r) => acc + r.second.revenue, 0) > 0
                            ? formatNumber((shopKPIs.reduce((acc, r) => acc + r.aPlus.revenue, 0) / shopKPIs.reduce((acc, r) => acc + r.second.revenue, 0)) * 100, 1)
                            : 0}%
                        </td>
                        <td className="number">{formatNumber(shopKPIs.reduce((acc, r) => acc + r.aPlus.kg, 0), 1)}</td>
                        <td className="number">
                          {formatCurrency(
                            shopKPIs.reduce((acc, r) => acc + r.aPlus.kg, 0) > 0
                              ? shopKPIs.reduce((acc, r) => acc + r.aPlus.revenue, 0) / shopKPIs.reduce((acc, r) => acc + r.aPlus.kg, 0)
                              : 0
                          )}
                        </td>
                        <td className="number">{formatCurrency(shopKPIs.reduce((acc, r) => acc + r.bedding.revenue, 0))}</td>
                        <td className="number">
                          {shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0) > 0
                            ? formatNumber((shopKPIs.reduce((acc, r) => acc + r.bedding.revenue, 0) / shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0)) * 100, 1)
                            : 0}%
                        </td>
                        <td className="number">
                          {formatCurrency(
                            shopKPIs.reduce((acc, r) => acc + (r.total.checks as unknown as number), 0) > 0
                              ? shopKPIs.reduce((acc, r) => acc + r.total.revenue, 0) / shopKPIs.reduce((acc, r) => acc + (r.total.checks as unknown as number), 0)
                              : 0
                          )}
                        </td>
                        <td className="number">
                          {formatCurrency(
                            shopKPIs.reduce((acc, r) => acc + (r.second.checks as unknown as number), 0) > 0
                              ? shopKPIs.reduce((acc, r) => acc + r.second.revenue, 0) / shopKPIs.reduce((acc, r) => acc + (r.second.checks as unknown as number), 0)
                              : 0
                          )}
                        </td>
                        <td className="number">
                          {formatCurrency(
                            shopKPIs.reduce((acc, r) => acc + (r.aPlus.checks as unknown as number), 0) > 0
                              ? shopKPIs.reduce((acc, r) => acc + r.aPlus.revenue, 0) / shopKPIs.reduce((acc, r) => acc + (r.aPlus.checks as unknown as number), 0)
                              : 0
                          )}
                        </td>
                        <td className="number">
                          {formatCurrency(
                            shopKPIs.reduce((acc, r) => acc + (r.bedding.checks as unknown as number), 0) > 0
                              ? shopKPIs.reduce((acc, r) => acc + r.bedding.revenue, 0) / shopKPIs.reduce((acc, r) => acc + (r.bedding.checks as unknown as number), 0)
                              : 0
                          )}
                        </td>
                        <td className="number highlight-red">
                          {formatNumber(
                            shopKPIs.reduce((acc, r) => acc + (r.total.checks as unknown as number), 0) > 0
                              ? shopKPIs.reduce((acc, r) => acc + r.total.pcs, 0) / shopKPIs.reduce((acc, r) => acc + (r.total.checks as unknown as number), 0)
                              : 0,
                            1
                          )}
                        </td>
                        {(() => {
                          const totalVisitors = visitorsData.reduce((sum, v) => sum + Number(v.visitor_count), 0);
                          const totalChecks = shopKPIs.reduce((acc, r) => acc + (r.total.checks as unknown as number), 0);
                          const totalConv = totalVisitors > 0 ? (totalChecks / totalVisitors) * 100 : 0;
                          const hasData = totalVisitors > 0;
                          return (
                            <>
                              <td className={`number ${hasData ? '' : 'dimmed'}`}>{hasData ? totalVisitors.toLocaleString('ru-RU') : '—'}</td>
                              <td className={`number ${hasData ? '' : 'dimmed'}`}>{hasData ? formatNumber(totalConv, 1) + '%' : '—'}</td>
                              <td className="number dimmed">—</td>
                              <td className="number dimmed">—</td>
                            </>
                          );
                        })()}
                      </tr>
                    </tbody>
                  </table>
                </div>

                {/* Legend from Excel */}
                <div className="table-legend" style={{ marginTop: 16, padding: 16, background: 'rgba(255,255,255,0.02)', borderRadius: 8, fontSize: '0.85rem', color: 'var(--text-muted)' }}>
                  <p><strong>Пояснения к колонкам:</strong></p>
                  <ul style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px 20px', listStyle: 'none', padding: 0 }}>
                    <li>• <strong>Прирост выручки:</strong> к такому же прошедшему периоду в %</li>
                    <li>• <strong>Выручка СЭКОНД:</strong> только весовой товар без нового</li>
                    <li>• <strong>Категория "А+":</strong> товары с меткой люкс/экстра в названии</li>
                    <li>• <strong>Выручка КПБ:</strong> продажи нового постельного белья</li>
                    <li>• <strong>Среднее кол-во товаров:</strong> общее кол-во штук / количество чеков</li>
                    <li>• <strong>Трафик:</strong> количество зашедших покупателей (в разработке)</li>
                    <li>• <strong>Конверсия:</strong> отношение чеков к трафику</li>
                  </ul>
                </div>
              </div>

              {/* ===== TURNOVER / ОБОРАЧИВАЕМОСТЬ ===== */}
              {(() => {
                // Exclude non-retail warehouses
                const excludeStores = ['Основной склад'];

                // Calculate stock per store (only weighted items in KG from inventory_analytics)
                const stockByStore = new Map<string, number>();
                inventoryData.forEach(item => {
                  if (excludeStores.includes(item.store)) return;
                  if (item.unit !== 'кг') return; // Only use KG items for turnover

                  // Apply global filters
                  if (selectedStores.length > 0 && !selectedStores.includes(item.store)) return;
                  if (selectedGroups.length > 0 && item.product_group && !selectedGroups.includes(item.product_group)) return;
                  if (selectedProducts.length > 0 && !selectedProducts.includes(item.product)) return;

                  stockByStore.set(item.store, (stockByStore.get(item.store) || 0) + item.quantity);
                });

                // Calculate total kg sold per store in selected period
                const salesKgByStore = new Map<string, number>();
                salesData.forEach(record => {
                  if (excludeStores.includes(record.store)) return;
                  const estimatedKg = calculateEstimatedWeight(record, productWeights);
                  const rowKg = estimatedKg > 0 ? estimatedKg : Number(record.quantity_kg);
                  salesKgByStore.set(record.store, (salesKgByStore.get(record.store) || 0) + rowKg);
                });

                // Calculate number of days in period
                const start = new Date(startDate);
                const end = new Date(endDate);
                const periodDays = Math.max(1, Math.ceil((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24)) + 1);

                // Build turnover data per store
                type TurnoverRow = {
                  store: string;
                  stockQty: number;
                  totalSalesKg: number;
                  dailySalesKg: number;
                  daysToSell: number;
                  status: 'excellent' | 'good' | 'warning' | 'critical';
                };

                const getTurnoverStatus = (days: number): TurnoverRow['status'] => {
                  if (days <= 30) return 'excellent';
                  if (days <= 60) return 'good';
                  if (days <= 90) return 'warning';
                  return 'critical';
                };

                const getStatusLabel = (status: string) => {
                  switch (status) {
                    case 'excellent': return '✅ Быстрая';
                    case 'good': return '🟡 Норма';
                    case 'warning': return '🟠 Медленная';
                    case 'critical': return '🔴 Застой';
                    default: return '';
                  }
                };

                const turnoverData: TurnoverRow[] = [];
                const allStores = new Set([...stockByStore.keys(), ...salesKgByStore.keys()]);

                allStores.forEach(store => {
                  const stockQty = stockByStore.get(store) || 0;
                  const totalSalesKg = salesKgByStore.get(store) || 0;
                  const dailySalesKg = totalSalesKg / periodDays;
                  const daysToSell = dailySalesKg > 0 ? stockQty / dailySalesKg : 999;

                  turnoverData.push({
                    store,
                    stockQty,
                    totalSalesKg,
                    dailySalesKg,
                    daysToSell: Math.round(daysToSell),
                    status: getTurnoverStatus(daysToSell)
                  });
                });

                // Sort: critical first, then by days descending
                turnoverData.sort((a, b) => b.daysToSell - a.daysToSell);

                // Summary counts
                const statusCounts = { excellent: 0, good: 0, warning: 0, critical: 0 };
                turnoverData.forEach(r => statusCounts[r.status]++);

                const totalStockAll = turnoverData.reduce((s, r) => s + r.stockQty, 0);
                const totalDailySales = turnoverData.reduce((s, r) => s + r.dailySalesKg, 0);
                const avgDaysAll = totalDailySales > 0 ? Math.round(totalStockAll / totalDailySales) : 0;

                return (
                  <div className="turnover-section">
                    <div className="turnover-header">
                      <h3>
                        📦 Оборачиваемость товара
                        <span className="period-badge">{periodDays} дн. • Ср. {avgDaysAll} дн.</span>
                      </h3>
                    </div>

                    {/* Summary */}
                    <div className="turnover-summary">
                      <div className="turnover-summary-item">
                        <span className="summary-dot excellent"></span>
                        <span className="summary-label">≤30 дн.</span>
                        <span className="summary-value">{statusCounts.excellent}</span>
                      </div>
                      <div className="turnover-summary-item">
                        <span className="summary-dot good"></span>
                        <span className="summary-label">31–60 дн.</span>
                        <span className="summary-value">{statusCounts.good}</span>
                      </div>
                      <div className="turnover-summary-item">
                        <span className="summary-dot warning"></span>
                        <span className="summary-label">61–90 дн.</span>
                        <span className="summary-value">{statusCounts.warning}</span>
                      </div>
                      <div className="turnover-summary-item">
                        <span className="summary-dot critical"></span>
                        <span className="summary-label">&gt;90 дн.</span>
                        <span className="summary-value">{statusCounts.critical}</span>
                      </div>
                      <div className="turnover-summary-item" style={{ marginLeft: 'auto' }}>
                        <span className="summary-label">Остаток всего</span>
                        <span className="summary-value" style={{ color: 'var(--accent-blue)' }}>{formatNumber(totalStockAll, 1)} кг</span>
                      </div>
                      <div className="turnover-summary-item">
                        <span className="summary-label">Продажи/день</span>
                        <span className="summary-value" style={{ color: 'var(--accent-purple)' }}>{formatNumber(totalDailySales, 1)} кг</span>
                      </div>
                    </div>

                    {turnoverData.length > 0 ? (
                      <div className="turnover-grid">
                        {turnoverData.map((row, i) => {
                          const progressPct = Math.min(100, (row.daysToSell / 120) * 100);
                          return (
                            <div className="turnover-card" key={i}>
                              <div className="store-name">
                                <span className="store-icon">🏬</span>
                                {row.store}
                              </div>

                              <div className="turnover-days-block">
                                <div>
                                  <div className={`turnover-days-value ${row.status}`}>
                                    {row.daysToSell > 900 ? '∞' : row.daysToSell}
                                  </div>
                                  <div className="turnover-days-label">дней до продажи</div>
                                </div>
                                <span className={`turnover-status-badge ${row.status}`}>
                                  {getStatusLabel(row.status)}
                                </span>
                              </div>

                              <div className="turnover-progress-track">
                                <div
                                  className={`turnover-progress-fill ${row.status}`}
                                  style={{ width: `${progressPct}%` }}
                                />
                              </div>

                              <div className="turnover-metrics">
                                <div className="turnover-metric">
                                  <div className="metric-label">Остаток, кг</div>
                                  <div className="metric-value stock">{formatNumber(row.stockQty, 1)}</div>
                                </div>
                                <div className="turnover-metric">
                                  <div className="metric-label">Прод./день</div>
                                  <div className="metric-value daily-sales">{formatNumber(row.dailySalesKg, 1)}</div>
                                </div>
                                <div className="turnover-metric">
                                  <div className="metric-label">Всего кг</div>
                                  <div className="metric-value">{formatNumber(row.totalSalesKg, 1)}</div>
                                </div>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <div className="no-turnover-data">
                        Нет данных по остаткам. Запустите синхронизацию инвентаря.
                      </div>
                    )}

                    <div className="table-legend" style={{ marginTop: 16, padding: 12, background: 'rgba(255,255,255,0.02)', borderRadius: 8, fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                      <p><strong>Формула:</strong> Дней до продажи = Остаток (кг) ÷ Средние продажи в день (кг). Остаток — текущий снапшот из 1С. Продажи — за выбранный период фильтров.</p>
                    </div>
                  </div>
                );
              })()}

              {/* Charts (Keep charts as is) */}
              <div className="charts-grid">
                <div className="chart-card daily-dynamics" style={{ gridColumn: '1 / -1' }}>
                  <h3>📉 Динамика выручки и цены сэконда (по дням)</h3>
                  <div className="chart-container" style={{ height: '400px' }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={dailyData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                        <XAxis dataKey="date" stroke="#6c6c7c" />
                        <YAxis
                          yAxisId="left"
                          stroke="#00d4ff"
                          tickFormatter={v => `${(v / 1000).toFixed(0)}К`}
                          label={{ value: 'Выручка (₽)', angle: -90, position: 'insideLeft', fill: '#00d4ff', offset: 10 }}
                        />
                        <YAxis
                          yAxisId="right"
                          orientation="right"
                          stroke="#eab308"
                          label={{ value: 'Цена сэконда (₽/кг)', angle: 90, position: 'insideRight', fill: '#eab308', offset: 10 }}
                        />
                        <Tooltip
                          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.1)', color: '#fff' }}
                          formatter={(value: any, name: string | undefined) => {
                            if (name === 'Выручка') return [formatCurrency(value), name];
                            return [`${value} ₽/кг`, name || ''];
                          }}
                        />
                        <Legend />
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey="revenue"
                          name="Выручка"
                          stroke="#00d4ff"
                          strokeWidth={3}
                          dot={{ r: 4, fill: '#00d4ff' }}
                          activeDot={{ r: 6 }}
                        />
                        <Line
                          yAxisId="right"
                          type="monotone"
                          dataKey="avgPriceSecond"
                          name="Цена сэконда"
                          stroke="#eab308"
                          strokeWidth={3}
                          dot={{ r: 4, fill: '#eab308' }}
                          activeDot={{ r: 6 }}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="chart-card">
                  <h3>🏪 Доля выручки по магазинам</h3>
                  <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie
                          data={pieData}
                          cx="50%"
                          cy="50%"
                          innerRadius={60}
                          outerRadius={100}
                          paddingAngle={2}
                          dataKey="value"
                        >
                          {pieData.map((_, index) => (
                            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                          ))}
                        </Pie>
                        <Tooltip
                          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.1)' }}
                          formatter={(value: number | undefined) => [formatCurrency(value), 'Выручка']}
                        />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="chart-card" style={rowDimension === 'product_group' ? { gridColumn: '1 / -1' } : {}}>
                  <h3>📊 {rowDimension === 'product_group' ? 'Выручка по товарным группам' : 'Топ магазинов по выручке'}</h3>
                  <div className="chart-container" style={rowDimension === 'product_group' ? { height: '600px' } : {}}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={rowDimension === 'product_group' ? aggregatedData : aggregatedData.slice(0, 10)} layout="vertical">
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                        <XAxis type="number" stroke="#6c6c7c" tickFormatter={v => `${(v / 1000000).toFixed(1)}М`} />
                        <YAxis dataKey="name" type="category" stroke="#6c6c7c" width={rowDimension === 'product_group' ? 150 : 100} interval={0} />
                        <Tooltip
                          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.1)' }}
                          formatter={(value: number | undefined) => [formatCurrency(value), 'Выручка']}
                        />
                        <Bar dataKey="revenue" fill="#22c55e" radius={[0, 4, 4, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>



              {/* Data Table with Stock Column */}
              <div className="table-section">
                <div className="table-header">
                  <h3>📋 Сводная таблица</h3>
                  <div className="pivot-controls">
                    <select value={rowDimension} onChange={e => setRowDimension(e.target.value)}>
                      <option value="store">По магазинам</option>
                      <option value="product_group">По товарным группам</option>
                      <option value="product">По номенклатуре</option>
                      <option value="weekday">По дням недели</option>
                      <option value="month">По месяцам</option>
                    </select>
                  </div>
                </div>
                <div className="data-table-wrapper">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Название</th>
                        <th className="sortable number" onClick={() => handleSort('revenue')}>
                          Выручка <ArrowUpDown size={14} />
                        </th>
                        {(rowDimension === 'store' || rowDimension === 'product') && (
                          <th className="sortable number" onClick={() => handleSort('stock')}>
                            Остаток (Склад) <ArrowUpDown size={14} />
                          </th>
                        )}
                        <th className="sortable number" onClick={() => handleSort('kg')}>
                          Кг <ArrowUpDown size={14} />
                        </th>
                        <th className="sortable number" onClick={() => handleSort('pcs')}>
                          Шт <ArrowUpDown size={14} />
                        </th>
                        <th className="sortable number" onClick={() => handleSort('transactions')}>
                          Транзакций <ArrowUpDown size={14} />
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {aggregatedData.map((row, index) => (
                        <tr key={index}>
                          <td>{row.name}</td>
                          <td className="number revenue">{formatCurrency(row.revenue)}</td>
                          {(rowDimension === 'store' || rowDimension === 'product') && (
                            <td className="number" style={{ color: '#eab308' }}>
                              {row.stock ? formatNumber(row.stock, 0) : '-'}
                            </td>
                          )}
                          <td className="number kg">{formatNumber(row.kg, 2)} кг</td>
                          <td className="number pcs">{formatNumber(row.pcs)} шт</td>
                          <td className="number">{formatNumber(row.transactions)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          ) : (
            <div className="table-section">
              <div className="table-header">
                <h3>📦 Остатки на складе ({filteredInventory.length})</h3>
              </div>
              <div className="data-table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Магазин</th>
                      <th>Группа товара</th>
                      <th>Номенклатура</th>
                      <th className="number">Остаток</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredInventory.slice(0, 500).map((item, index) => (
                      <tr key={index}>
                        <td>{item.store}</td>
                        <td>{item.product_group || '-'}</td>
                        <td>{item.product}</td>
                        <td className="number" style={{ color: '#eab308', fontWeight: 'bold' }}>
                          {formatNumber(item.quantity, 0)}
                        </td>
                      </tr>
                    ))}
                    {filteredInventory.length === 0 && (
                      <tr>
                        <td colSpan={4} style={{ textAlign: 'center', padding: '20px' }}>Нет данных по выбранным фильтрам</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
