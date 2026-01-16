import { useState, useEffect, useMemo } from 'react';
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  Area, AreaChart
} from 'recharts';
import {
  TrendingUp, Package, Weight, ShoppingCart, Receipt,
  Calendar, Store, Filter, ArrowUpDown, RefreshCw
} from 'lucide-react';
import { fetchSalesData, fetchDistinctValues, fetchKPIs, fetchInventory, type SalesRecord, type InventoryRecord } from './lib/supabase';
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

export default function App() {
  // Date state
  const [startDate, setStartDate] = useState('2025-09-01');
  const [endDate, setEndDate] = useState('2026-01-15');

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
  const [loading, setLoading] = useState(true);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  // Pivot state
  const [rowDimension, setRowDimension] = useState('store');
  const [sortColumn, setSortColumn] = useState('revenue');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');

  // Load initial data
  useEffect(() => {
    async function loadFilters() {
      const [storeList, groupList, prodList] = await Promise.all([
        fetchDistinctValues('store'),
        fetchDistinctValues('product_group'),
        fetchDistinctValues('product')
      ]);
      setStores(storeList);
      setProductGroups(groupList);
      setProductsList(prodList);
    }
    loadFilters();
  }, []);

  // Load sales and inventory data
  const loadData = async () => {
    setLoading(true);
    const [data, kpiData, inventory] = await Promise.all([
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
      fetchInventory()
    ]);
    setSalesData(data);
    setKpis(kpiData);
    setInventoryData(inventory);
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

      const existing = grouped.get(key) || { revenue: 0, kg: 0, pcs: 0, count: 0, stock: 0 };
      grouped.set(key, {
        ...existing,
        revenue: existing.revenue + Number(record.revenue),
        kg: existing.kg + Number(record.quantity_kg),
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
      grouped.set(monthKey, {
        revenue: existing.revenue + Number(record.revenue),
        kg: existing.kg + Number(record.quantity_kg),
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
  }, [salesData]);

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
      <div className="filters-bar">
        {activeTab === 'dashboard' && (
          <>
            <div className="filter-group">
              <label><Calendar size={12} /> Начало</label>
              <input
                type="date"
                value={startDate}
                onChange={e => setStartDate(e.target.value)}
              />
            </div>
            <div className="filter-group">
              <label><Calendar size={12} /> Конец</label>
              <input
                type="date"
                value={endDate}
                onChange={e => setEndDate(e.target.value)}
              />
            </div>
          </>
        )}
        <div className="filter-group">
          <label><Store size={12} /> Магазин</label>
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
          <label><Package size={12} /> Номенклатура</label>
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
          <Filter size={16} style={{ marginRight: 8 }} />
          Применить
        </button>
      </div>

      {loading ? (
        <div className="loading">
          <div className="loading-spinner"></div>
          Загрузка данных...
        </div>
      ) : (
        <>
          {/* KPI Cards */}
          <div className="kpi-grid">
            <div className="kpi-card blue">
              <div className="kpi-icon"><TrendingUp size={24} /></div>
              <div className="kpi-label">Выручка</div>
              <div className="kpi-value">{formatCurrency(kpis?.totalRevenue || 0)}</div>
            </div>
            <div className="kpi-card green">
              <div className="kpi-icon"><Weight size={24} /></div>
              <div className="kpi-label">Продано (кг)</div>
              <div className="kpi-value">{formatNumber(kpis?.totalKg || 0, 0)} кг</div>
            </div>
            <div className="kpi-card purple">
              <div className="kpi-icon"><Package size={24} /></div>
              <div className="kpi-label">Продано (шт)</div>
              <div className="kpi-value">{formatNumber(kpis?.totalPcs || 0)} шт</div>
            </div>
            <div className="kpi-card pink">
              <div className="kpi-icon"><ShoppingCart size={24} /></div>
              <div className="kpi-label">Чеков</div>
              <div className="kpi-value">{formatNumber(kpis?.uniqueChecks || 0)}</div>
            </div>
            <div className="kpi-card orange">
              <div className="kpi-icon"><Receipt size={24} /></div>
              <div className="kpi-label">Средний чек</div>
              <div className="kpi-value">{formatCurrency(kpis?.avgCheck || 0)}</div>
            </div>
          </div>

          {/* Charts (Keep charts as is) */}
          <div className="charts-grid">
            <div className="chart-card">
              <h3>📈 Выручка по месяцам</h3>
              <div className="chart-container">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={monthlyData}>
                    <defs>
                      <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#00d4ff" stopOpacity={0.3} />
                        <stop offset="95%" stopColor="#00d4ff" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                    <XAxis dataKey="month" stroke="#6c6c7c" />
                    <YAxis stroke="#6c6c7c" tickFormatter={v => `${(v / 1000000).toFixed(1)}М`} />
                    <Tooltip
                      contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.1)' }}
                      formatter={(value: number | undefined) => [formatCurrency(value), 'Выручка']}
                    />
                    <Area
                      type="monotone"
                      dataKey="revenue"
                      stroke="#00d4ff"
                      strokeWidth={3}
                      fill="url(#colorRevenue)"
                    />
                  </AreaChart>
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

            <div className="chart-card">
              <h3>📦 Продажи в кг по месяцам</h3>
              <div className="chart-container">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={monthlyData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                    <XAxis dataKey="month" stroke="#6c6c7c" />
                    <YAxis stroke="#6c6c7c" />
                    <Tooltip
                      contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.1)' }}
                      formatter={(value: number | undefined) => [formatNumber(value, 0) + ' кг', 'Количество']}
                    />
                    <Bar dataKey="kg" fill="#a855f7" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="chart-card">
              <h3>📊 Топ магазинов по выручке</h3>
              <div className="chart-container">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={aggregatedData.slice(0, 10)} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                    <XAxis type="number" stroke="#6c6c7c" tickFormatter={v => `${(v / 1000000).toFixed(1)}М`} />
                    <YAxis dataKey="name" type="category" stroke="#6c6c7c" width={100} />
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
                      <td className="number kg">{formatNumber(row.kg, 0)} кг</td>
                      <td className="number pcs">{formatNumber(row.pcs)} шт</td>
                      <td className="number">{formatNumber(row.transactions)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
