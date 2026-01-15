#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
LiderTeks Sales Daily Groups Pipeline
═══════════════════════════════════════════════════════════════════════════════
Data Pipeline: 1C:Retail 3.0 (MS SQL) → Excel Report

Author: Antigravity
Date: 2026-01-15
Output: LeaderTex_Sales_Dec2025.xlsx
═══════════════════════════════════════════════════════════════════════════════
"""

import pymssql
import pandas as pd
import numpy as np
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import sys

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

# Column mappings (verified)
WAREHOUSE_REF = '_Fld53725RRef'  # Warehouse reference
NOMENCLATURE_REF = '_Fld53716RRef'  # Nomenclature reference (CORRECT!)
REVENUE_COL = '_Fld53732'  # Revenue
QUANTITY_COL = '_Fld53731'  # Quantity
RECORDER_REF = '_RecorderRRef'  # Document reference (for check count)

# Date offset for 1C
DATE_OFFSET_YEARS = 2000

# Validation constants
VALIDATION_STORE = 'Большевиков'
VALIDATION_REVENUE = 776661.00
VALIDATION_TOLERANCE = 0.01  # 1%

# Output file
OUTPUT_FILE = 'LeaderTex_Sales_Dec2025.xlsx'

# Period
START_DATE_1C = '4025-12-01'
END_DATE_1C = '4026-01-01'
REPORT_MONTH = 'Декабрь 2025'

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

def get_connection():
    """Establish connection to MS SQL database."""
    return pymssql.connect(**DB_CONFIG)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

def extract_sales_data(cursor) -> pd.DataFrame:
    """
    Extract raw sales data with all required dimensions.
    
    Returns DataFrame with columns:
    - sale_date: Date (with -2000 years offset applied)
    - warehouse: Warehouse name
    - store: Store name (parent of warehouse)
    - product: Product name
    - product_group: Product group (extracted from product name)
    - quantity: Quantity sold
    - revenue: Revenue amount
    - recorder: Document reference (for check counting)
    """
    log.info("Extracting sales data from database...")
    
    query = f"""
    SELECT 
        s._Period AS sale_date_1c,
        w._Description AS warehouse,
        m._Description AS store,
        n._Description AS product,
        u._Description AS unit,
        s.{QUANTITY_COL} AS quantity,
        s.{REVENUE_COL} AS revenue,
        s.{RECORDER_REF} AS recorder
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF} = w._IDRRef
    LEFT JOIN _Reference640 m ON w._ParentIDRRef = m._IDRRef
    LEFT JOIN _Reference387 n ON s.{NOMENCLATURE_REF} = n._IDRRef
    LEFT JOIN _Reference188 u ON n._Fld9817RRef = u._IDRRef
    WHERE s._Period >= %s AND s._Period < %s
    """
    
    cursor.execute(query, (START_DATE_1C, END_DATE_1C))
    rows = cursor.fetchall()
    
    log.info(f"Fetched {len(rows):,} raw records")
    
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=[
        'sale_date_1c', 'warehouse', 'store', 'product', 'unit',
        'quantity', 'revenue', 'recorder'
    ])
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# DATA TRANSFORMATION
# ═══════════════════════════════════════════════════════════════════════════════

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply business logic transformations:
    - Convert dates (subtract 2000 years)
    - Extract product groups
    - Calculate date dimensions (week, month, quarter, year)
    - Handle unit types (pcs vs kg)
    """
    log.info("Transforming data...")
    
    # 1. Convert dates (subtract 2000 years)
    # Handle 1C dates manually since they're in year 4025
    def convert_1c_date(dt_1c):
        if pd.isna(dt_1c):
            return None
        try:
            # Convert to string and parse
            dt_str = str(dt_1c)
            # Extract year and subtract 2000
            if len(dt_str) >= 4:
                year_1c = int(dt_str[:4])
                year_real = year_1c - DATE_OFFSET_YEARS
                # Reconstruct datetime
                new_dt_str = str(year_real) + dt_str[4:]
                return pd.to_datetime(new_dt_str)
        except:
            pass
        return None
    
    df['sale_date'] = df['sale_date_1c'].apply(convert_1c_date)
    
    # 2. Extract date dimensions
    df['date'] = df['sale_date'].dt.date
    df['day'] = df['sale_date'].dt.day
    df['week'] = df['sale_date'].dt.isocalendar().week.astype(int)
    df['month'] = df['sale_date'].dt.month
    df['quarter'] = df['sale_date'].dt.quarter
    df['year'] = df['sale_date'].dt.year
    df['weekday'] = df['sale_date'].dt.day_name()
    
    # 3. Handle missing stores (use warehouse name)
    df['store'] = df['store'].fillna(df['warehouse'])
    
    # 4. Extract product group from product name
    # Pattern: "Group.Season" or "Group Season" or just "Group"
    def extract_group(product_name):
        if pd.isna(product_name):
            return 'Без группы'
        
        name = str(product_name).strip()
        
        # Common patterns
        patterns = [
            'Аксессуары', 'Брюки', 'Дети', 'Джемпер', 'Куртки', 
            'Обувь', 'Платье', 'Рубашки', 'Сопутка', 'Спорт',
            'Текстиль', 'Трикотаж', 'АКЦИЯ', 'Наволочка', 'Пододеяльник',
            'Простыня', 'Полотенце'
        ]
        
        for pattern in patterns:
            if pattern.lower() in name.lower():
                # Return full category including season if present
                if '.' in name:
                    parts = name.split('.')
                    return parts[0].strip()
                elif ' ' in name:
                    # Check for known season suffixes
                    for season in ['Зима', 'Лето', 'Всесезон']:
                        if season in name:
                            idx = name.find(season)
                            return name[:idx].strip().rstrip('.')
                return pattern
        
        # If no pattern matches, use first word or full name
        if len(name) > 30:
            return name.split()[0] if ' ' in name else name[:30]
        return name
    
    df['product_group'] = df['product'].apply(extract_group)
    
    # 5. Convert numeric columns
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
    
    # 6. Determine unit type (pcs vs kg) based on ACTUAL unit from database
    def get_unit_type(unit):
        if pd.isna(unit):
            return 'pcs'  # Default to pcs if no unit specified
        unit_str = str(unit).lower().strip()
        if 'кг' in unit_str or 'kg' in unit_str:
            return 'kg'
        return 'pcs'
    
    df['unit_type'] = df['unit'].apply(get_unit_type)
    
    # Log unit distribution
    unit_counts = df['unit_type'].value_counts()
    log.info(f"Unit distribution: kg={unit_counts.get('kg', 0):,}, pcs={unit_counts.get('pcs', 0):,}")
    
    # 7. Split quantity into pcs and kg
    # VERIFIED: All distinct quantities in DB are integers. Quantity is ALWAYS pieces.
    # Unit 'kg' just implies 'priced by weight items' or 'category'.
    df['quantity_pcs'] = df['quantity']
    
    # Keep kg separate if needed for specific weight reporting, but it implies count of weight-items
    df['quantity_kg'] = df.apply(
        lambda x: x['quantity'] if x['unit_type'] == 'kg' else 0, axis=1
    )
    
    log.info(f"Transformation complete. Records: {len(df):,}")
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# AGGREGATION
# ═══════════════════════════════════════════════════════════════════════════════

def aggregate_data(df: pd.DataFrame) -> dict:
    """
    Aggregate data into various views for reporting.
    
    Returns dict with DataFrames:
    - daily_groups: Date × Store × ProductGroup
    - by_store: Store-level aggregation
    - by_group: Product group aggregation
    - by_store_group: Store × ProductGroup
    """
    log.info("Aggregating data...")
    
    results = {}
    
    # 1. Daily Groups (main granularity)
    daily_groups = df.groupby(['date', 'store', 'product_group']).agg({
        'revenue': 'sum',
        'quantity_pcs': 'sum',
        'quantity_kg': 'sum',
        'recorder': 'nunique'  # Unique checks
    }).reset_index()
    
    daily_groups.columns = ['date', 'store', 'product_group', 'revenue', 
                           'quantity_pcs', 'quantity_kg', 'checks']
    
    # Calculate average check (handle division by zero)
    daily_groups['avg_check'] = daily_groups.apply(
        lambda x: x['revenue'] / x['checks'] if x['checks'] > 0 else 0, axis=1
    )
    
    results['daily_groups'] = daily_groups
    
    # 2. By Store
    by_store = df.groupby('store').agg({
        'revenue': 'sum',
        'quantity_pcs': 'sum',
        'quantity_kg': 'sum',
        'recorder': 'nunique'
    }).reset_index()
    
    by_store.columns = ['store', 'revenue', 'quantity_pcs', 'quantity_kg', 'checks']
    by_store['avg_check'] = by_store.apply(
        lambda x: x['revenue'] / x['checks'] if x['checks'] > 0 else 0, axis=1
    )
    by_store = by_store.sort_values('revenue', ascending=False)
    
    results['by_store'] = by_store
    
    # 3. By Product Group
    by_group = df.groupby('product_group').agg({
        'revenue': 'sum',
        'quantity_pcs': 'sum',
        'quantity_kg': 'sum',
        'recorder': 'nunique'
    }).reset_index()
    
    by_group.columns = ['product_group', 'revenue', 'quantity_pcs', 'quantity_kg', 'checks']
    
    # Calculate share
    total_revenue = by_group['revenue'].sum()
    by_group['share_pct'] = (by_group['revenue'] / total_revenue * 100).round(2)
    by_group = by_group.sort_values('revenue', ascending=False)
    
    results['by_group'] = by_group
    
    # 4. Store × Group
    by_store_group = df.groupby(['store', 'product_group']).agg({
        'revenue': 'sum',
        'quantity_pcs': 'sum',
        'quantity_kg': 'sum'
    }).reset_index()
    
    by_store_group.columns = ['store', 'product_group', 'revenue', 'quantity_pcs', 'quantity_kg']
    by_store_group = by_store_group.sort_values(['store', 'revenue'], ascending=[True, False])
    
    results['by_store_group'] = by_store_group
    
    log.info(f"Aggregation complete. Store count: {len(by_store)}, Group count: {len(by_group)}")
    
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_data(aggregated: dict) -> bool:
    """
    Validate data against known benchmarks.
    Raises exception if validation fails.
    """
    log.info("Validating data against 1C benchmark...")
    
    by_store = aggregated['by_store']
    
    # Find Bolshevikov store
    bolsh = by_store[by_store['store'].str.contains(VALIDATION_STORE, case=False, na=False)]
    
    if bolsh.empty:
        log.error(f"Validation store '{VALIDATION_STORE}' not found!")
        raise ValueError(f"Validation failed: Store '{VALIDATION_STORE}' not found in data")
    
    actual_revenue = bolsh['revenue'].sum()
    diff = abs(actual_revenue - VALIDATION_REVENUE)
    diff_pct = diff / VALIDATION_REVENUE * 100
    
    log.info(f"Validation: {VALIDATION_STORE}")
    log.info(f"  Expected: {VALIDATION_REVENUE:,.2f} ₽")
    log.info(f"  Actual:   {actual_revenue:,.2f} ₽")
    log.info(f"  Diff:     {diff:,.2f} ₽ ({diff_pct:.4f}%)")
    
    if diff_pct > VALIDATION_TOLERANCE * 100:
        log.error(f"VALIDATION FAILED! Difference exceeds {VALIDATION_TOLERANCE*100}% tolerance")
        raise ValueError(
            f"Validation failed: Revenue for '{VALIDATION_STORE}' is {actual_revenue:,.2f} ₽, "
            f"expected {VALIDATION_REVENUE:,.2f} ₽ (diff: {diff_pct:.2f}%)"
        )
    
    log.info("✅ Validation PASSED!")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL REPORT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def generate_excel_report(aggregated: dict, raw_df: pd.DataFrame):
    """
    Generate Excel report with multiple sheets:
    - Dashboard: Summary KPIs
    - By_Groups: Product group breakdown with share %
    - By_Stores: Store breakdown
    - Top_Bottom: Top 10 and Bottom 10 groups
    """
    log.info(f"Generating Excel report: {OUTPUT_FILE}")
    
    with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # ═══════════════════════════════════════════════════════════════════
        # FORMATS
        # ═══════════════════════════════════════════════════════════════════
        header_fmt = workbook.add_format({
            'bold': True, 'bg_color': '#1F4E79', 'font_color': 'white',
            'border': 1, 'align': 'center', 'valign': 'vcenter'
        })
        money_fmt = workbook.add_format({'num_format': '#,##0.00 ₽', 'border': 1})
        number_fmt = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
        percent_fmt = workbook.add_format({'num_format': '0.00%', 'border': 1})
        title_fmt = workbook.add_format({
            'bold': True, 'font_size': 16, 'font_color': '#1F4E79'
        })
        kpi_label_fmt = workbook.add_format({
            'bold': True, 'font_size': 12, 'bg_color': '#D9E2F3', 'border': 1
        })
        kpi_value_fmt = workbook.add_format({
            'bold': True, 'font_size': 14, 'num_format': '#,##0.00',
            'bg_color': '#E2EFDA', 'border': 1
        })
        
        # ═══════════════════════════════════════════════════════════════════
        # SHEET 1: Dashboard
        # ═══════════════════════════════════════════════════════════════════
        ws = workbook.add_worksheet('Dashboard')
        
        # Calculate summary KPIs
        total_revenue = raw_df['revenue'].sum()
        total_qty_pcs = raw_df['quantity_pcs'].sum()
        total_qty_kg = raw_df['quantity_kg'].sum()
        total_checks = raw_df['recorder'].nunique()
        avg_check = total_revenue / total_checks if total_checks > 0 else 0
        store_count = raw_df['store'].nunique()
        group_count = raw_df['product_group'].nunique()
        
        # Title
        ws.write(0, 0, f'LiderTeks - Sales Dashboard - {REPORT_MONTH}', title_fmt)
        ws.merge_range('A1:D1', f'LiderTeks - Sales Dashboard - {REPORT_MONTH}', title_fmt)
        
        # KPI Cards
        kpis = [
            ('Выручка, ₽', f'{total_revenue:,.2f}'),
            ('Чеки', f'{total_checks:,}'),
            ('Средний чек, ₽', f'{avg_check:,.2f}'),
            ('Количество (шт)', f'{total_qty_pcs:,.0f}'),
            ('Количество (кг)', f'{total_qty_kg:,.2f}'),
            ('Магазинов', f'{store_count}'),
            ('Товарных групп', f'{group_count}'),
        ]
        
        for i, (label, value) in enumerate(kpis):
            ws.write(3 + i*2, 0, label, kpi_label_fmt)
            ws.write(3 + i*2, 1, value, kpi_value_fmt)
        
        ws.set_column('A:A', 25)
        ws.set_column('B:B', 20)
        
        # ═══════════════════════════════════════════════════════════════════
        # SHEET 2: By_Groups
        # ═══════════════════════════════════════════════════════════════════
        by_group = aggregated['by_group'].copy()
        by_group['share_pct'] = by_group['share_pct'] / 100  # Convert to decimal for Excel
        
        by_group.to_excel(writer, sheet_name='By_Groups', index=False, startrow=1)
        
        ws2 = writer.sheets['By_Groups']
        ws2.write(0, 0, f'Продажи по товарным группам - {REPORT_MONTH}', title_fmt)
        
        # Format columns
        ws2.set_column('A:A', 35)
        ws2.set_column('B:B', 18)
        ws2.set_column('C:C', 15)
        ws2.set_column('D:D', 15)
        ws2.set_column('E:E', 12)
        ws2.set_column('F:F', 12)
        
        # ═══════════════════════════════════════════════════════════════════
        # SHEET 3: By_Stores
        # ═══════════════════════════════════════════════════════════════════
        by_store = aggregated['by_store'].copy()
        
        by_store.to_excel(writer, sheet_name='By_Stores', index=False, startrow=1)
        
        ws3 = writer.sheets['By_Stores']
        ws3.write(0, 0, f'Продажи по магазинам - {REPORT_MONTH}', title_fmt)
        
        ws3.set_column('A:A', 40)
        ws3.set_column('B:B', 18)
        ws3.set_column('C:C', 15)
        ws3.set_column('D:D', 15)
        ws3.set_column('E:E', 12)
        ws3.set_column('F:F', 15)
        
        # ═══════════════════════════════════════════════════════════════════
        # SHEET 4: Top_Bottom
        # ═══════════════════════════════════════════════════════════════════
        by_group_sorted = aggregated['by_group'].copy()
        
        top_10 = by_group_sorted.head(10).copy()
        top_10['rank'] = range(1, len(top_10) + 1)
        top_10 = top_10[['rank', 'product_group', 'revenue', 'quantity_pcs', 'share_pct']]
        
        bottom_10 = by_group_sorted[by_group_sorted['revenue'] > 0].tail(10).copy()
        bottom_10['rank'] = range(len(by_group_sorted) - len(bottom_10) + 1, len(by_group_sorted) + 1)
        bottom_10 = bottom_10[['rank', 'product_group', 'revenue', 'quantity_pcs', 'share_pct']]
        
        # Write Top 10
        ws4 = workbook.add_worksheet('Top_Bottom')
        ws4.write(0, 0, 'ТОП-10 товарных групп по выручке', title_fmt)
        top_10.to_excel(writer, sheet_name='Top_Bottom', index=False, startrow=2)
        
        # Write Bottom 10
        start_row = len(top_10) + 5
        ws4.write(start_row, 0, 'Антитоп-10 (минимальные продажи)', title_fmt)
        bottom_10.to_excel(writer, sheet_name='Top_Bottom', index=False, startrow=start_row + 2)
        
        ws4.set_column('A:A', 8)
        ws4.set_column('B:B', 35)
        ws4.set_column('C:C', 18)
        ws4.set_column('D:D', 15)
        ws4.set_column('E:E', 12)
        
        # ═══════════════════════════════════════════════════════════════════
        # SHEET 5: Store_Groups (detailed)
        # ═══════════════════════════════════════════════════════════════════
        by_store_group = aggregated['by_store_group'].copy()
        by_store_group.to_excel(writer, sheet_name='Store_Groups', index=False, startrow=1)
        
        ws5 = writer.sheets['Store_Groups']
        ws5.write(0, 0, f'Продажи по магазинам и группам - {REPORT_MONTH}', title_fmt)
        
        ws5.set_column('A:A', 35)
        ws5.set_column('B:B', 35)
        ws5.set_column('C:C', 18)
        ws5.set_column('D:D', 15)
        ws5.set_column('E:E', 15)
    
    log.info(f"✅ Excel report saved: {OUTPUT_FILE}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Main pipeline execution."""
    print()
    print("═" * 70)
    print("  LiderTeks Sales Daily Groups Pipeline")
    print("  Period: December 2025")
    print("═" * 70)
    print()
    
    try:
        # Connect
        log.info("Connecting to database...")
        conn = get_connection()
        cursor = conn.cursor()
        log.info("✅ Connected!")
        
        # Extract
        raw_df = extract_sales_data(cursor)
        
        # Transform
        transformed_df = transform_data(raw_df)
        
        # Aggregate
        aggregated = aggregate_data(transformed_df)
        
        # Validate
        validate_data(aggregated)
        
        # Generate report
        generate_excel_report(aggregated, transformed_df)
        
        # Final summary
        print()
        print("═" * 70)
        log.info("PIPELINE COMPLETE")
        print("═" * 70)
        
        by_store = aggregated['by_store']
        total_revenue = by_store['revenue'].sum()
        total_checks = by_store['checks'].sum()
        
        log.info(f"Обработано строк:   {len(raw_df):,}")
        log.info(f"Магазинов:          {len(by_store)}")
        log.info(f"Товарных групп:     {len(aggregated['by_group'])}")
        log.info(f"Итого выручка:      {total_revenue:,.2f} ₽")
        log.info(f"Итого чеков:        {total_checks:,}")
        log.info(f"Выходной файл:      {OUTPUT_FILE}")
        
        cursor.close()
        conn.close()
        
        return 0
        
    except ValueError as e:
        log.error(f"Validation Error: {e}")
        return 1
    except pymssql.Error as e:
        log.error(f"Database Error: {e}")
        return 1
    except Exception as e:
        log.error(f"Unexpected Error: {e}")
        raise


if __name__ == "__main__":
    sys.exit(main())
