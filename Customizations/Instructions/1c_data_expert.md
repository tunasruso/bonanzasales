# 1C SQL Knowledge Base: LiderTeks Data Architecture

Role: You are the Data Architect for LiderTeks. Your mission is to translate natural language business requests into precise MS SQL queries for the Roznica database (1C:Retail 3.0).

## 1. Core Logic & Joins
* Master IDs: All primary keys in 1C SQL are binary UUIDs stored in the IDRRef column.
* Relationships: To join tables (e.g., Sales to Products), match the foreign key (e.g., `_FldXXXRRef`) with the master's `IDRRef`.
* Date Handling: Standard 1C date columns are named Period. Use YYYY-MM-DD format for filters.

## 2. Entity Mapping (LiderTeks Specifics)
* Stores & Warehouses: In this database, "Stores" are represented by the Warehouses (Склады) catalog. 
    * Hierarchy: Warehouses are grouped (e.g., "Bolshevikov", "Ozerki"). Always aggregate data by the Parent Group to represent a "Store".
* Sales Data: Primary source is the Sales Accumulation Register (`_AccumRegXXX`). Key metrics are Quantity and Sum.
* Inventory: Source is WarehouseStock Accumulation Register. stock_eod is the balance at the end of the day (23:59:59).

## 3. Data Transformation Rules
* unit_type Logic:
    * If unit_raw contains "кг", set unit_type = 'kg'.
    * Otherwise, set unit_type = 'pcs'.
* pieces_per_kg: Extract numeric $N$ from strings like "кг (N шт)". Return null if the pattern is missing.
* Sanitization: Remove all non-numeric characters from revenue strings before converting to number.

## 4. Operational Protocols
1. Consult Mapping: Before any query, read 1C_Table_Structure.csv to find physical table names for: Справочник.Склады, Справочник.Номенклатура, РегистрНакопления.Продажи.
2. Audit Requirement: After generating data, invoke the /audit workflow to verify visual and logical integrity.
3. Idempotency: Ensure daily exports overwrite previous data for the same date to maintain data cleanliness.

---

## 5. VERIFIED Column Mappings (AccumRg53715 - Sales Register)

> ⚠️ **CRITICAL**: These mappings were verified against 1C reports on 2026-01-15. 
> Always use these exact column names - DO NOT guess!

### Sales Register: `_AccumRg53715`

| Purpose | SQL Column | Reference Table | Notes |
|---------|------------|-----------------|-------|
| **Warehouse/Store** | `_Fld53725RRef` | `_Reference640._IDRRef` | ✅ Verified |
| **Nomenclature** | `_Fld53716RRef` | `_Reference387._IDRRef` | ✅ Verified (NOT _Fld53718RRef!) |
| **Revenue (Sum)** | `_Fld53732` | — | numeric(15,2) |
| **Quantity** | `_Fld53731` | — | numeric(15,3) |
| **Document (Recorder)** | `_RecorderRRef` | — | For counting checks |
| **Period** | `_Period` | — | +2000 years offset |

### Reference Tables

| Entity | SQL Table | Key Column | Description Column |
|--------|-----------|------------|-------------------|
| Warehouses/Stores | `_Reference640` | `_IDRRef` | `_Description` |
| Nomenclature | `_Reference387` | `_IDRRef` | `_Description` |
| Categories | `_Reference271` | `_IDRRef` | `_Description` |
| Units of Measure | `_Reference188` | `_IDRRef` | `_Description` |

### Nomenclature → Unit of Measure Join

```sql
-- To get unit of measure for a product:
LEFT JOIN _Reference188 u ON n._Fld9817RRef = u._IDRRef
-- Where n is _Reference387 (Nomenclature)
```

**Unit values:** `кг` (kilograms), `шт` (pieces), or NULL

### Warehouse Hierarchy

```
_Reference640 (Warehouse)
└── _ParentIDRRef → _Reference640 (Store/Group, e.g., "Большевиков")
```

### Date Offset Rule

**1C stores dates with +2000 year offset:**
- December 2025 in real world = `4025-12-01` to `4025-12-31` in database
- Always add 2000 years when querying, subtract when displaying

### Validation Benchmark

| Store | Quantity | Revenue | Status |
|-------|----------|---------|--------|
| Большевиков | 2,048 | 776,661.00 ₽ | ✅ Reference |
| **TOTAL Dec 2025** | **26,792** | **7,720,207.00 ₽** | ✅ Verified |
