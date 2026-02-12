-- Create table for product weights
create table if not exists product_weights (
  id bigint primary key generated always as identity,
  product_group text not null,
  product_name_pattern text, -- Optional: Regex or substring to match specific products within group
  avg_weight_kg numeric not null,
  created_at timestamp with time zone default timezone('utc'::text, now()) not null
);

-- Enable RLS
alter table product_weights enable row level security;

-- Policy for reading
create policy "Enable read access for all users" on product_weights for select using (true);

-- Seed Data based on User's Table
-- 1. Трикотаж -> 0.199
insert into product_weights (product_group, avg_weight_kg) values ('Трикотаж', 0.199);

-- 2.1 Брюки (Лето) -> 0.330
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Брюки', 'Лето', 0.330);

-- 2.2 Брюки (Зима) -> 0.462
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Брюки', 'Зима', 0.462);

-- Джинсы (Use 'Брюки' group? Or is it separate? User said "Джинсы" category in table, but in data "Джинсы М/Ж АКЦИЯ"). 
-- Let's try to match "Джинсы" pattern in any group or generic.
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('%', 'Джинсы', 0.596);
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('АКЦИЯ', 'Джинсы', 0.596);

-- 3. Джемпер -> 0.388
insert into product_weights (product_group, avg_weight_kg) values ('Джемпер', 0.388);

-- 4.1 Платье (Лето) -> 0.315
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Платье', 'Лето', 0.315);

-- 4.2 Платье (Всесезон/Зима? User table says "Всесезон" 2.74 -> 0.366 kg)
-- 1 / 2.74 = 0.3649... ~ 0.365. Table says 0.366.
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Платье', 'Всесезон', 0.366);

-- 5. Спорт -> 0.301
insert into product_weights (product_group, avg_weight_kg) values ('Спорт', 0.301);

-- 6.1 Куртки (Лето) -> 0.614
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Куртки', 'Лето', 0.614);
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Куртки', 'Всесезон', 0.614); -- Assumption: "Всесезон" maps to Summer/Light? Or Winter? Let's assume Light/Medium check avg later.

-- 6.2 Куртки (Зима) -> 0.972
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Куртки', 'Зима', 0.972);

-- 7. Рубашки/Блузки -> 0.186
insert into product_weights (product_group, avg_weight_kg) values ('Рубашки/Блузки', 0.186);

-- 8. Дети -> 0.217
insert into product_weights (product_group, avg_weight_kg) values ('Дети (от 0 до 14)', 0.217);
insert into product_weights (product_group, avg_weight_kg) values ('Детская одежда 0-1 г', 0.217);

-- 9.1 Обувь (Лето) -> 0.394 (units/kg 2.54)
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Обувь', 'Лето', 0.394);

-- 9.2 Обувь (Зима) -> 0.818 (units/kg 1.22)
insert into product_weights (product_group, product_name_pattern, avg_weight_kg) values ('Обувь', 'Зима', 0.818);

-- 10. Аксессуары -> 0.137
insert into product_weights (product_group, avg_weight_kg) values ('Аксессуары', 0.137);

-- 11. Текстиль -> 0.464
insert into product_weights (product_group, avg_weight_kg) values ('Текстиль', 0.464);
-- Detailed Textile items mappings? User just said "11. Текстиль". 
-- I'll map specific textile groups to this generic weight for now unless specified.
insert into product_weights (product_group, avg_weight_kg) values ('Наволочка', 0.464);
insert into product_weights (product_group, avg_weight_kg) values ('Пододеяльник', 0.464);
insert into product_weights (product_group, avg_weight_kg) values ('Простыня', 0.464);

-- 12. Сопутка -> 0.224
insert into product_weights (product_group, avg_weight_kg) values ('Сопутка', 0.224);
