-- ============================================================================
-- Скрипт для настройки Row-Level Security (RLS) для таблицы sales_raw
-- ============================================================================
-- Назначение: Ограничить доступ к данным продаж на основе company_id
-- Пользователи видят только данные своей компании
--
-- Как использовать:
-- 1. Откройте BigQuery Console: https://console.cloud.google.com/bigquery
-- 2. Выберите проект: shirman-group-app
-- 3. Создайте новый SQL запрос
-- 4. Скопируйте один из вариантов ниже и выполните
-- 5. При необходимости замените значения проектов/датасетов/таблиц
-- ============================================================================

-- ============================================================================
-- ВАРИАНТ 1: Простой RLS с подзапросом к таблице users (РЕКОМЕНДУЕТСЯ)
-- ============================================================================

-- Шаг 1: Удаляем существующую политику (если есть)
DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`;

-- Шаг 2: Создаем RLS политику
CREATE ROW ACCESS POLICY sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`
GRANT TO ('allAuthenticatedUsers')
FILTER USING (
  -- Пользователь видит только данные своей компании
  company_id IN (
    SELECT company_id 
    FROM `shirman-group-app.user_data.users` 
    WHERE email = SESSION_USER()
  )
  -- Для тестирования: можно временно разрешить доступ к определенным email
  -- Раскомментируйте следующую строку для добавления тестовых пользователей:
  -- OR SESSION_USER() IN ('admin@example.com', 'test@example.com')
);

-- ============================================================================
-- ВАРИАНТ 1А: Простой RLS с поддержкой администраторов (is_admin)
-- ============================================================================
-- Для готового к использованию скрипта смотрите: setup_sales_rls_variant_1_with_admin.sql

/*
-- Шаг 1: Добавляем поле is_admin в таблицу users (если его нет)
ALTER TABLE `shirman-group-app.user_data.users`
ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;

-- Шаг 2: Удаляем существующую политику (если есть)
DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`;

-- Шаг 3: Создаем RLS политику с поддержкой администраторов
CREATE ROW ACCESS POLICY sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`
GRANT TO ('allAuthenticatedUsers')
FILTER USING (
  -- Администраторы видят все данные
  EXISTS (
    SELECT 1 
    FROM `shirman-group-app.user_data.users` 
    WHERE email = SESSION_USER() 
      AND is_admin = TRUE
  )
  OR
  -- Обычные пользователи видят только данные своей компании
  company_id IN (
    SELECT company_id 
    FROM `shirman-group-app.user_data.users` 
    WHERE email = SESSION_USER()
  )
);

-- Шаг 4 (опционально): Назначаем администраторов
-- Пример: сделать пользователя admin@example.com администратором
-- UPDATE `shirman-group-app.user_data.users`
-- SET is_admin = TRUE
-- WHERE email = 'admin@example.com';
*/

-- ============================================================================
-- ВАРИАНТ 2: RLS с таблицей-маппингом (для больших объемов данных)
-- ============================================================================
-- Раскомментируйте весь блок ниже, если хотите использовать более производительный вариант

/*
-- Шаг 1: Создаем таблицу-маппинг user -> company_id с кластеризацией
CREATE OR REPLACE TABLE `shirman-group-app.wildberries_raw.user_company_mapping` 
CLUSTER BY email
AS 
SELECT DISTINCT
  email,
  company_id
FROM `shirman-group-app.user_data.users`
WHERE email IS NOT NULL 
  AND company_id IS NOT NULL;

-- Шаг 2: Удаляем существующую политику
DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`;

-- Шаг 3: Создаем RLS политику с использованием таблицы-маппинга
CREATE ROW ACCESS POLICY sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`
GRANT TO ('allAuthenticatedUsers')
FILTER USING (
  company_id IN (
    SELECT company_id 
    FROM `shirman-group-app.wildberries_raw.user_company_mapping` 
    WHERE email = SESSION_USER()
  )
);
*/

-- ============================================================================
-- ВАРИАНТ 3: RLS с поддержкой администраторов
-- ============================================================================
-- Раскомментируйте, если нужна поддержка администраторов

/*
-- Шаг 1: Добавляем поле is_admin в таблицу users (если его нет)
ALTER TABLE `shirman-group-app.user_data.users`
ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;

-- Шаг 2: Удаляем существующую политику
DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`;

-- Шаг 3: Создаем RLS политику с поддержкой администраторов
CREATE ROW ACCESS POLICY sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`
GRANT TO ('allAuthenticatedUsers')
FILTER USING (
  -- Администраторы видят все данные
  EXISTS (
    SELECT 1 
    FROM `shirman-group-app.user_data.users` 
    WHERE email = SESSION_USER() 
      AND is_admin = TRUE
  )
  OR
  -- Обычные пользователи видят только свою компанию
  company_id IN (
    SELECT company_id 
    FROM `shirman-group-app.user_data.users` 
    WHERE email = SESSION_USER()
  )
);
*/

-- ============================================================================
-- ПРОВЕРКА: Проверяем, что RLS политика создана
-- ============================================================================
-- Выполните этот запрос после создания политики

SELECT
  policy_name,
  grantee_type,
  grantee,
  filter_predicate
FROM `shirman-group-app.wildberries_raw.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`
WHERE table_name = 'sales_raw'
ORDER BY policy_name;

-- ============================================================================
-- ТЕСТИРОВАНИЕ: Проверяем работу RLS
-- ============================================================================
-- Выполните этот запрос под разными пользователями для проверки
-- Каждый пользователь должен видеть только данные своей компании

-- Проверка 1: Сколько записей видит текущий пользователь?
-- SELECT 
--   COUNT(*) as total_rows,
--   COUNT(DISTINCT company_id) as companies_count,
--   SESSION_USER() as current_user
-- FROM `shirman-group-app.wildberries_raw.sales_raw`;

-- Проверка 2: Список company_id, которые видит текущий пользователь
-- SELECT DISTINCT company_id
-- FROM `shirman-group-app.wildberries_raw.sales_raw`
-- ORDER BY company_id;

-- ============================================================================
-- ОБНОВЛЕНИЕ: Обновление таблицы-маппинга (если используете Вариант 2)
-- ============================================================================

/*
-- Если используете Вариант 2, периодически обновляйте таблицу-маппинг
CREATE OR REPLACE TABLE `shirman-group-app.wildberries_raw.user_company_mapping` 
CLUSTER BY email
AS 
SELECT DISTINCT
  email,
  company_id
FROM `shirman-group-app.user_data.users`
WHERE email IS NOT NULL 
  AND company_id IS NOT NULL;
*/

-- ============================================================================
-- УДАЛЕНИЕ: Если нужно удалить RLS политику
-- ============================================================================

/*
DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`;
*/

