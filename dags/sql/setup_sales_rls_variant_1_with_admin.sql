-- ============================================================================
-- ВАРИАНТ 1 с поддержкой администраторов (is_admin)
-- ============================================================================
-- Готовый к использованию скрипт для настройки RLS с поддержкой is_admin
--
-- Как использовать:
-- 1. Откройте BigQuery Console: https://console.cloud.google.com/bigquery
-- 2. Выберите проект: shirman-group-app
-- 3. Создайте новый SQL запрос
-- 4. Сначала выполните проверку датасета (Шаг 0)
-- 5. Затем выполните остальные команды последовательно
-- ============================================================================
--
-- ВАЖНО: Если вы получаете ошибку "Dataset not found in location EU":
-- 1. Выполните Шаг 0 для проверки location датасета
-- 2. Если датасет существует в другом location (US), это нормально -
--    просто продолжите выполнение остальных шагов
-- 3. Если датасет не существует, создайте его (инструкции в Шаге 0)
-- ============================================================================

-- Шаг 0: Проверяем существование и location датасета user_data
-- Сначала выполните этот запрос, чтобы узнать location существующего датасета:
/*
SELECT
  schema_name,
  location,
  creation_time
FROM `shirman-group-app.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name = 'user_data';
*/

-- Если датасет user_data НЕ существует, создайте его:
-- Вариант 1: Создать в US (по умолчанию)
-- CREATE SCHEMA IF NOT EXISTS `shirman-group-app.user_data`
-- OPTIONS(
--   location="US",
--   description="Dataset for user data and companies"
-- );

-- Вариант 2: Создать в EU (если нужен тот же location, что и wildberries_raw)
-- CREATE SCHEMA IF NOT EXISTS `shirman-group-app.user_data`
-- OPTIONS(
--   location="EU",
--   description="Dataset for user data and companies"
-- );

-- ВАЖНО: Если датасет УЖЕ существует в другом location (например, US),
-- НЕ создавайте его заново - просто используйте существующий location.
-- В BigQuery нельзя изменить location существующего датасета!

-- Шаг 1: Добавляем поле is_admin в таблицу users (если его нет)
-- Если таблицы users еще нет, сначала создайте ее
-- Если таблица существует, выполните эту команду:
ALTER TABLE `shirman-group-app.user_data.users`
ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;

-- Шаг 2: Удаляем существующую политику (если есть)

DROP ALL ROW ACCESS POLICIES ON `shirman-group-app.wildberries_raw.sales_raw`;


DROP ROW ACCESS POLICY IF EXISTS sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`;

-- Шаг 3: Создаем RLS политику с поддержкой администраторов
CREATE OR REPLACE ROW ACCESS POLICY sales_rls_policy
ON `shirman-group-app.wildberries_raw.sales_raw`
GRANT TO ('allAuthenticatedUsers')
FILTER USING (
  -- Администраторы видят все данные (is_admin = TRUE для текущего пользователя)
  EXISTS (
    SELECT 1
    FROM `shirman-group-app.user_data.users`
    WHERE email = SESSION_USER() AND is_admin = TRUE
  )
  OR
  -- Обычные пользователи видят только данные своей компании
  company_id IN (
    SELECT company_id
    FROM `shirman-group-app.user_data.users`
    WHERE email = SESSION_USER()
  )
);

-- ============================================================================
-- Проверка: Убеждаемся, что политика создана
-- ============================================================================

SELECT
  policy_name,
  grantee_type,
  grantee,
  filter_predicate
FROM `shirman-group-app.wildberries_raw.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`
WHERE table_name = 'sales_raw'
ORDER BY policy_name;

-- ============================================================================
-- Назначение администраторов (опционально)
-- ============================================================================
-- Раскомментируйте и замените email на реальные адреса администраторов

/*
-- Пример 1: Назначить одного администратора
UPDATE `shirman-group-app.user_data.users`
SET is_admin = TRUE
WHERE email = 'admin@example.com';

-- Пример 2: Назначить нескольких администраторов
UPDATE `shirman-group-app.user_data.users`
SET is_admin = TRUE
WHERE email IN (
  'admin1@example.com',
  'admin2@example.com',
  'admin3@example.com'
);

-- Пример 3: Снять права администратора
UPDATE `shirman-group-app.user_data.users`
SET is_admin = FALSE
WHERE email = 'old-admin@example.com';
*/

-- ============================================================================
-- Проверка работы RLS
-- ============================================================================
-- Выполните эти запросы под разными пользователями для проверки

/*
-- Проверка 1: Администратор должен видеть все данные
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT company_id) as companies_count,
  SESSION_USER() as current_user,
  -- Проверяем, является ли текущий пользователь администратором
  EXISTS (
    SELECT 1
    FROM `shirman-group-app.user_data.users`
    WHERE email = SESSION_USER()
      AND is_admin = TRUE
  ) as is_admin_user
FROM `shirman-group-app.wildberries_raw.sales_raw`;

-- Проверка 2: Обычный пользователь должен видеть только свою компанию
SELECT DISTINCT company_id
FROM `shirman-group-app.wildberries_raw.sales_raw`
ORDER BY company_id;
*/
