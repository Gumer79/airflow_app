-- ============================================================================
-- Скрипт для проверки и создания датасетов
-- ============================================================================
-- Используйте этот скрипт для проверки location датасетов и их создания
-- ============================================================================

-- Проверка 1: Проверяем существование и location датасета user_data
SELECT 
  schema_name,
  location,
  creation_time
FROM `shirman-group-app.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name IN ('user_data', 'wildberries_raw')
ORDER BY schema_name;

-- Если датасет user_data не найден, создайте его:
-- Вариант 1: Если нужно создать в US
CREATE SCHEMA IF NOT EXISTS `shirman-group-app.user_data`
OPTIONS(
  location="US",
  description="Dataset for user data and companies"
);

-- Вариант 2: Если нужно создать в EU
/*
CREATE SCHEMA IF NOT EXISTS `shirman-group-app.user_data`
OPTIONS(
  location="EU",
  description="Dataset for user data and companies"
);
*/

-- Вариант 3: Multi-region (без указания location)
/*
CREATE SCHEMA IF NOT EXISTS `shirman-group-app.user_data`
OPTIONS(
  description="Dataset for user data and companies"
);
*/

-- Проверка 2: Проверяем существование таблицы users
SELECT 
  table_name,
  table_type,
  creation_time
FROM `shirman-group-app.user_data.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'users';

-- Если таблицы users нет, но у вас есть данные, проверьте правильность имени датасета:
SELECT DISTINCT schema_name 
FROM `shirman-group-app.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'users'
  OR table_name = 'companies';

