-- Write grants on AUD_ and CFG_ tables reaching PUBLIC or another ordinary login.
SELECT DISTINCT c.relname AS table_name,
       CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE granted.rolname END AS role_name,
       a.privilege_type AS privilege
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) a
LEFT JOIN pg_roles granted ON granted.oid = a.grantee
WHERE n.nspname = current_schema() AND c.relkind = 'r'
  AND (c.relname LIKE 'aud\_%' ESCAPE '\' OR c.relname LIKE 'cfg\_%' ESCAPE '\')
  AND a.privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')
  AND a.grantee <> c.relowner
  AND (a.grantee = 0 OR EXISTS (
    SELECT 1 FROM pg_roles login
    WHERE login.rolcanlogin AND NOT login.rolsuper AND login.oid <> c.relowner
      AND login.rolname <> current_user
      AND (login.oid = a.grantee OR pg_has_role(login.oid, a.grantee, 'MEMBER'))))
ORDER BY table_name, role_name, privilege
