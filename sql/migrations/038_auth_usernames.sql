ALTER TABLE auth.users
  ADD COLUMN IF NOT EXISTS username text;

WITH candidates AS (
  SELECT
    u.id,
    CASE
      WHEN COALESCE(btrim(u.username), '') <> '' THEN lower(btrim(u.username))
      ELSE lower(split_part(u.email, '@', 1))
    END AS candidate,
    u.created_at
  FROM auth.users u
),
ranked AS (
  SELECT
    c.id,
    c.candidate,
    row_number() OVER (PARTITION BY c.candidate ORDER BY c.created_at NULLS FIRST, c.id) AS candidate_rank,
    EXISTS (
      SELECT 1
      FROM auth.users other
      WHERE other.id <> c.id
        AND lower(other.email) = c.candidate
    ) AS email_conflict
  FROM candidates c
),
resolved AS (
  SELECT
    r.id,
    CASE
      WHEN r.candidate ~ '^[a-z0-9._-]{3,32}$'
       AND r.candidate_rank = 1
       AND NOT r.email_conflict
        THEN r.candidate
      ELSE 'u-' || substr(replace(r.id::text, '-', ''), 1, 30)
    END AS username
  FROM ranked r
)
UPDATE auth.users u
SET username = resolved.username
FROM resolved
WHERE u.id = resolved.id
  AND u.username IS DISTINCT FROM resolved.username;

ALTER TABLE auth.users
  ALTER COLUMN username SET NOT NULL;

DROP INDEX IF EXISTS auth.uq_auth_users_username;
CREATE UNIQUE INDEX IF NOT EXISTS uq_auth_users_username ON auth.users (username);

ALTER TABLE auth.users
  DROP CONSTRAINT IF EXISTS ck_auth_users_username_format;

ALTER TABLE auth.users
  ADD CONSTRAINT ck_auth_users_username_format CHECK (
    username = lower(username)
    AND username ~ '^[a-z0-9._-]{3,32}$'
  );
