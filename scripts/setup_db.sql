DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'contractor_app') THEN
      CREATE ROLE contractor_app LOGIN PASSWORD 'contractor_app_pw';
   END IF;
END
$$;
