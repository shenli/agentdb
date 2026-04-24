ALTER TABLE admin_tokens
    ADD COLUMN scope TEXT NOT NULL DEFAULT 'admin';
