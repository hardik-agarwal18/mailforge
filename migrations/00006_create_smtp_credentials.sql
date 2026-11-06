-- +goose Up

-- SMTP submission credentials are separate from application/database users.
-- Passwords are stored as salted PBKDF2-HMAC-SHA256 hashes, never plaintext.
create table smtp_credentials (
   id                    uuid primary key default gen_random_uuid(),
   username              text not null unique,
   email_address         text not null unique,
   password_hash         text not null,
   enabled               boolean not null default true,
   last_authenticated_at timestamptz,
   created_at            timestamptz not null default now(),
   updated_at            timestamptz not null default now(),
   constraint smtp_credentials_username_length
      check ( char_length(username) between 3 and 64 ),
   constraint smtp_credentials_username_normalized check ( username = lower(username) ),
   constraint smtp_credentials_email_not_empty
      check ( length(trim(email_address)) > 3
         and email_address = lower(email_address) ),
   constraint smtp_credentials_password_hash_not_empty check ( length(password_hash) > 0 )
);

-- +goose Down

drop table if exists smtp_credentials;