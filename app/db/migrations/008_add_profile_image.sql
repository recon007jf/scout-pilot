-- Add profile_image column to target_brokers table
ALTER TABLE target_brokers
ADD COLUMN IF NOT EXISTS profile_image TEXT;