SELECT COALESCE(SUM(total_vnd), 0) AS value FROM "public"."orders" WHERE status != 'cancelled'
