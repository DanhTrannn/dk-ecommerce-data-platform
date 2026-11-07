SELECT COALESCE(SUM(amount_vnd), 0) AS value FROM "public"."payments" WHERE status = 'succeeded'
