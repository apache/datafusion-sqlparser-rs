ALTER VIEW IF EXISTS s.v OWNER TO CURRENT_USER;
ALTER VIEW v SET (security_invoker = true, security_barrier);
ALTER VIEW v RESET (check_option);
ALTER VIEW v ALTER c SET DEFAULT 1;
ALTER VIEW v RENAME c TO d
