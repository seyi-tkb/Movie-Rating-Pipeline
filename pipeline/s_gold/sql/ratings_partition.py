partition_functions = """
-- 1. Create initial function
CREATE OR REPLACE FUNCTION prod.ensure_monthly_partition (tstamp DATE)
RETURNS VOID -- can be int, text, boolean, void (null), even tables & triggers 
AS $$
--optional
DECLARE
	partition_name TEXT;
	start_date DATE;
	end_date DATE;
	sql TEXT;
BEGIN
	-- Get dynamic partition boundaries
	start_date := date_trunc('month', tstamp) :: DATE;
	end_date := (start_date + INTERVAL '1 month') :: DATE;

	-- Get Partition name
	partition_name := format('ratings_%s_%s',
							EXTRACT(YEAR FROM start_date) :: INT,
							LPAD(EXTRACT (MONTH FROM start_date) :: TEXT, 2, '0')
							);
	-- Check if partition exists
	IF NOT EXISTS (
		SELECT 1
		FROM pg_class c
        JOIN pg_namespace n 
		ON c.relnamespace = n.oid
        WHERE c.relname = partition_name
		AND n.nspname = 'prod'
	) 
	THEN
		-- Create if it does not exist.
		sql := format($a$ CREATE TABLE prod.%I
						  PARTITION OF prod.ratings
						  FOR VALUES FROM ('%s') TO ('%s');
					  $a$, partition_name, start_date, end_date);

		EXECUTE sql;
		RAISE NOTICE 'Created partition: %', partition_name;
	END IF;
END;
$$ 
LANGUAGE plpgsql;

-- 2. Function calls above function on values in stg but creates the partition in prod
CREATE OR REPLACE FUNCTION prod.ensure_partitions_for_staging()
RETURNS VOID AS $$
DECLARE
    tstamp DATE;
BEGIN
    FOR tstamp IN
        SELECT DISTINCT date_trunc('month', timestamp)::DATE
        FROM stg.stg_ratings
    LOOP
        PERFORM prod.ensure_monthly_partition(tstamp);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
"""