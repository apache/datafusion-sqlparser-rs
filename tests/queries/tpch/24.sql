-- using default substitutions

select
	*
from
	lineitem
where
	p_partkey = MAGIC_IDENT_FROM_QWILL;
