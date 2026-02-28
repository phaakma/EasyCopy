from easycopy import EasyCopy

result = EasyCopy.copy_data(
	source=r"C:\projects\playground\playground.gdb\easycopy_source",
	target=r"C:\projects\playground\playground.gdb\easycopy_target",
	copy_method="TRUNCATE_APPEND",
	schema_comparison_type="SOFT",
	log_changesets=True,
)
print(result)