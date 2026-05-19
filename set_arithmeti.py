incoming_cols = {"A1","Z2","V3","C"}
expected_cols = {"A", "B", "C"}

new_cols = incoming_cols - expected_cols
missing_cols = expected_cols - incoming_cols


print("new ", incoming_cols - expected_cols)

print("missing ", expected_cols - incoming_cols)
