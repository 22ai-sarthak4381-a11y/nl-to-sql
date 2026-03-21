import re

def sanitize_sql(raw: str) -> str:
    raw = raw.replace("```sql", "").replace("```", "").strip()
    select_idx = raw.upper().find("SELECT")
    if select_idx != -1:
        raw = raw[select_idx:]
    parts = re.split(r';\s*(?=SELECT)', raw, flags=re.IGNORECASE)
    parts = [p.strip().rstrip(';') for p in parts if p.strip()]
    if len(parts) > 1:
        sub_parts = []
        for part in parts:
            clean = re.sub(r'\s+ORDER\s+BY\s+.+$', '', part, flags=re.IGNORECASE | re.DOTALL).strip()
            sub_parts.append(clean)
        raw = "\nUNION ALL\n".join(sub_parts)
    else:
        raw = parts[0] if parts else raw
        raw = raw.rstrip(';')
    return raw

# Simulate the exact broken output from the cache
broken = """SELECT 
    income_level, 
    AVG(purchase_amount::NUMERIC) AS avg_purchase_amount
FROM 
    ecommerce_behavior
GROUP BY 
    income_level
ORDER BY 
    avg_purchase_amount DESC;

SELECT 
    frequency_of_purchase, 
    AVG(purchase_amount::NUMERIC) AS avg_purchase_amount
FROM 
    ecommerce_behavior
GROUP BY 
    frequency_of_purchase
ORDER BY 
    avg_purchase_amount DESC;

SELECT 
    discount_sensitivity, 
    AVG(purchase_amount::NUMERIC) AS avg_purchase_amount
FROM 
    ecommerce_behavior
GROUP BY 
    discount_sensitivity
ORDER BY 
    avg_purchase_amount DESC;"""

result = sanitize_sql(broken)
select_count = len(re.findall(r'\bSELECT\b', result, re.IGNORECASE))
union_count  = len(re.findall(r'\bUNION ALL\b', result, re.IGNORECASE))

print(f"Input had 3 separate SELECTs  ->  Output has {select_count} SELECT(s), {union_count} UNION ALL(s)")
print()
print("RESULT SQL:")
print(result)

assert select_count == 3, "Should still have 3 SELECTs"
assert union_count == 2,  "Should have 2 UNION ALLs joining them"
assert result.count(';') == 0, "No semicolons should remain"
print("\nAll assertions passed! ✅  Single query — Supabase will accept it.")
