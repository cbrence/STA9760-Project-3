SELECT a.company, a.hour, b.ts AS datetime, a.hourly_high_price
FROM 
    (SELECT name AS company, hour(date_add('hour',-4,cast(ts AS timestamp))) AS hour, max(high) AS hourly_high_price
    FROM "proj3buck"
    GROUP BY  1, 2
    ORDER BY  1, 2 
    ) a, "proj3buck" b
WHERE a.company=b.name 
AND a.hour = hour(date_add('hour',-4,cast(b.ts AS timestamp))) 
AND a.hourly_high_price = b.high
ORDER BY  1, 3