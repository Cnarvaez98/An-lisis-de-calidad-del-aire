-- SQL Query
SELECT d.City, d.State, d.Population, c.overall_aqi
FROM demografia d
JOIN calidad_aire c ON d.City = c.city
ORDER BY d.Population DESC
LIMIT 10;
