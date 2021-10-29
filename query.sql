SELECT * FROM (
  SELECT temp1.name, temp1.hour, temp1.ts, temp2.highest_price FROM (
  SELECT DISTINCT name, high, ts, SUBSTRING(ts, 12, 2) AS hour  FROM project3datastream) temp1
INNER JOIN (SELECT name, SUBSTRING(ts, 12, 2) AS hour, MAX(high) AS highest_price FROM project3datastream 
GROUP BY name, SUBSTRING(ts, 12, 2)) temp2
ON temp1.name = temp2.name AND temp1.hour = temp2.hour AND temp1.high = temp2.highest_price)
ORDER BY name, hour