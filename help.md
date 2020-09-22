SELECT <COLUMNS>, FUNC(COLUMN1)
FROM <TABLE>
GROUP BY <COLUMNS>
HAVING FUNC(COLUMN1)>X
--Here FUNC can be COUNT, MAX, MIN, SUM

r1
r2

Mapper 1:
(columns) : column1
example:
(review.cust) : salesrank

Reducer 1:
groups


Reducer
having clause