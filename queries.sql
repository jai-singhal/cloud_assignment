select date, COUNT(votes) from reviews WHERE votes == 1 group by date HAVING COUNT(votes) > 2;

[k1, k2] : 
[k3 k1]
[k1 k2]