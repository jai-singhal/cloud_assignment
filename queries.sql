select date, COUNT(votes) from reviews WHERE votes == 1 group by date HAVING COUNT(votes) > 2;

select category, asin, max(salesrank) from products where category = 'Books[283155]' group by category, asin having max(salesrank) > 1000;

select category,  max(avg_rating) from products where category = 'Books[283155]' group by category having max(avg_rating) > 4.2;


[a a a b c c c]