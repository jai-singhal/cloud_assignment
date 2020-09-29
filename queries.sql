select date, COUNT(votes) from reviews WHERE votes == 1 group by date HAVING COUNT(votes) > 2;

select category, asin, max(salesrank) from products where category = 'Books[283155]' group by category, asin having max(salesrank) > 1000;

select category,  max(avg_rating) from products where category = 'Books[283155]' group by category having max(avg_rating) > 4.2;

select category, asin, max(salesrank) from products where category = 'Books[283155]' group by category, asin having max(salesrank) >= 200000;

select category, title, count(salesrank) from products where category = 'Books[283155]' group by category, title having count(salesrank) >= 1;

select category, avg_rating, max(avg_rating) from products where category = 'Books[283155]' group by category, avg_rating having max(avg_rating) >= 2;

select category_name, downloaded, max(avg_rating) from products where category_name = 'Travel' group by category_name, downloaded having max(avg_rating) >= 2

select similar_asin, downloaded, max(avg_rating) from products where similar_asin = '0590769855' group by similar_asin, downloaded having max(avg_rating) >= 1;