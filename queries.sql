select date, COUNT(votes) from reviews WHERE votes == 1 group by date HAVING COUNT(votes) > 2;

select category, asin, max(salesrank) from products where category = 'Books[283155]' group by category, asin having max(salesrank) > 1000;

select category,  max(avg_rating) from products where category = 'Books[283155]' group by category having max(avg_rating) > 4.2;

select category, asin, max(salesrank) from products where category = 'Books[283155]' group by category, asin having max(salesrank) >= 200000;

select category, title, count(salesrank) from products where category = 'Books[283155]' group by category, title having count(salesrank) >= 1;

select category, avg_rating, max(avg_rating) from products where category = 'Books[283155]' group by category, avg_rating having max(avg_rating) >= 2;

select category_name, downloaded, max(avg_rating) from products where category_name = 'Travel' group by category_name, downloaded having max(avg_rating) >= 2

select similar_asin, downloaded, max(avg_rating) from products where similar_asin = '0590769855' group by similar_asin, downloaded having max(avg_rating) >= 1;

select user_id, date, max(votes) from reviews where user_id = 'A28ZN9L5P6PDKP' group by user_id, date having max(votes) >= 4;

select helpful, user_id, date, max(votes) from reviews where date = '2001-12-5' group by helpful, user_id, date having max(votes) >= 0

select category_name, group, max(avg_rating) from products where category_name = 'Religion & Spirituality' group by category_name, group having max(avg_rating) >= 3

nw:
"select category_name, count(avg_rating) from products where avg_rating>100 group by category_name, similar_asin having count(similar_asin)>3"

select user_id, count(rating) from reviews where votes >=10 group by user_id having count(rating) >= 3