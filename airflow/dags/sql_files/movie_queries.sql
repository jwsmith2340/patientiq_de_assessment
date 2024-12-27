-- Third highest movie by revenue
SELECT * FROM movies ORDER BY revenue DESC LIMIT 1 OFFSET 2;

-- Will need movies that didn't recoup budget (revenue < budget, top 3 ordered by imdb_id)
SELECT title FROM movies WHERE revenue < budget ORDER BY imdb_id LIMIT 3;