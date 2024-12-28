-- Third highest movie by revenue
SELECT title
FROM movies 
ORDER BY revenue DESC 
LIMIT 1 
OFFSET 2;
-- Titanic

-- Will need movies that didn't recoup budget (revenue < budget, top 3 ordered by imdb_id)
SELECT title
FROM movies 
WHERE (revenue < budget) AND revenue > 0
ORDER BY imdb_id
LIMIT 3;
-- Foolish Wives, The Merry Widow, and Metropolis

-- What's the average revenue per genre?  List them in order from highest to lowest average revenue.  For brevity, list the first three as the response to this question.
SELECT g.genre AS genre, AVG(m.revenue) AS average_revenue
FROM movies m
INNER JOIN movie_genre_nm mg ON m.id = mg.movie_id
INNER JOIN genres g ON mg.genre_id = g.id
WHERE m.revenue > 0
GROUP BY g.genre
ORDER BY average_revenue DESC
LIMIT 3;
-- Adventure: 182900545.74
-- Fantasy: 173280838.27
-- Animation: 172119567.78

-- How many movies are in more than one language?
SELECT COUNT(*) AS movies_w_multi_langs
FROM (
    SELECT movie_id
    FROM movie_language_nm
    GROUP BY movie_id
    HAVING COUNT(language_id) > 1
) AS multi_lang_movies;
-- 9073

-- You want to understand if there is a seasonal component to movie releases.  For each month, which genre had the highest proportion of releases?  There should be 1 answer for each calendar month.  If there's a tie, list all the genres.
WITH genre_counts_by_month AS (
	SELECT COUNT(*) AS genre_count, EXTRACT (MONTH FROM m.release_date) AS release_month, genre 
	FROM movie_genre_nm nm
	INNER JOIN movies m ON nm.movie_id = m.id
	INNER JOIN genres g ON nm.genre_id = g.id
    WHERE m.release_date <> '1901-01-01'
	GROUP BY release_month, g.genre
),
total_releases_by_month AS (
	SELECT SUM(genre_count) total_genre_count, release_month
	FROM genre_counts_by_month
	GROUP BY release_month
),
genre_proportion_per_month AS (
	SELECT gc.genre, gc.release_month, gc.genre_count, tr.total_genre_count, (gc.genre_count / tr.total_genre_count) AS proportion
	FROM genre_counts_by_month gc
	INNER JOIN total_releases_by_month tr ON gc.release_month = tr.release_month 
),
top_proportions_per_month AS (
	SELECT MAX(gp.proportion) AS max_proportion_each_month, gp.release_month
	FROM genre_proportion_per_month gp
	GROUP BY gp.release_month
)
SELECT gpm.genre, tpm.max_proportion_each_month, tpm.release_month
FROM genre_proportion_per_month gpm
INNER JOIN top_proportions_per_month tpm ON gpm.release_month = tpm.release_month AND tpm.max_proportion_each_month = gpm.proportion
ORDER BY tpm.release_month, gpm.genre
-- Drama was the top category all 12 months


-- Verification that Drama had the highest number of releases for every month
WITH genre_counts_by_month AS (
	SELECT COUNT(*) AS genre_count, EXTRACT (MONTH FROM m.release_date) AS release_month, genre 
	FROM movie_genre_nm nm
	INNER JOIN movies m ON nm.movie_id = m.id
	INNER JOIN genres g ON nm.genre_id = g.id
    WHERE m.release_date <> '1901-01-01'
	GROUP BY release_month, g.genre
),
total_releases_by_month AS (
	SELECT SUM(genre_count) total_genre_count, release_month
	FROM genre_counts_by_month
	GROUP BY release_month
)
SELECT * FROM genre_counts_by_month ORDER BY release_month, genre_count