SELECT COUNT(saa.artist_id) artist_total_sales_yesterday FROM sales s
LEFT JOIN sale_album_assignment saa USING(sale_id)
LEFT JOIN album a USING(album_id)
LEFT JOIN artist_album_assignment aaa USING(album_id)
LEFT JOIN artist ar USING(artist_id)
