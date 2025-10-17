// ============================================================
// GOODREADS BOOKS KNOWLEDGE GRAPH - IMPORT SCRIPT
// ============================================================

// ===== STEP 1: CREATE CONSTRAINTS =====
CREATE CONSTRAINT book_id_unique IF NOT EXISTS FOR (b:Book) REQUIRE b.book_id IS UNIQUE;
CREATE CONSTRAINT author_id_unique IF NOT EXISTS FOR (a:Author) REQUIRE a.author_id IS UNIQUE;
CREATE CONSTRAINT publisher_id_unique IF NOT EXISTS FOR (p:Publisher) REQUIRE p.publisher_id IS UNIQUE;
CREATE CONSTRAINT lang_id_unique IF NOT EXISTS FOR (l:Language) REQUIRE l.lang_id IS UNIQUE;
CREATE CONSTRAINT year_id_unique IF NOT EXISTS FOR (y:Year) REQUIRE y.year_id IS UNIQUE;

// ===== STEP 2: IMPORT NODES =====

// Books
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/nodes_books.csv' AS row
CREATE (b:Book {
  book_id: toInteger(row.book_id),
  title: row.title,
  avg_rating: toFloat(row.avg_rating),
  isbn: row.isbn,
  language: row.language,
  pages: toInteger(row.pages),
  ratings_count: toInteger(row.ratings_count),
  reviews_count: toInteger(row.reviews_count),
  publication_date: row.publication_date,
  rating_category: row.rating_category,
  popularity: row.popularity
});

/ Authors
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/nodes_authors.csv' AS row
CREATE (a:Author {
  author_id: row.author_id,
  name: row.name
});

// Publishers
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/nodes_publishers.csv' AS row
CREATE (p:Publisher {
  publisher_id: row.publisher_id,
  name: row.name
});

// Languages
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/nodes_languages.csv' AS row
CREATE (l:Language {
  lang_id: row.lang_id,
  code: row.code
});

// Years
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/nodes_years.csv' AS row
CREATE (y:Year {
  year_id: row.year_id,
  year: toInteger(row.year),
  decade: toInteger(row.decade)
});

// Verify nodes
MATCH (n) RETURN labels(n)[0] AS NodeType, COUNT(n) AS Count ORDER BY Count DESC;

// ===== STEP 3: CREATE INDEXES =====
CREATE INDEX book_title_idx IF NOT EXISTS FOR (b:Book) ON (b.title);
CREATE INDEX book_rating_idx IF NOT EXISTS FOR (b:Book) ON (b.avg_rating);
CREATE INDEX author_name_idx IF NOT EXISTS FOR (a:Author) ON (a.name);
CREATE INDEX publisher_name_idx IF NOT EXISTS FOR (p:Publisher) ON (p.name);

// ===== STEP 4: IMPORT RELATIONSHIPS =====

// WRITTEN_BY
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/rel_book_written_by_author.csv' AS row
MATCH (b:Book {book_id: toInteger(row.book_id)})
MATCH (a:Author {author_id: row.author_id})
CREATE (b)-[:WRITTEN_BY]->(a);

// PUBLISHED_BY
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/rel_book_published_by_publisher.csv' AS row
MATCH (b:Book {book_id: toInteger(row.book_id)})
MATCH (p:Publisher {publisher_id: row.publisher_id})
CREATE (b)-[:PUBLISHED_BY]->(p);

// IN_LANGUAGE
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/rel_book_in_language.csv' AS row
MATCH (b:Book {book_id: toInteger(row.book_id)})
MATCH (l:Language {lang_id: row.lang_id})
CREATE (b)-[:IN_LANGUAGE]->(l);

// PUBLISHED_IN_YEAR
LOAD CSV WITH HEADERS FROM 'file:///neo4j_books_import/rel_book_published_in_year.csv' AS row
MATCH (b:Book {book_id: toInteger(row.book_id)})
MATCH (y:Year {year_id: row.year_id})
CREATE (b)-[:PUBLISHED_IN]->(y);

// Verify relationships
MATCH ()-[r]->() RETURN type(r) AS RelType, COUNT(r) AS Count ORDER BY Count DESC;

// ===== STEP 5: SAMPLE VISUALIZATION =====
MATCH path = (b:Book)-[r]->(x)
RETURN path
LIMIT 50;


//Hidden Gems (Rating Tinggi tapi Kurang Dikenal)
MATCH (b:Book)
WHERE b.avg_rating >= 4.3 
  AND b.ratings_count < 5000 
  AND b.ratings_count > 100
RETURN b.title AS Judul,
       b.avg_rating AS Rating,
       b.ratings_count AS JumlahRating,
       b.popularity AS Popularitas
ORDER BY b.avg_rating DESC
LIMIT 15;

//Penulis dengan Rating Konsisten Tinggi
MATCH (b:Book)-[:WRITTEN_BY]->(a:Author)
WITH a, 
     AVG(b.avg_rating) AS AvgRating,
     MIN(b.avg_rating) AS MinRating,
     MAX(b.avg_rating) AS MaxRating,
     COUNT(b) AS TotalBuku
WHERE TotalBuku >= 3 AND MinRating >= 4.0
RETURN a.name AS Penulis,
       ROUND(AvgRating, 2) AS RataRating,
       ROUND(MinRating, 2) AS RatingTerendah,
       ROUND(MaxRating, 2) AS RatingTertinggi,
       TotalBuku
ORDER BY AvgRating DESC
LIMIT 10;

//Publisher mana yang menerbitkan buku paling populer?
MATCH (b:Book)-[:PUBLISHED_BY]->(p:Publisher)
WITH p, 
     COUNT(b) AS TotalBuku,
     AVG(b.avg_rating) AS RataRating,
     SUM(b.ratings_count) AS TotalRatings
RETURN p.name AS Publisher,
       TotalBuku,
       ROUND(RataRating, 2) AS AvgRating,
       TotalRatings
ORDER BY TotalRatings DESC
LIMIT 10;

//Buku Paling Controversial (Banyak Review tapi Rating Biasa)
MATCH (b:Book)
WHERE b.reviews_count > 1000
WITH b, 
     (b.reviews_count * 1.0 / b.ratings_count) AS ReviewRatio
WHERE ReviewRatio > 0.1
RETURN b.title AS Judul,
       b.avg_rating AS Rating,
       b.reviews_count AS JumlahReview,
       b.ratings_count AS JumlahRating,
       ROUND(ReviewRatio, 3) AS RatioReview
ORDER BY ReviewRatio DESC
LIMIT 10;