-- The URL the catalog site was last published at.
SELECT DOCS_PUBLICATION_ID AS docs_publication_id, PUBLISHED_URL AS published_url,
       FIRST_PUBLISHED AS first_published, LAST_PUBLISHED AS last_published
FROM AUD_DOCS_PUBLICATION
ORDER BY DOCS_PUBLICATION_ID DESC
LIMIT 1
