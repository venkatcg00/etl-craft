-- The catalog site was published again at its recorded URL.
UPDATE AUD_DOCS_PUBLICATION SET LAST_PUBLISHED = CURRENT_TIMESTAMP
WHERE DOCS_PUBLICATION_ID = :docs_publication_id
