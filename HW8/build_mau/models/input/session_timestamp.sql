WITH src_session_timestamp AS (
    SELECT
        sessionId,
        ts
    FROM {{ source('raw_data', 'session_timestamp') }}
    WHERE sessionId IS NOT NULL
)
SELECT sessionId, ts FROM src_session_timestamp
