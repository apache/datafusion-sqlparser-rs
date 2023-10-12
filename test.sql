SELECT
    c1
FROM
    GFC_SOCIAL.SLOT
WHERE
    DATE >= DATE(:myvar) -3
    AND DATE < DATE(SYSDATE()) -- SELECT
    --     :1
