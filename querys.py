precios_oferta = '''
    SELECT
        DISTINCT
        LPTO.*,
        FP.PROM_FECHA_INICIO AS INICIO,
        FP.PROM_FECHA_FIN AS FIN,
        LAA.ORIN,
        LAA.ARTC_ARTC_COD AS ESTADISTICO,
        FP.PROM_PVP_OFERTA
    FROM
        MSTRDB.DWH.FT_PROMOS AS FP
        INNER JOIN MSTRDB.DWH.LU_PROM_EVENTO AS LPE ON LPE.EVENTO_ID = FP.EVENTO_ID
        INNER JOIN MSTRDB.DWH.LU_PROM_TIPO_OFERTA AS LPTO ON LPTO.TIPO_OFERTA_ID = LPE.TIPO_OFERTA_ID
        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FP.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
    WHERE
        LPTO.TIPO_OFERTA_ID in (10,11,12)
        AND FP.PROM_FECHA_INICIO >= DATEADD(DAY, -30, '{desde_snow}')
        AND FP.PROM_FECHA_FIN < '{desde_snow}'
'''

precios_stock_mediano = '''
    SELECT
        DISTINCT
        LPTO.*,
        FP.PROM_FECHA_INICIO AS INICIO,
        FP.PROM_FECHA_FIN AS FIN,
        --LAA.ARTC_ARTC_ID,
        --LAA.ARTC_ARTC_COD,
        --LAA.ARTC_ARTC_DESC,
        LAA.ORIN,
        MEDIAN(FS.STCK_PRECIO_VENTA_DIA_CIVA) AS PRECIO_STOCK_MEDIANO
    FROM
        MSTRDB.DWH.FT_PROMOS AS FP
        INNER JOIN MSTRDB.DWH.LU_PROM_EVENTO AS LPE ON LPE.EVENTO_ID = FP.EVENTO_ID
        INNER JOIN MSTRDB.DWH.LU_PROM_TIPO_OFERTA AS LPTO ON LPTO.TIPO_OFERTA_ID = LPE.TIPO_OFERTA_ID
        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FP.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
        INNER JOIN MSTRDB.DWH.FT_STOCK AS FS ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID AND FS.TIEM_DIA_ID = CURRENT_DATE - 1
    WHERE
        LPTO.TIPO_OFERTA_ID IN (10, 11, 12)
    GROUP BY
        ALL
'''

opt = '''
SELECT
    --LAA.ARTC_ARTC_ID,
    --LAA.ARTC_ARTC_COD,
    --LAA.ARTC_ARTC_DESC,
    LAA.ORIN,
    ROUND(AVG(OPT.PRECIO_COMPETENCIA), 1) AS AVG_PRECIO_COMPETENCIA
FROM
    SPIKE.SPIKE.BASE_OPT AS OPT
    INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON OPT.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
    INNER JOIN MSTRDB.DWH.LU_GEOG_LOCAL AS LGL ON OPT.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
GROUP BY
    ALL
'''

locales_activos_ayer = '''
SELECT
    --LAA.ARTC_ARTC_DESC AS ARTICULO,
    LAA.ORIN,
    --LAA.ARTC_ARTC_ID,
    COUNT(DISTINCT FS.GEOG_LOCL_ID) AS LOCALES_ACTIVOS_AYER
FROM
    MSTRDB.DWH.FT_STOCK AS FS
    LEFT JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
WHERE
    ARTC_ESTA_ID = 4
    AND LAA.ORIN IS NOT NULL
    AND GEOG_LOCL_ID IN (SELECT GEOG_LOCL_ID FROM MSTRDB.DWH.LU_GEOG_LOCAL WHERE GEOG_UNNG_ID = 2)
    AND GEOG_LOCL_ID IN (SELECT GEOG_LOCL_ID FROM MSTRDB.DWH.LU_GEOG_LOCAL WHERE GEOG_LOCL_COD NOT IN (198, 100))
    AND FS.TIEM_DIA_ID = CURRENT_DATE() - 1
    AND FS.GEOG_LOCL_ID NOT IN (SELECT GEOG_LOCL_ID FROM MSTRDB.DWH.LU_GEOG_LOCAL WHERE GEOG_LOCL_COD IN {cercania_snow})
GROUP BY
    ALL
HAVING
    LOCALES_ACTIVOS_AYER > 0
'''

days_on_hand_articulo = '''
WITH STOCK AS
    (
    SELECT
        IM.SUBCLASE,
        LAA.ARTC_ARTC_DESC,
        LAA.ORIN,
        LAA.ARTC_ARTC_ID,
        SUM(FS.STCK_UNIDADES) AS UNIDADES
    FROM
        MSTRDB.DWH.FT_STOCK AS FS
        LEFT JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
        INNER JOIN MSTRDB.DWH.ITEM_MASTER AS IM ON LAA.ORIN = IM.ITEM
    WHERE
        FS.TIEM_DIA_ID = CURRENT_DATE() - 1
    GROUP BY
        ALL
    ),

VENTAS AS
    (
    SELECT
        FV.ARTC_ARTC_ID,
        (SUM(FV.VNTA_UNIDADES) / 30) AS UNIDADES_VENDIDAS
    FROM
        MSTRDB.DWH.FT_VENTAS AS FV
    WHERE
        FV.TIEM_DIA_ID >= DATEADD(MONTH, -1, CURRENT_DATE()-1)
    GROUP BY
        ALL
    )

SELECT
    STOCK.*,
    COALESCE(VENTAS.UNIDADES_VENDIDAS, 0) AS UNIDADES_VENDIDAS
FROM
    STOCK
    LEFT JOIN VENTAS ON VENTAS.ARTC_ARTC_ID = STOCK.ARTC_ARTC_ID
'''

days_on_hand_subclase = '''
WITH STOCK AS
    (
    SELECT
        SUB.SUBCLASE,
        SUB.SUB_NAME,
        SUM(FS.STCK_UNIDADES) AS UNIDADES
    FROM
        MSTRDB.DWH.FT_STOCK AS FS
        LEFT JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
        INNER JOIN MSTRDB.DWH.ITEM_MASTER AS IM ON IM.ITEM = LAA.ORIN
        INNER JOIN MSTRDB.DWH.SUBCLASS AS SUB ON SUB.SUBCLASE = IM.SUBCLASE
    WHERE
        FS.TIEM_DIA_ID = CURRENT_DATE() - 1
    GROUP BY
        ALL
    ),

VENTAS AS
    (
    SELECT
        SUB.SUBCLASE,
        (SUM(FV.VNTA_UNIDADES) / 30) AS UNIDADES_VENDIDAS
    FROM
        MSTRDB.DWH.FT_VENTAS AS FV
        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FV.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
        INNER JOIN MSTRDB.DWH.ITEM_MASTER AS IM ON IM.ITEM = LAA.ORIN
        INNER JOIN MSTRDB.DWH.SUBCLASS AS SUB ON SUB.SUBCLASE = IM.SUBCLASE
    WHERE
        FV.TIEM_DIA_ID >= DATEADD(MONTH, -1, CURRENT_DATE()-1)
    GROUP BY
        ALL
    )

SELECT
    STOCK.*,
    COALESCE(VENTAS.UNIDADES_VENDIDAS, 0) AS UNIDADES_VENDIDAS
FROM
    STOCK
    LEFT JOIN VENTAS ON VENTAS.SUBCLASE = STOCK.SUBCLASE
'''