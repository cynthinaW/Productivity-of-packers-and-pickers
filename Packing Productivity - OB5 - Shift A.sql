SELECT   packed.packer_id::                                                                                  INT,
         packed.name                                                                                         packer,
         CAST(COUNT(DISTINCT packed.shipments)/COUNT(DISTINCT packed.show_dates):: NUMERIC AS DECIMAL(16,2)) shipments_pack_per_day,
         CAST(SUM(packed.cnt)                 /COUNT(DISTINCT packed.show_dates):: NUMERIC AS DECIMAL(16,2)) units_pack_per_day,
         CAST(COUNT(DISTINCT packed.shipments)/COUNT(DISTINCT packed.show_hrs)::   NUMERIC AS DECIMAL(16,2)) shipments_pack_per_hour,
         CAST(SUM(packed.cnt)                 /COUNT(DISTINCT packed.show_hrs)::   NUMERIC AS DECIMAL(16,2)) units_pack_per_hour
FROM     ( 
                SELECT spss.packer_id packer_id, 
                       a.first_name 
                              || ' ' 
                              || a.last_name                                   AS NAME, 
                       spss.shipment_id                                           shipments, 
                       TO_CHAR(spss.packing_complete_date, 'YYYY-MON-DD')         show_dates, 
                       TO_CHAR(spss.packing_complete_date, 'YYYY-MON-DD HH24')    show_hrs, 
                       os.item_cnt                                                cnt 
                FROM   
                         ugcld.shipment s
                   JOIN  ugcld.sg_packing_shipment_state spss
                     ON s.shipment_id = spss.shipment_id 
                   JOIN  ugcld.administrators a
                     ON spss.packer_id = a.id 
                   JOIN    ( 
                                SELECT   shipment_id, 
                                         SUM(quantity) item_cnt 
                                FROM     ugcld.order_shipment 
                                GROUP BY shipment_id) os 
                     ON   os.shipment_id = spss.shipment_id 
                  JOIN       ugcld.exporting_flags ef
                    ON  s.shipment_id = ef.shipment_id  
                WHERE 
                       ef.work_type_ID IN (29, 
                                           22, 
                                           26, 
                                           27, 
                                           31, 
                                           32, 
                                           33, 
                                           12) 
        
                AND    spss.packing_complete_date IS NOT NULL 
                AND    s.is_cancelled != 1 
                AND    s.is_drop_ship != 1 
                AND    spss.packing_complete_date >= {StartDate}
                AND    spss.packing_complete_date <= {EndDate}
                AND    ( 
                              SUBSTRING(spss.packing_complete_date::          VARCHAR,12,2) 
                                     || SUBSTRING(spss.packing_complete_date::VARCHAR,15,2))::INTEGER /100 :: NUMERIC < 15.00
                AND    SUBSTRING(spss.packing_complete_date::VARCHAR,12,2)::INTEGER >= 6
                AND    s.shipment_id NOT IN 
                       ( 
                              SELECT shipment_id 
                              FROM   ( 
                                                     SELECT          s.shipment_id, 
                                                                     CASE 
                                                                                     WHEN al.packing_line = 9 THEN 'Assembly Line'
                                                                                     WHEN al.packing_line = 10 THEN 'Assembly Line'
                                                                     END AS assembly_line, 
                                                                     CASE 
                                                                                     WHEN al2.packing_line = 1 THEN 'Speed'
                                                                                     WHEN al2.packing_line = 2 THEN 'Speed'
                                                                                     WHEN al2.packing_line = 3 THEN 'Speed'
                                                                                     WHEN al2.packing_line = 4 THEN 'Speed'
                                                                                     WHEN al2.packing_line = 5 THEN 'Speed'
                                                                                     WHEN al2.packing_line = 6 THEN 'Speed'
                                                                                     WHEN al2.packing_line = 7 THEN 'Speed'
                                                                                     WHEN al2.packing_line = 8 THEN 'Speed'
                                                                     END AS speed 
                                                     FROM            shipment s 
                                                     LEFT OUTER JOIN 
                                                                     ( 
                                                                            SELECT s.shipment_id,
                                                                                   CASE 
                                                                                          WHEN fc.is_fragile = 1 THEN 'Fragile'
                                                                                          WHEN fc.is_fragile = 0 THEN 'Non-Fragile' 
                                                                                          ELSE NULL
                                                                                   END AS fragile,
                                                                                   CASE 
                                                                                          WHEN fc.is_fragile = 0 THEN bs.line
                                                                                          WHEN fc.is_fragile = 1 THEN bs.line_f
                                                                                          ELSE NULL
                                                                                   END AS packing_line
                                                                            FROM   ugcld.shipment s,
                                                                                   ugcld.shipment_cubing sc,
                                                                                   ugcld.box_size bs,
                                                                                   ugcld.exporting_flags ef,
                                                                                   ( 
                                                                                          SELECT shipment_id,
                                                                                                 sku,
                                                                                                 is_fragile
                                                                                          FROM   (
                                                                                                          SELECT   s.shipment_id,
                                                                                                                   sku.sku,
                                                                                                                   sku.is_fragile,
                                                                                                                   row_number() over (partition BY s.shipment_id ORDER BY sku.is_fragile DESC) AS sku_order
                                                                                                          FROM     ugcld.shipment s
                                                                                                             JOIN ugcld.order_shipment osh
                                                                                                                ON s.shipment_id = osh.shipment_id
                                                                                                             JOIN  ugcld.order_sku os
                                                                                                                ON osh.order_sku_id = os.order_sku_id
                                                                                                            JOIN ugcld.sku
                                                                                                                ON  os.sku = sku.sku
                                                                                                          WHERE    
                                                                                                            s.authorized :: DATE >= {StartDate}:: DATE -10)so
                                                                                          WHERE  sku_order = 1)fc
                                                                            WHERE  s.shipment_id = fc.shipment_id
                                                                            AND    s.shipment_id = ef.shipment_id
                                                                            AND    s.shipment_id = sc.shipment_id
                                                                            AND    sc.box_id = bs.id
                                                                            AND    s.authorized :: DATE >= {StartDate}:: DATE - 10
                                                                            AND    ef.work_type_id                           IN ( 26,
                                                                                                                                 27,
                                                                                                                                 30,
                                                                                                                                 31,
                                                                                                                                 22,
                                                                                                                                 29 ))AL
                                                     ON              s.shipment_id = al.shipment_id
                                                     LEFT OUTER JOIN 
                                                                     ( 
                                                                            SELECT s.shipment_id,
                                                                                   CASE 
                                                                                          WHEN fc.is_fragile = 1 THEN 'Fragile'
                                                                                          WHEN fc.is_fragile = 0 THEN 'Non-Fragile' 
                                                                                          ELSE NULL
                                                                                   END AS fragile,
                                                                                   CASE 
                                                                                          WHEN ef.work_type_id = 32 THEN '2'
                                                                                          WHEN fc.is_fragile = 0 THEN bs.line
                                                                                          WHEN fc.is_fragile = 1 THEN bs.line_f
                                                                                          ELSE NULL
                                                                                   END AS packing_line
                                                                            FROM   ugcld.shipment s,
                                                                                   ugcld.shipment_cubing sc,
                                                                                   ugcld.box_size bs,
                                                                                   ugcld.exporting_flags ef,
                                                                                   ( 
                                                                                          SELECT shipment_id,
                                                                                                 sku,
                                                                                                 is_fragile
                                                                                          FROM   (
                                                                                                          SELECT   s.shipment_id,
                                                                                                                   sku.sku,
                                                                                                                   sku.is_fragile,
                                                                                                                   row_number() over ( partition BY s.shipment_id ORDER BY sku.is_fragile DESC ) AS sku_order
                                                                                                          FROM     ugcld.shipment s
                                                                                                             JOIN  ugcld.order_shipment osh
                                                                                                               ON  s.shipment_id = osh.shipment_id
                                                                                                             JOIN  ugcld.order_sku os  
                                                                                                               ON  osh.order_sku_id = os.order_sku_id
                                                                                                             JOIN  ugcld.sku
                                                                                                               ON  os.sku = sku.sku 
                                                                                                          WHERE    
                                                                                                                s.authorized :: DATE >= {StartDate}:: DATE -10)so
                                                                                          WHERE  sku_order = 1)fc
                                                                            WHERE  s.shipment_id = fc.shipment_id
                                                                            AND    s.shipment_id = ef.shipment_id
                                                                            AND    s.shipment_id = sc.shipment_id
                                                                            AND    sc.box_id = bs.id
                                                                            AND    s.authorized :: DATE >= {StartDate}:: DATE - 10
                                                                            AND    ef.work_type_id                           IN ( 26,
                                                                                                                                 27,
                                                                                                                                 32,
                                                                                                                                 30,
                                                                                                                                 31,
                                                                                                                                 22,
                                                                                                                                 29 ))AL2
                                                     ON              s.shipment_id = al2.shipment_id
                                                     WHERE           s.authorized :: DATE >= {StartDate}:: DATE - 10 ) bw
                              WHERE  bw.assembly_line = 'Assembly Line' )
                              ) packed 
GROUP BY packed.packer_id, 
         packed.name 
ORDER BY shipments_pack_per_day DESC;