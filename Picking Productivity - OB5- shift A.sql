SELECT   picked.picker_id,
         picked.NAME                                                                                         picker,
         Cast(Count(DISTINCT picked.shipments)/Count(DISTINCT picked.show_dates):: numeric AS DECIMAL(16,2)) shipments_pick_per_day,
         Cast(Sum(picked.cnt)                 /Count(DISTINCT picked.show_dates):: numeric AS DECIMAL(16,2)) units_pick_per_day,
         Cast(Count(DISTINCT picked.shipments)/Count(DISTINCT picked.show_hrs)::   numeric AS DECIMAL(16,2)) shipments_pick_per_hour,
         Cast(Sum(picked.cnt)                 /Count(DISTINCT picked.show_hrs)::   numeric AS DECIMAL(16,2)) units_pick_per_hour
FROM     (
                SELECT sgs.picker_id picker_id,
                       a.first_name
                              || ' '
                              || a.last_name                                   AS NAME,
                       sgs.shipment_id                                            shipments,
                       To_char(sgs.picking_completed_date, 'YYYY-MON-DD')         show_dates,
                       To_char(sgs.picking_completed_date, 'YYYY-MON-DD HH24')    show_hrs,
                       os.item_cnt                                                cnt
                FROM   shipment s JOIN  sg_shipment_state sgs ON s.shipment_id = sgs.shipment_id
                       JOIN administrators a ON sgs.picker_id = a.id
                       JOIN exporting_flags ef ON s.shipment_id = ef.shipment_id
                       JOIN (
                                SELECT   s.shipment_id,
                                         Sum(quantity) item_cnt
                                FROM     order_shipment os JOIN shipment s ON s.shipment_id = os.shipment_id
                                WHERE    s.authorized::Date >= {StartDate}:: DATE - 30
                                GROUP BY s.shipment_id) os
                       ON os.shipment_id = s.shipment_id
                WHERE     ef.work_type_id IN (29,
                                           22,
                                           26,
                                           27,
                                           31,
                                           32,
                                           33,
                                           12)
                AND    sgs.picking_completed_date IS NOT NULL
                AND    sgs.is_picking_in_progress = 0
                AND    s.is_cancelled != 1
                AND    s.is_drop_ship != 1
                AND    sgs.picking_completed_date >= {StartDate}:: DATE
                AND    sgs.picking_completed_date <= {EndDate}:: DATE
                AND    (
                              Substring(sgs.picking_completed_date::          varchar,12,2)
                                     || Substring(sgs.picking_completed_date::varchar,15,2))::integer /100 :: numeric < 15.00
                AND    substring(sgs.picking_completed_date::varchar,12,2)::integer >= 6 ) picked
GROUP BY picked.picker_id,
         picked.NAME
ORDER BY shipments_pick_per_day DESC;








