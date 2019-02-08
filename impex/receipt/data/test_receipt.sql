-- 1. receipt_rwbreceipt -> receipt_rwbreceiptcar - д.б. 0 записей
--    для всех вагонов должна быть накладная (и наоборот)
select distinct A.key_number
from receipt_rwbreceipt A
left join receipt_rwbreceiptcar B
on A.key_number=B.rwbill_no
where B.rwbill_no is null;

-- 2. receipt_rwbreceiptcar -> receipt_rwbreceipt - д.б. 0 записей
--    для всех вагонов должна быть накладная (и наоборот)
select distinct A.rwbill_no
from receipt_rwbreceiptcar A
left join receipt_rwbreceipt B
on B.key_number=A.rwbill_no
where B.key_number is null;

-- 3. receipt_rwbreceiptcar -> receipt_rwbreceiptcartime
--    для всех вагонов(+накладная) должно быть соответствие.
--    ненормально - есть вагон+накладная, нет записи о прибытии, д.б. 0 записей
select distinct A.rwbill_no, C.doc_date
from receipt_rwbreceiptcar A
inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
left join receipt_rwbreceiptcartime B
on A.key_number=B.key_number
where C.doc_date<'2019-01-01' and B.key_number is null;

-- 4. receipt_rwbreceiptcartime -> receipt_rwbreceiptcar
--    ненормально - есть время нет пары вагон+накладная, д.б. 0 записей
select distinct  A.rwbill_no
from receipt_rwbreceiptcartime A
left join receipt_rwbreceiptcar B
on A.key_number=B.key_number
where B.key_number is null;

-- 5. receipt_memocar
--    ненормально - для пары вагон+накладная нет значения маршрута, д.б. 0 записей
select count(*) from receipt_memocar where route is null;

-- 6. receipt_rwbreceiptcartime
--    в receipt_memocar и receipt_rwbreceiptcartime должны быть A.route == B.route, д.б. 0 записей
select A.key_number, A.memo, B.memo
from receipt_rwbreceiptcartime A
inner join receipt_memocar B on A.rwbill_no=B.rwbill_no and A.car_no=B.car_no
where A.route<>B.route;

-- 7. receipt_rwbreceiptcar -> receipt_gtd - д.б. 0 записей
--    для всех вагонов должна быть гтд
select distinct A.gtd, C.doc_date --, A.rwbill_no
from receipt_rwbreceiptcar A
inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
left join receipt_gtd B
on B.key_number=A.gtd
where B.key_number is null order by C.doc_date desc;

-- 8. receipt_rwbreceiptcartime -> receipt_memotime - д.б. 0 записей
--    для всех вагонов должен быть путь прибытия
select A.rwbill_no, A.route, B.path_no
from receipt_rwbreceiptcartime A
left join receipt_memotime B
on B.route=A.route and B.memo=A.memo and B.notice=A.notice
where B.path_no is null;

-- Обновления для адаптера
-- 0. receipt_rwbreceiptcartime, где нет пути ищем по маршруту, предположение что все по маршруту на один путь
--    исключены все, где по одному маршруту приходили на разные пути
select A.key_number, B.path_no
from receipt_rwbreceiptcartime A
inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
inner join (select route, path_no from
(select distinct route, path_no from receipt_memotime) D
group by route having count(*) = 1) B
on B.route=A.route;


-- Обновления для адаптера
-- 1. receipt_rwbreceiptcar -> receipt_rwbreceiptcartime
--    ненормально - запись не содержит данных route, doc_date, get_date
-- заполнить route по умолчанию
select distinct  A.rwbill_no, B.doc_date, B.get_date, B.route
from receipt_rwbreceiptcar A
inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
left join receipt_rwbreceiptcartime B
on A.key_number=B.key_number
where C.doc_date<'2019-01-01' and B.route='';
-- заполнить doc_date из get_date или по умолчанию (и врермя)
select distinct  A.rwbill_no, B.doc_date, B.get_date, B.route
from receipt_rwbreceiptcar A
inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
left join receipt_rwbreceiptcartime B
on A.key_number=B.key_number
where C.doc_date<'2019-01-01' and B.doc_date='2000-01-01';
-- заполнить get_date из doc_date (и врермя)
select distinct  A.rwbill_no, B.doc_date, B.get_date
from receipt_rwbreceiptcar A
inner join receipt_rwbreceipt C on A.rwbill_no=C.key_number
left join receipt_rwbreceiptcartime B
on A.key_number=B.key_number
where C.doc_date<'2019-01-01' and B.get_date='2000-01-01';

-- 2. receipt_rwbreceiptcartime
--    проставим значение памятка из receipt_memocar если оно не определено в таблице receipt_rwbreceiptcartime
select A.key_number, A.doc_date, A.memo, B.memo
from receipt_rwbreceiptcartime A
inner join receipt_memocar B on A.rwbill_no=B.rwbill_no and A.car_no=B.car_no and A.route=B.route
where A.memo = 'б/н' order by A.doc_date;
