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
