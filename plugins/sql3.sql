with top5 as (select Publisher from (select Publisher, sum(Global_Sales) as pub_sal
from default.video_game_sales
group by Publisher
order by pub_sal desc
limit 5) as src)

select sumIf(Global_Sales, Publisher not in top5) as glob_sal
from default.video_game_sales