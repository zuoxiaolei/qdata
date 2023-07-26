select code,
       name,
       date,
       close,
       scale,
       type,
       ((slope - slope_mean) / slope_std) slope_standard
from (select code,
             name,
             date,
             close,
             scale,
             type,
             coef.slope                                                                            slope,
             coef.intercept                                                                        intercept,
             coef.r2                                                                               r2,
             std(coef.slope)
                 over (partition by code order by date rows between 599 preceding and current row) slope_std,
             avg(coef.slope)
                 over (partition by code order by date rows between 599 preceding and current row) slope_mean
      from (select code,
                   name,
                   date,
                   close,
                   scale,
                   type,
                   get_ols(x, y) coef
            from (select code,
                         name,
                         date,
                         close,
                         scale,
                         type,
                         rn,
                         collect_list(close)
                                      over (partition by code order by date rows between 17 preceding and current row) y,
                         collect_list(rn)
                                      over (partition by code order by date rows between 17 preceding and current row) x
                  from (select t1.code,
                               t3.name,
                               date,
                               close,
                               t2.scale,
                               t3.type,
                               row_number() over (partition by t1.code order by t1.date) rn
                        from df t1
                                 join scale_df t2
                                      on t1.code = t2.code
                                 join fund_etf_fund_daily_em_df t3
                                      on t1.code = t3.code
                                 join (select count(1) cnt, code from df group by code having cnt >= 200) t4
                                      on t1.code = t4.code) t
                  ) t
            ) t
      ) t
order by code, date
;

-- 1
select code,
       date,
       close,
       ((slope - slope_mean) / slope_std) slope_standard
from (select code,
             date,
             close,
             coef.slope                                                                            slope,
             coef.intercept                                                                        intercept,
             coef.r2                                                                               r2,
             std(coef.slope)
                 over (partition by code order by date rows between 599 preceding and current row) slope_std,
             avg(coef.slope)
                 over (partition by code order by date rows between 599 preceding and current row) slope_mean
      from (select code,
                   date,
                   close,
                   get_ols(x, y) coef
            from (select code,
                         date,
                         close,
                         rn,
                         collect_list(close)
                                      over (partition by code order by date rows between 17 preceding and current row) y,
                         collect_list(rn)
                                      over (partition by code order by date rows between 17 preceding and current row) x
                  from (select t1.code,
                               date,
                               close,
                               row_number() over (partition by t1.code order by t1.date) rn
                        from df t1
                        ) t
                  ) t
            ) t
      ) t
order by code, date