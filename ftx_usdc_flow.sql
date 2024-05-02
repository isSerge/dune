-- FTX addresses CTE
with address_cte (address) as (
  values (0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2),
    (0xc098b2a3aa256d2140208c3de6543aaef5cd3a94)
) 
,
-- Token price CTE
token_price_decimal_cte as (
  select date_trunc ('day', minute) as time,
    symbol,
    decimals,
    avg(price) as avg_price
  from prices.usd
  where blockchain = 'ethereum'
    and contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    and minute >= cast('2022-11-01' as TIMESTAMP)
    and minute <= cast('2022-11-11' as TIMESTAMP)
  group by 1,
    2,
    3
) 
,
-- Inflows CTE
inflow_cte as (
  select date_trunc ('day', evt_block_time) as time,
    'in' as category,
    sum(cast(value as double) / pow (10, tk.decimals)) as raw_amount,
    sum(
      cast(value as double) / pow (10, tk.decimals) * tk.avg_price
    ) as usd_amount
  from erc20_ethereum.evt_transfer t
    left join token_price_decimal_cte tk on tk.time = date_trunc ('day', evt_block_time)
  where t.contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    and t.evt_block_time >= cast('2022-11-01' as TIMESTAMP)
    and t.evt_block_time <= cast('2022-11-11' as TIMESTAMP)
    and t.to in (
      select address
      from address_cte
    )
  group by 1,
    2
)
,
-- Outflows CTE
outflow_cte as (
  select date_trunc ('day', evt_block_time) as time,
    'out' as category,
    -1 * sum(cast(value as double) / pow (10, tk.decimals)) as raw_amount,
    -1 * sum(
      cast(value as double) / pow (10, tk.decimals) * tk.avg_price
    ) as usd_amount
  from erc20_ethereum.evt_transfer t
    left join token_price_decimal_cte tk on tk.time = date_trunc ('day', evt_block_time)
  where t.contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    and t.evt_block_time >= cast('2022-11-01' as TIMESTAMP)
    and t.evt_block_time <= cast('2022-11-11' as TIMESTAMP)
    and t."from" in (
      select address
      from address_cte
    )
  group by 1,
    2
)

select * from inflow_cte

union all

select * from outflow_cte
