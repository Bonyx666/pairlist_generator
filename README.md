# freqtrade-stuff
Use the examples / information here at your own risk.

If you don't care about how to use that code then you dont need to look at it.

Inside the .7z file you see files sorted by exchange and min_price.

What I mostly use is the monthly file and then when a backtest should be started from a certain month the previous months' pairlist is taken and used as static list. Just copy out what block you need.
With this the the behavior is closer to live since volumes of pairs change over time.

2022-06-01:
Now you need a file called pairlist_generator_config_template.json in your root directory.
This file will be the base for your pairlists and will produce one config file per each step (daily / weekly / monthly).
I am using this to have pairlists automatically generated for backtesting multiple exchanges.

2024-05-04:
Now it doesn't rely on a template config file
now downloads all exchanges freqtrade can possibly get a handle of
The net is intentionally cast as wide as possible, you ll need to decide yourself which exchange+stake+config combo you ll want to use.
